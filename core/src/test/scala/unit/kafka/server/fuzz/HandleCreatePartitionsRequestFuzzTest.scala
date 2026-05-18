/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.{ControllerMutationQuota, KafkaApisTest, MetadataCache, UnboundedControllerMutationQuota, ZkBrokerEpochManager}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{TopicExistsException, UnsupportedVersionException}
import org.apache.kafka.common.message.CreatePartitionsRequestData
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiError, CreatePartitionsRequest}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.collection.immutable.Map

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleCreatePartitionsRequest` (ZK metadata path).
 *
 * Covers Raft `shouldAlwaysForward` guard, inactive controller (`NOT_CONTROLLER`), duplicate
 * topic entries (`INVALID_REQUEST`), `ALTER` topic authorization, topics queued for deletion
 * (`INVALID_TOPIC_EXCEPTION`), `ZkAdminManager.createPartitions` callback success and error
 * merge with precomputed errors, empty topic list and empty `valid` list, client vs controller
 * mutation throttling, and forwarded inner requests.
 */
class HandleCreatePartitionsRequestFuzzTest extends KafkaApisTest {

  private def validateCreatePartitionsRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val m = e.getMessage
    if (m == null || !m.startsWith(prefix)) throw e
  }

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val s = data.consumeString(48)
    if (s == null || s.isEmpty) fallback
    else s
  }

  private def resetZkAndCommonMocks(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubControllerMutationQuota(quota: ControllerMutationQuota): Unit = {
    when(clientControllerQuotaManager.newQuotaFor(
      any[RequestChannel.Request](),
      ArgumentMatchers.eq(3))).thenReturn(quota)
  }

  private def stubCreatePartitionsCallback(result: Map[String, ApiError]): Unit = {
    when(adminManager.createPartitions(anyInt(), any(), anyBoolean(), any(), any()))
      .thenAnswer(invocation => {
        val cb = invocation.getArgument(4).asInstanceOf[Map[String, ApiError] => Unit]
        cb(result)
      })
  }

  private def authorizerAlterTopicsAllowed(allowedTopicNames: Set[String]): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { act =>
        val res =
          if (act.operation == AclOperation.ALTER && act.resourcePattern.resourceType == ResourceType.TOPIC)
            if (allowedTopicNames.contains(act.resourcePattern.name())) AuthorizationResult.ALLOWED
            else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        out.add(res)
      }
      out
    })
    a
  }

  private def buildCreatePartitionsRequest(
    version: Short,
    timeoutMs: Int,
    validateOnly: Boolean,
    topics: CreatePartitionsTopic*
  ): CreatePartitionsRequest = {
    val data = new CreatePartitionsRequestData()
      .setTimeoutMs(timeoutMs)
      .setValidateOnly(validateOnly)
    topics.foreach(t => data.topics().add(t))
    new CreatePartitionsRequest.Builder(data).build(version)
  }

  private def partitionTopic(name: String, count: Int): CreatePartitionsTopic =
    new CreatePartitionsTopic()
      .setName(name)
      .setAssignments(null)
      .setCount(count)

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsRaftAlwaysForwardUnsupported(data: FuzzedDataProvider): Unit = {
    val v0 = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val v1 = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val useFirst = data.consumeBoolean()
    val version = if (useFirst) v0 else v1
    val topic = safeTopicName(data, "fuzz-cp-raft")
    val count = data.consumeInt(1, 200)

    resetZkAndCommonMocks()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    stubNoThrottle()

    val built = buildCreatePartitionsRequest(version, timeoutMs = 30_000, validateOnly = false,
      partitionTopic(topic, count))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleCreatePartitionsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateCreatePartitionsRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsNotController(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-cp-nc-a")
    val t2 = safeTopicName(data, "fuzz-cp-nc-b")
    val c1 = data.consumeInt(1, 100)
    val c2 = data.consumeInt(1, 100)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(false)

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(t1, c1), partitionTopic(t2, c2))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsDuplicateTopicNamesInRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val dup = safeTopicName(data, "fuzz-cp-dup")
    val other = safeTopicName(data, "fuzz-cp-dup-other")
    val cDup1 = data.consumeInt(2, 50)
    val cDup2 = data.consumeInt(2, 50)
    val cOther = data.consumeInt(2, 50)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(other -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(dup, cDup1),
      partitionTopic(dup, cDup2),
      partitionTopic(other, cOther))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsTopicAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val allowed = safeTopicName(data, "fuzz-cp-auth-ok")
    val denied = safeTopicName(data, "fuzz-cp-auth-no")
    val cAllow = data.consumeInt(2, 80)
    val cDeny = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(allowed -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(allowed, cAllow),
      partitionTopic(denied, cDeny))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set(allowed))))
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsTopicQueuedForDeletion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val queuedName = safeTopicName(data, "fuzz-cp-qdel")
    val count = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion(queuedName)).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(queuedName, count))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAdminManagerSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-cp-ok-a")
    val t2 = safeTopicName(data, "fuzz-cp-ok-b")
    val c1 = data.consumeInt(2, 80)
    val c2 = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(t1 -> ApiError.NONE, t2 -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(t1, c1), partitionTopic(t2, c2))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAdminManagerErrorMerge(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val okTopic = safeTopicName(data, "fuzz-cp-merge-ok")
    val badTopic = safeTopicName(data, "fuzz-cp-merge-bad")
    val cOk = data.consumeInt(2, 80)
    val cBad = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(
      okTopic -> ApiError.NONE,
      badTopic -> ApiError.fromThrowable(new TopicExistsException("fuzz exists"))
    ))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(okTopic, cOk),
      partitionTopic(badTopic, cBad))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAllTopicsUnauthorizedEmptyValid(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-cp-none-a")
    val t2 = safeTopicName(data, "fuzz-cp-none-b")
    val c1 = data.consumeInt(2, 80)
    val c2 = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(t1, c1), partitionTopic(t2, c2))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set.empty)))
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsEmptyTopicsList(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsMixedDupAuthQueueAndAdmin(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val dup = safeTopicName(data, "fuzz-cp-mix-dup")
    val denied = safeTopicName(data, "fuzz-cp-mix-den")
    val queued = safeTopicName(data, "fuzz-cp-mix-q")
    val ok = safeTopicName(data, "fuzz-cp-mix-ok")
    val cDup1 = data.consumeInt(2, 40)
    val cDup2 = data.consumeInt(2, 40)
    val cDeny = data.consumeInt(2, 40)
    val cQueue = data.consumeInt(2, 40)
    val cOk = data.consumeInt(2, 40)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion(queued)).thenReturn(true)
    stubCreatePartitionsCallback(Map(ok -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly,
      partitionTopic(dup, cDup1),
      partitionTopic(dup, cDup2),
      partitionTopic(denied, cDeny),
      partitionTopic(queued, cQueue),
      partitionTopic(ok, cOk))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set(ok, queued))))
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsThrottling(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val controllerPrimary = data.consumeBoolean()
    val low = data.consumeInt(1, 100)
    val high = data.consumeInt(101, 250)
    val (reqMs, ctrlMs) = if (controllerPrimary) (low, high) else (high, low)
    val topic = safeTopicName(data, "fuzz-cp-throttle")
    val count = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlMs)
    stubControllerMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(topic -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly, partitionTopic(topic, count))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val reqThrottleMs = data.consumeInt(0, 100)
    val ctrlThrottleMs = data.consumeInt(0, 100)
    val topic = safeTopicName(data, "fuzz-cp-fwd")
    val count = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlThrottleMs)
    stubControllerMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(topic -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(version, timeoutMs, validateOnly, partitionTopic(topic, count))
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreatePartitionsRequest(request)
    finally kafkaApis.close()
  }
}
