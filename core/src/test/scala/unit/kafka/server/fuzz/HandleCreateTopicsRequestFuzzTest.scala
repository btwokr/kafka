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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.ControllerMutationQuota
import kafka.server.{KafkaApisTest, MetadataCache, UnboundedControllerMutationQuota, ZkBrokerEpochManager}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiError, CreateTopicsRequest}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.collection.immutable.Map

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleCreateTopicsRequest` (ZK metadata path).
 *
 * Covers inactive controller, internal metadata topic rejection, duplicate topic names,
 * topic-level CREATE authorization when cluster CREATE is denied, DESCRIBE_CONFIGS
 * authorization on creatable results, `ZkAdminManager.createTopics` success and error
 * merge paths (including empty `toCreate`), request vs controller mutation throttling,
 * and forwarded inner requests.
 */
class HandleCreateTopicsRequestFuzzTest extends KafkaApisTest {

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val s = data.consumeString(48)
    if (s == null || s.isEmpty) fallback
    else if (s == Topic.CLUSTER_METADATA_TOPIC_NAME) fallback + "-not-internal"
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
      ArgumentMatchers.eq(6))).thenReturn(quota)
  }

  private def stubCreateTopicsCallback(result: Map[String, ApiError]): Unit = {
    when(adminManager.createTopics(anyInt(), anyBoolean(), any(), any(), any(), any()))
      .thenAnswer(invocation => {
        val cb = invocation.getArgument(5).asInstanceOf[scala.collection.Map[String, ApiError] => Unit]
        cb(result)
      })
  }

  private def authorizerForCreateTopics(
    allowClusterCreate: Boolean,
    topicCreateAllowed: Set[String],
    topicDescribeConfigsAllowed: Set[String]
  ): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { act =>
        val op = act.operation()
        val rt = act.resourcePattern.resourceType()
        val name = act.resourcePattern.name()
        val res =
          if (op == AclOperation.CREATE && rt == ResourceType.CLUSTER)
            if (allowClusterCreate) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else if (op == AclOperation.CREATE && rt == ResourceType.TOPIC)
            if (topicCreateAllowed.contains(name)) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else if (op == AclOperation.DESCRIBE_CONFIGS && rt == ResourceType.TOPIC)
            if (topicDescribeConfigsAllowed.contains(name)) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        out.add(res)
      }
      out
    })
    a
  }

  private def buildCreateTopicsRequest(
    version: Short,
    timeoutMs: Int,
    validateOnly: Boolean,
    topics: CreatableTopic*
  ): CreateTopicsRequest = {
    val data = new CreateTopicsRequestData()
      .setTimeoutMs(timeoutMs)
      .setValidateOnly(validateOnly)
    topics.foreach(t => data.topics().add(t))
    new CreateTopicsRequest.Builder(data).build(version)
  }

  private def creatable(name: String, partitions: Int, replication: Short): CreatableTopic =
    new CreatableTopic()
      .setName(name)
      .setNumPartitions(partitions)
      .setReplicationFactor(replication)

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsNotController(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-ct-nc-a")
    val t2 = safeTopicName(data, "fuzz-ct-nc-b")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(false)

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(t1, 1, 1), creatable(t2, 2, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsClusterMetadataTopicRejected(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val regular = safeTopicName(data, "fuzz-ct-meta-ok")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(regular -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(regular, 1, 1),
      creatable(Topic.CLUSTER_METADATA_TOPIC_NAME, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsDuplicateNamesInRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val dup = safeTopicName(data, "fuzz-ct-dup")
    val other = safeTopicName(data, "fuzz-ct-dup-other")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(other -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(dup, 1, 1),
      creatable(dup, 2, 1),
      creatable(other, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsTopicCreateAuthorizationWithoutCluster(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val allowed = safeTopicName(data, "fuzz-ct-auth-ok")
    val denied = safeTopicName(data, "fuzz-ct-auth-no")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(allowed -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(allowed, 1, 1),
      creatable(denied, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForCreateTopics(
      allowClusterCreate = false,
      topicCreateAllowed = Set(allowed),
      topicDescribeConfigsAllowed = Set(allowed, denied)
    )))
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsDescribeConfigsAuthorization(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val withDescribe = safeTopicName(data, "fuzz-ct-dcfg-yes")
    val withoutDescribe = safeTopicName(data, "fuzz-ct-dcfg-no")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(
      Map(withDescribe -> ApiError.NONE, withoutDescribe -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(withDescribe, 1, 1),
      creatable(withoutDescribe, 2, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForCreateTopics(
      allowClusterCreate = true,
      topicCreateAllowed = Set(withDescribe, withoutDescribe),
      topicDescribeConfigsAllowed = Set(withDescribe)
    )))
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsAdminManagerSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-ct-ok-a")
    val t2 = safeTopicName(data, "fuzz-ct-ok-b")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(t1 -> ApiError.NONE, t2 -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(t1, 1, 1),
      creatable(t2, 3, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsAdminManagerErrorMerge(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val okTopic = safeTopicName(data, "fuzz-ct-merge-ok")
    val badTopic = safeTopicName(data, "fuzz-ct-merge-bad")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(
      okTopic -> ApiError.NONE,
      badTopic -> ApiError.fromThrowable(new TopicExistsException("fuzz exists"))
    ))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(okTopic, 1, 1),
      creatable(badTopic, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsAllTopicsUnauthorizedEmptyToCreate(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val t1 = safeTopicName(data, "fuzz-ct-none-a")
    val t2 = safeTopicName(data, "fuzz-ct-none-b")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map.empty)

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly,
      creatable(t1, 1, 1),
      creatable(t2, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForCreateTopics(
      allowClusterCreate = false,
      topicCreateAllowed = Set.empty,
      topicDescribeConfigsAllowed = Set.empty
    )))
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsThrottling(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val controllerPrimary = data.consumeBoolean()
    val low = data.consumeInt(1, 100)
    val high = data.consumeInt(101, 250)
    val (reqMs, ctrlMs) = if (controllerPrimary) (low, high) else (high, low)
    val topic = safeTopicName(data, "fuzz-ct-throttle")

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlMs)
    stubControllerMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(topic -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly, creatable(topic, 1, 1))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTopicsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_TOPICS.oldestVersion(), ApiKeys.CREATE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val validateOnly = data.consumeBoolean()
    val reqThrottleMs = data.consumeInt(0, 100)
    val ctrlThrottleMs = data.consumeInt(0, 100)
    val topic = safeTopicName(data, "fuzz-ct-fwd")

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlThrottleMs)
    stubControllerMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubCreateTopicsCallback(Map(topic -> ApiError.NONE))

    val built = buildCreateTopicsRequest(version, timeoutMs, validateOnly, creatable(topic, 1, 1))
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateTopicsRequest(request)
    finally kafkaApis.close()
  }
}
