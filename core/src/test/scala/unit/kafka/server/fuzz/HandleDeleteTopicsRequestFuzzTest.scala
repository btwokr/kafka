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
import kafka.controller.ControllerContext
import kafka.network.RequestChannel
import kafka.server.metadata.ZkMetadataCache
import kafka.server.{ControllerMutationQuota, KafkaApisTest, MetadataCache, UnboundedControllerMutationQuota, ZkBrokerEpochManager}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.DeleteTopicsRequestData
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteTopicsRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ServerConfigs
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyInt, anyLong, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.collection.immutable.Map

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDeleteTopicsRequest` (ZK metadata path).
 *
 * Covers inactive controller, topic deletion disabled (legacy vs current error codes),
 * invalid name plus topic-id combination, name-based and id-based authorization outcomes
 * (including unresolved topic ids and describe-without-leaking-name), metadata miss,
 * `ZkAdminManager.deleteTopics` success and partial error callbacks, the immediate response
 * path when nothing is queued for deletion, throttling, and forwarded inner requests.
 */
class HandleDeleteTopicsRequestFuzzTest extends KafkaApisTest {

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(48)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def resetZkAndCommonMocks(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def installMetadataCacheContains(existingTopicNames: Set[String]): Unit = {
    metadataCache = mock(classOf[ZkMetadataCache])
    when(metadataCache.contains(anyString())).thenAnswer(invocation =>
      existingTopicNames.contains(invocation.getArgument(0)))
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubDeleteTopicsMutationQuota(quota: ControllerMutationQuota): Unit = {
    when(clientControllerQuotaManager.newQuotaFor(
      any[RequestChannel.Request](),
      ArgumentMatchers.eq(5))).thenReturn(quota)
  }

  private def stubDeleteTopicsCallback(results: Map[String, Errors]): Unit = {
    when(adminManager.deleteTopics(anyInt(), any(), any(), any()))
      .thenAnswer(invocation => {
        val cb = invocation.getArgument(3).asInstanceOf[Map[String, Errors] => Unit]
        cb(results)
      })
  }

  private def authorizerForDeleteTopics(
    describeAllowed: Set[String],
    deleteAllowed: Set[String]
  ): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { act =>
        val op = act.operation()
        val rt = act.resourcePattern.resourceType()
        val name = act.resourcePattern.name()
        val res =
          if (rt != ResourceType.TOPIC)
            AuthorizationResult.ALLOWED
          else if (op == AclOperation.DESCRIBE)
            if (describeAllowed.contains(name)) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else if (op == AclOperation.DELETE)
            if (deleteAllowed.contains(name)) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        out.add(res)
      }
      out
    })
    mockAuthorizer
  }

  private def buildDeleteTopicsRequestLegacy(version: Short, timeoutMs: Int, names: Seq[String]): DeleteTopicsRequest = {
    val requestData = new DeleteTopicsRequestData().setTimeoutMs(timeoutMs)
    names.foreach(topicName => requestData.topicNames().add(topicName))
    new DeleteTopicsRequest.Builder(requestData).build(version)
  }

  private def buildDeleteTopicsRequestV6Plus(version: Short, timeoutMs: Int, states: Seq[DeleteTopicState]): DeleteTopicsRequest = {
    val requestData = new DeleteTopicsRequestData().setTimeoutMs(timeoutMs)
    states.foreach(topicState => requestData.topics().add(topicState))
    new DeleteTopicsRequest.Builder(requestData).build(version)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsNotController(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic1 = safeTopicName(data, "fuzz-dt-nc-a")
    val topic2 = safeTopicName(data, "fuzz-dt-nc-b")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(false)

    val built = buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic1, topic2))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsDeletionDisabled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic1 = safeTopicName(data, "fuzz-dt-dis-a")
    val topic2 = safeTopicName(data, "fuzz-dt-dis-b")

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val built =
      if (version >= 6) {
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic1).setTopicId(Uuid.ZERO_UUID),
          new DeleteTopicState().setName(topic2).setTopicId(Uuid.ZERO_UUID)
        ))
      } else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic1, topic2))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis(overrideProperties = Map(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG -> "false"))
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsInvalidNameAndNonZeroTopicId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(6.toShort, ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val badName = safeTopicName(data, "fuzz-dt-bad")
    val badId = uuidFromTwoLongs(data)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val built = buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
      new DeleteTopicState().setName(badName).setTopicId(badId)
    ))
    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleDeleteTopicsRequest(request)
      catch {
        case _: InvalidRequestException =>
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsUnknownTopicByName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val unknown = safeTopicName(data, "fuzz-dt-unk")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set.empty)
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(unknown).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(unknown))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsByIdDescribeDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(6.toShort, ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic = safeTopicName(data, "fuzz-dt-ndesc")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic))
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val topicId = uuidFromTwoLongs(data)
    val mockControllerContext = mock(classOf[ControllerContext])
    when(controller.controllerContext).thenReturn(mockControllerContext)
    when(mockControllerContext.topicName(topicId)).thenReturn(Some(topic))

    val built = buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
      new DeleteTopicState().setTopicId(topicId)
    ))
    val request = buildRequest(built)
    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForDeleteTopics(
      describeAllowed = Set.empty,
      deleteAllowed = Set(topic)
    )))
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsByNameDeleteDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic = safeTopicName(data, "fuzz-dt-nodel")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic))
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForDeleteTopics(
      describeAllowed = Set(topic),
      deleteAllowed = Set.empty
    )))
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsByIdUnknownTopicId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(6.toShort, ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topicId = uuidFromTwoLongs(data)

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set.empty)
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val mockControllerContext = mock(classOf[ControllerContext])
    when(controller.controllerContext).thenReturn(mockControllerContext)
    when(mockControllerContext.topicName(topicId)).thenReturn(None)

    val built = buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
      new DeleteTopicState().setTopicId(topicId)
    ))
    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsByIdDeleteDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(6.toShort, ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic = safeTopicName(data, "fuzz-dt-idndel")
    val topicId = uuidFromTwoLongs(data)

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic))
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val mockControllerContext = mock(classOf[ControllerContext])
    when(controller.controllerContext).thenReturn(mockControllerContext)
    when(mockControllerContext.topicName(topicId)).thenReturn(Some(topic))

    val built = buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
      new DeleteTopicState().setTopicId(topicId)
    ))
    val request = buildRequest(built)
    val kafkaApis = createKafkaApis(authorizer = Some(authorizerForDeleteTopics(
      describeAllowed = Set(topic),
      deleteAllowed = Set.empty
    )))
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsAdminManagerSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val topic1 = safeTopicName(data, "fuzz-dt-am-1")
    val topic2 = safeTopicName(data, "fuzz-dt-am-2")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic1, topic2))
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubDeleteTopicsCallback(Map.empty)

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic1).setTopicId(Uuid.ZERO_UUID),
          new DeleteTopicState().setName(topic2).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic1, topic2))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsAdminManagerCallbackErrors(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val topic1 = safeTopicName(data, "fuzz-dt-cb-ok")
    val topic2 = safeTopicName(data, "fuzz-dt-cb-bad")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic1, topic2))
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubDeleteTopicsCallback(Map(
      topic1 -> Errors.NONE,
      topic2 -> Errors.REQUEST_TIMED_OUT
    ))

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic1).setTopicId(Uuid.ZERO_UUID),
          new DeleteTopicState().setName(topic2).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic1, topic2))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsEmptyToDeleteImmediateResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val topic1 = safeTopicName(data, "fuzz-dt-empty-a")
    val topic2 = safeTopicName(data, "fuzz-dt-empty-b")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set.empty)
    stubNoThrottle()
    stubDeleteTopicsMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic1).setTopicId(Uuid.ZERO_UUID),
          new DeleteTopicState().setName(topic2).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic1, topic2))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val controllerPrimary = data.consumeBoolean()
    val low = data.consumeInt(1, 100)
    val high = data.consumeInt(101, 250)
    val (reqMs, ctrlMs) = if (controllerPrimary) (low, high) else (high, low)
    val topic = safeTopicName(data, "fuzz-dt-thr")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic))
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlMs)
    stubDeleteTopicsMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubDeleteTopicsCallback(Map(topic -> Errors.NONE))

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic))

    val request = buildRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteTopicsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_TOPICS.oldestVersion(), ApiKeys.DELETE_TOPICS.latestVersion())
    val timeoutMs = data.consumeInt(1, 120_000)
    val reqThrottleMs = data.consumeInt(0, 100)
    val ctrlThrottleMs = data.consumeInt(0, 100)
    val topic = safeTopicName(data, "fuzz-dt-fwd")

    resetZkAndCommonMocks()
    installMetadataCacheContains(Set(topic))
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)

    val ctrlQuota = mock(classOf[ControllerMutationQuota])
    when(ctrlQuota.throttleTime).thenReturn(ctrlThrottleMs)
    stubDeleteTopicsMutationQuota(ctrlQuota)

    when(controller.isActive).thenReturn(true)
    stubDeleteTopicsCallback(Map(topic -> Errors.NONE))

    val built =
      if (version >= 6)
        buildDeleteTopicsRequestV6Plus(version, timeoutMs, Seq(
          new DeleteTopicState().setName(topic).setTopicId(Uuid.ZERO_UUID)))
      else
        buildDeleteTopicsRequestLegacy(version, timeoutMs, Seq(topic))

    val request = buildForwardedRequest(built)
    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteTopicsRequest(request)
    finally kafkaApis.close()
  }
}
