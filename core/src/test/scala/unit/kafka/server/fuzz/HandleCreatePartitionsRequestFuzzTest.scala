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
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ApiError, CreatePartitionsRequest, CreatePartitionsResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.collection.immutable.Map
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleCreatePartitionsRequest` (ZK metadata path).
 *
 * Covers Raft `shouldAlwaysForward` guard, inactive controller (`NOT_CONTROLLER`), duplicate
 * topic entries (`INVALID_REQUEST`), `ALTER` topic authorization, topics queued for deletion
 * (`INVALID_TOPIC_EXCEPTION`), `ZkAdminManager.createPartitions` callback success and error
 * merge with precomputed errors, empty topic list and empty `valid` list, client vs controller
 * mutation throttling, and forwarded inner requests. Response error codes and error messages are
 * asserted where the handler supplies them.
 */
class HandleCreatePartitionsRequestFuzzTest extends KafkaApisTest {

  /** Matches `KafkaApis.handleCreatePartitionsRequest` duplicate-topic branch. */
  private val duplicateTopicInRequestMessage = "Duplicate topic in request."
  /** Matches `KafkaApis.handleCreatePartitionsRequest` unauthorized-topic branch. */
  private val topicAuthorizationFailedMessage = "The topic authorization is failed."
  /** Matches `KafkaApis.handleCreatePartitionsRequest` topic queued for deletion branch. */
  private val topicQueuedForDeletionMessage = "The topic is queued for deletion."

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
    val topicAuthorizer = mock(classOf[Authorizer])
    when(topicAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val authorizationResults = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { action =>
        val decision =
          if (action.operation == AclOperation.ALTER && action.resourcePattern.resourceType == ResourceType.TOPIC)
            if (allowedTopicNames.contains(action.resourcePattern.name())) AuthorizationResult.ALLOWED
            else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        authorizationResults.add(decision)
      }
      authorizationResults
    })
    topicAuthorizer
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

  private def partitionTopic(name: String, targetPartitionCount: Int): CreatePartitionsTopic =
    new CreatePartitionsTopic()
      .setName(name)
      .setAssignments(null)
      .setCount(targetPartitionCount)

  private def topicResultOrThrow(response: CreatePartitionsResponse, topicName: String): CreatePartitionsTopicResult = {
    response.data.results.asScala.find(_.name == topicName).getOrElse(
      throw new AssertionError(s"No create-partitions result for topic '$topicName'"))
  }

  /**
   * Asserts per-topic result. For `expectedErrorMessage == None`, the wire error message must be null
   * (as produced by `ApiError` with no custom text, e.g. `NOT_CONTROLLER` with a null message).
   */
  private def assertCreatePartitionsTopicResult(
    topicResult: CreatePartitionsTopicResult,
    expectedError: Errors,
    expectedErrorMessage: Option[String]
  ): Unit = {
    assertEquals(expectedError.code, topicResult.errorCode)
    expectedErrorMessage match {
      case None => assertNull(topicResult.errorMessage)
      case Some(expectedText) => assertEquals(expectedText, topicResult.errorMessage)
    }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsRaftAlwaysForwardUnsupported(data: FuzzedDataProvider): Unit = {
    val firstWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val secondWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val useFirstWireVersion = data.consumeBoolean()
    val requestWireVersion = if (useFirstWireVersion) firstWireVersion else secondWireVersion
    val topicName = safeTopicName(data, "fuzz-cp-raft")
    val targetPartitionCount = data.consumeInt(1, 200)

    resetZkAndCommonMocks()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    stubNoThrottle()

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs = 30_000, validateOnly = false,
      partitionTopic(topicName, targetPartitionCount))
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
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val firstTopicName = safeTopicName(data, "fuzz-cp-nc-a")
    val secondTopicName = safeTopicName(data, "fuzz-cp-nc-b")
    val firstTopicTargetCount = data.consumeInt(1, 100)
    val secondTopicTargetCount = data.consumeInt(1, 100)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(false)

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(firstTopicName, firstTopicTargetCount),
      partitionTopic(secondTopicName, secondTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, firstTopicName), Errors.NOT_CONTROLLER, None)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, secondTopicName), Errors.NOT_CONTROLLER, None)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsDuplicateTopicNamesInRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val duplicateTopicName = safeTopicName(data, "fuzz-cp-dup")
    val uniqueTopicName = safeTopicName(data, "fuzz-cp-dup-other")
    val duplicateFirstTargetCount = data.consumeInt(2, 50)
    val duplicateSecondTargetCount = data.consumeInt(2, 50)
    val uniqueTopicTargetCount = data.consumeInt(2, 50)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(uniqueTopicName -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(duplicateTopicName, duplicateFirstTargetCount),
      partitionTopic(duplicateTopicName, duplicateSecondTargetCount),
      partitionTopic(uniqueTopicName, uniqueTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, duplicateTopicName),
        Errors.INVALID_REQUEST,
        Some(duplicateTopicInRequestMessage))
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, uniqueTopicName),
        Errors.NONE,
        None)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsTopicAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val authorizedTopicName = safeTopicName(data, "fuzz-cp-auth-ok")
    val unauthorizedTopicName = safeTopicName(data, "fuzz-cp-auth-no")
    val authorizedTopicTargetCount = data.consumeInt(2, 80)
    val unauthorizedTopicTargetCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(authorizedTopicName -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(authorizedTopicName, authorizedTopicTargetCount),
      partitionTopic(unauthorizedTopicName, unauthorizedTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set(authorizedTopicName))))
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, authorizedTopicName),
        Errors.NONE,
        None)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, unauthorizedTopicName),
        Errors.TOPIC_AUTHORIZATION_FAILED,
        Some(topicAuthorizationFailedMessage))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsTopicQueuedForDeletion(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val topicQueuedForDeletion = safeTopicName(data, "fuzz-cp-qdel")
    val targetPartitionCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion(topicQueuedForDeletion)).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(topicQueuedForDeletion, targetPartitionCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, topicQueuedForDeletion),
        Errors.INVALID_TOPIC_EXCEPTION,
        Some(topicQueuedForDeletionMessage))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAdminManagerSuccess(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val firstSuccessTopic = safeTopicName(data, "fuzz-cp-ok-a")
    val secondSuccessTopic = safeTopicName(data, "fuzz-cp-ok-b")
    val firstTopicTargetCount = data.consumeInt(2, 80)
    val secondTopicTargetCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(firstSuccessTopic -> ApiError.NONE, secondSuccessTopic -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(firstSuccessTopic, firstTopicTargetCount),
      partitionTopic(secondSuccessTopic, secondTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, firstSuccessTopic), Errors.NONE, None)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, secondSuccessTopic), Errors.NONE, None)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAdminManagerErrorMerge(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val succeedingTopicName = safeTopicName(data, "fuzz-cp-merge-ok")
    val failingTopicName = safeTopicName(data, "fuzz-cp-merge-bad")
    val adminFailureDetailMessage = "fuzz exists"
    val succeedingTopicTargetCount = data.consumeInt(2, 80)
    val failingTopicTargetCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(
      succeedingTopicName -> ApiError.NONE,
      failingTopicName -> ApiError.fromThrowable(new TopicExistsException(adminFailureDetailMessage))
    ))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(succeedingTopicName, succeedingTopicTargetCount),
      partitionTopic(failingTopicName, failingTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, succeedingTopicName), Errors.NONE, None)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, failingTopicName),
        Errors.TOPIC_ALREADY_EXISTS,
        Some(adminFailureDetailMessage))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsAllTopicsUnauthorizedEmptyValid(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val firstUnauthorizedTopic = safeTopicName(data, "fuzz-cp-none-a")
    val secondUnauthorizedTopic = safeTopicName(data, "fuzz-cp-none-b")
    val firstTopicTargetCount = data.consumeInt(2, 80)
    val secondTopicTargetCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(firstUnauthorizedTopic, firstTopicTargetCount),
      partitionTopic(secondUnauthorizedTopic, secondTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set.empty)))
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, firstUnauthorizedTopic),
        Errors.TOPIC_AUTHORIZATION_FAILED,
        Some(topicAuthorizationFailedMessage))
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, secondUnauthorizedTopic),
        Errors.TOPIC_AUTHORIZATION_FAILED,
        Some(topicAuthorizationFailedMessage))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsEmptyTopicsList(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map.empty)

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertEquals(0, response.data.results.size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsMixedDupAuthQueueAndAdmin(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val duplicateTopicName = safeTopicName(data, "fuzz-cp-mix-dup")
    val unauthorizedTopicName = safeTopicName(data, "fuzz-cp-mix-den")
    val topicQueuedForDeletionName = safeTopicName(data, "fuzz-cp-mix-q")
    val succeedingTopicName = safeTopicName(data, "fuzz-cp-mix-ok")
    val duplicateFirstTargetCount = data.consumeInt(2, 40)
    val duplicateSecondTargetCount = data.consumeInt(2, 40)
    val unauthorizedTopicTargetCount = data.consumeInt(2, 40)
    val queuedTopicTargetCount = data.consumeInt(2, 40)
    val succeedingTopicTargetCount = data.consumeInt(2, 40)

    resetZkAndCommonMocks()
    stubNoThrottle()
    stubControllerMutationQuota(UnboundedControllerMutationQuota)
    when(controller.isActive).thenReturn(true)
    when(controller.isTopicQueuedForDeletion(topicQueuedForDeletionName)).thenReturn(true)
    stubCreatePartitionsCallback(Map(succeedingTopicName -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly,
      partitionTopic(duplicateTopicName, duplicateFirstTargetCount),
      partitionTopic(duplicateTopicName, duplicateSecondTargetCount),
      partitionTopic(unauthorizedTopicName, unauthorizedTopicTargetCount),
      partitionTopic(topicQueuedForDeletionName, queuedTopicTargetCount),
      partitionTopic(succeedingTopicName, succeedingTopicTargetCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterTopicsAllowed(Set(succeedingTopicName, topicQueuedForDeletionName))))
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, duplicateTopicName),
        Errors.INVALID_REQUEST,
        Some(duplicateTopicInRequestMessage))
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, unauthorizedTopicName),
        Errors.TOPIC_AUTHORIZATION_FAILED,
        Some(topicAuthorizationFailedMessage))
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, topicQueuedForDeletionName),
        Errors.INVALID_TOPIC_EXCEPTION,
        Some(topicQueuedForDeletionMessage))
      assertCreatePartitionsTopicResult(
        topicResultOrThrow(response, succeedingTopicName),
        Errors.NONE,
        None)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsThrottling(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val controllerThrottleIsLower = data.consumeBoolean()
    val lowerThrottleBoundMs = data.consumeInt(1, 100)
    val higherThrottleBoundMs = data.consumeInt(101, 250)
    val (requestThrottleMs, controllerThrottleMs) =
      if (controllerThrottleIsLower) (lowerThrottleBoundMs, higherThrottleBoundMs)
      else (higherThrottleBoundMs, lowerThrottleBoundMs)
    val topicName = safeTopicName(data, "fuzz-cp-throttle")
    val targetPartitionCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val controllerMutationQuotaMock = mock(classOf[ControllerMutationQuota])
    when(controllerMutationQuotaMock.throttleTime).thenReturn(controllerThrottleMs)
    stubControllerMutationQuota(controllerMutationQuotaMock)

    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(topicName -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly, partitionTopic(topicName, targetPartitionCount))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, topicName), Errors.NONE, None)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreatePartitionsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(ApiKeys.CREATE_PARTITIONS.oldestVersion(), ApiKeys.CREATE_PARTITIONS.latestVersion())
    val timeoutMs = data.consumeInt(0, 120_000)
    val validateOnly = data.consumeBoolean()
    val requestThrottleMs = data.consumeInt(0, 100)
    val controllerThrottleMs = data.consumeInt(0, 100)
    val topicName = safeTopicName(data, "fuzz-cp-fwd")
    val targetPartitionCount = data.consumeInt(2, 80)

    resetZkAndCommonMocks()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val controllerMutationQuotaMock = mock(classOf[ControllerMutationQuota])
    when(controllerMutationQuotaMock.throttleTime).thenReturn(controllerThrottleMs)
    stubControllerMutationQuota(controllerMutationQuotaMock)

    when(controller.isActive).thenReturn(true)
    stubCreatePartitionsCallback(Map(topicName -> ApiError.NONE))

    val built = buildCreatePartitionsRequest(requestWireVersion, timeoutMs, validateOnly, partitionTopic(topicName, targetPartitionCount))
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleCreatePartitionsRequest(request)
      val response = verifyNoThrottling[CreatePartitionsResponse](request)
      assertCreatePartitionsTopicResult(topicResultOrThrow(response, topicName), Errors.NONE, None)
    } finally kafkaApis.close()
  }
}
