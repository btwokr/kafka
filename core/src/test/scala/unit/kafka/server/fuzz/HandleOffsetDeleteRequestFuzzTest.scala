/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the License); you may not use this file except in compliance with
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
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.message.OffsetDeleteRequestData
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.OffsetDeleteRequest
import org.apache.kafka.common.requests.OffsetDeleteResponse
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, never, reset, verify, when}

import java.nio.charset.StandardCharsets
import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleOffsetDeleteRequest`: group `DELETE`
 * authorization, per-topic `READ` filtering and metadata validation,
 * `groupCoordinator.deleteOffsets` success and failure paths, throttling, and
 * forwarded inner requests.
 */
class HandleOffsetDeleteRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val consumedString = data.consumeString(48)
    if (consumedString == null || consumedString.isEmpty) fallback else consumedString
  }

  private def groupIdSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def topicFromRaw(raw: String, fallback: String): String = {
    val base = if (raw == null || raw.isEmpty) fallback else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubClientThrottle(throttleTimeMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)
  }

  private def authorizerAllowAll(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val authorizationResults = new util.ArrayList[AuthorizationResult]()
      (0 until actions.size()).foreach(_ => authorizationResults.add(AuthorizationResult.ALLOWED))
      authorizationResults
    })
    authorizer
  }

  /** Denies `DELETE` on a specific consumer group id. */
  private def authorizerDenyGroupDelete(deniedGroupId: String): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val authorizationResults = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        val denied = action.operation == AclOperation.DELETE &&
          action.resourcePattern.resourceType == ResourceType.GROUP &&
          deniedGroupId == action.resourcePattern.name
        authorizationResults.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      authorizationResults
    })
    authorizer
  }

  /** Denies `READ` on a specific topic name (other actions allowed). */
  private def authorizerDenyTopicRead(deniedTopicName: String): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val authorizationResults = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        val denied = action.operation == AclOperation.READ &&
          action.resourcePattern.resourceType == ResourceType.TOPIC &&
          deniedTopicName == action.resourcePattern.name
        authorizationResults.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      authorizationResults
    })
    authorizer
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteGroupAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-deny-group")
    val topicName = topicSafe(data, "fuzz-od-deny-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topicName, numPartitions = 2)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(util.Arrays.asList(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1))))

    val built = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyGroupDelete(groupId)))
    try {
      kafkaApis.handleOffsetDeleteRequest(request, requestLocal).get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
    } finally {
      kafkaApis.close()
      verify(groupCoordinator, never()).deleteOffsets(any(), any(), any())
    }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteHappyPathCoordinatorMerge(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-happy-group")
    val topicName = topicSafe(data, "fuzz-od-happy-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topicName, numPartitions = 2)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(util.Arrays.asList(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      requestData,
      requestLocal.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val coordinatorPartitionResponse = new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
      new OffsetDeleteResponseTopic()
        .setName(topicName)
        .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
          new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code),
          new OffsetDeleteResponsePartition().setPartitionIndex(1).setErrorCode(Errors.NONE.code)
        ).asJava.iterator))
    ).asJava.iterator())

    val coordinatorResponseData = new OffsetDeleteResponseData().setTopics(coordinatorPartitionResponse)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, requestLocal)
      coordinatorFuture.complete(coordinatorResponseData)
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.NONE.code, response.data.errorCode)
      assertEquals(2, response.data.topics.find(topicName).partitions.size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteUnknownTopicInMetadata(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-unknown-group")
    val knownTopic = topicSafe(data, "fuzz-od-known-topic")
    val unknownTopic = topicSafe(data, "fuzz-od-unknown-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(knownTopic, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(unknownTopic)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))
    topics.add(new OffsetDeleteRequestTopic()
      .setName(knownTopic)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val expectedCoordinatorRequest = new OffsetDeleteRequestData()
      .setGroupId(groupId)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName(knownTopic)
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0)))
      ).asJava.iterator()))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      expectedCoordinatorRequest,
      requestLocal.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, requestLocal)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(knownTopic)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
        response.data.topics.find(unknownTopic).partitions.find(0).errorCode)
      assertEquals(Errors.NONE.code,
        response.data.topics.find(knownTopic).partitions.find(0).errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteTopicReadDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-tauth-group")
    val allowedTopic = topicSafe(data, "fuzz-od-allowed-topic")
    val deniedTopic = topicSafe(data, "fuzz-od-denied-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(allowedTopic, numPartitions = 1)
    addTopicToMetadataCache(deniedTopic, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(allowedTopic)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))
    topics.add(new OffsetDeleteRequestTopic()
      .setName(deniedTopic)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val expectedCoordinatorRequest = new OffsetDeleteRequestData()
      .setGroupId(groupId)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName(allowedTopic)
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0)))
      ).asJava.iterator()))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      expectedCoordinatorRequest,
      requestLocal.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTopicRead(deniedTopic)))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, requestLocal)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(allowedTopic)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code,
        response.data.topics.find(deniedTopic).partitions.find(0).errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteInvalidPartitionIndex(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-invpart-group")
    val topicName = topicSafe(data, "fuzz-od-invpart-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topicName, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(util.Arrays.asList(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(7))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val expectedCoordinatorRequest = new OffsetDeleteRequestData()
      .setGroupId(groupId)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0)))
      ).asJava.iterator()))

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      expectedCoordinatorRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(topicName)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
        response.data.topics.find(topicName).partitions.find(7).errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteCoordinatorException(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-exc-group")
    val topicName = topicSafe(data, "fuzz-od-exc-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topicName, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      requestData,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.completeExceptionally(Errors.GROUP_ID_NOT_FOUND.exception())
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(Errors.GROUP_ID_NOT_FOUND.code, response.data.errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteEmptyTopicsCallsCoordinator(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-empty-group")

    resetHarness()
    stubNoThrottle()

    val emptyTopics = new OffsetDeleteRequestTopicCollection()
    val requestData = new OffsetDeleteRequestData()
      .setGroupId(groupId)
      .setTopics(emptyTopics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val expectedCoordinatorRequest = new OffsetDeleteRequestData()
      .setGroupId(groupId)
      .setTopics(new OffsetDeleteRequestTopicCollection())

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      expectedCoordinatorRequest,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetDeleteResponseData())
      handleFuture.get()
      verifyNoThrottling[OffsetDeleteResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val groupId = groupIdSafe(data, "fuzz-od-throttle-group")
    val topicName = topicSafe(data, "fuzz-od-throttle-topic")

    resetHarness()
    stubClientThrottle(throttleTimeMs)
    addTopicToMetadataCache(topicName, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      requestData,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(topicName)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteForwardedInnerThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val throttleTimeMs = data.consumeInt(0, 200)
    val groupId = groupIdSafe(data, "fuzz-od-fwd-group")
    val topicName = topicSafe(data, "fuzz-od-fwd-topic")

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)

    addTopicToMetadataCache(topicName, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(built)

    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      requestData,
      RequestLocal.NoCaching.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(topicName)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetDeleteTopicFromFuzzTail(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_DELETE.oldestVersion(), ApiKeys.OFFSET_DELETE.latestVersion())
    val groupId = groupIdSafe(data, "fuzz-od-tail-group")
    val splitSize = data.consumeInt(1, 64)
    val tailBytes = data.consumeRemainingAsBytes()
    val rawTopicByteLength = if (tailBytes == null || tailBytes.isEmpty) 0 else Math.min(splitSize, tailBytes.length)
    val rawTopicPrefix =
      if (rawTopicByteLength == 0) ""
      else new String(tailBytes, 0, rawTopicByteLength, StandardCharsets.UTF_8)
    val topicName = topicFromRaw(rawTopicPrefix, "fuzz-od-tail-topic")

    resetHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topicName, numPartitions = 1)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName(topicName)
      .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition().setPartitionIndex(0))))

    val requestData = new OffsetDeleteRequestData().setGroupId(groupId).setTopics(topics)
    val built = new OffsetDeleteRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val coordinatorFuture = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      requestData,
      requestLocal.bufferSupplier
    )).thenReturn(coordinatorFuture)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      val handleFuture = kafkaApis.handleOffsetDeleteRequest(request, requestLocal)
      coordinatorFuture.complete(new OffsetDeleteResponseData()
        .setTopics(new OffsetDeleteResponseTopicCollection(List(
          new OffsetDeleteResponseTopic()
            .setName(topicName)
            .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
              new OffsetDeleteResponsePartition().setPartitionIndex(0).setErrorCode(Errors.NONE.code)
            ).asJava.iterator))
        ).asJava.iterator)))
      handleFuture.get()
      val response = verifyNoThrottling[OffsetDeleteResponse](request)
      assertEquals(topicName, response.data.topics.find(topicName).name)
    } finally kafkaApis.close()
  }
}
