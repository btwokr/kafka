/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import kafka.controller.KafkaController
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiError, ElectLeadersRequest, ElectLeadersResponse}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{doAnswer, mock, reset, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleElectLeaders`.
 *
 * The handler either rejects the request with `CLUSTER_AUTHORIZATION_FAILED`
 * (mapping the error onto every explicitly listed partition), or, when
 * authorized, forwards to `ReplicaManager.electLeaders` and builds an
 * `ElectLeadersResponse` from the callback. When the request omits a partition
 * list (`topicPartitions == null` in the wire message), partition results
 * omit entries whose error is `ELECTION_NOT_NEEDED`; explicit lists keep all
 * entries.
 */
class HandleElectLeadersRequestFuzzTest extends KafkaApisTest {

  private def resetZkMetadataToLatestTesting(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  }

  private def authorizerUniform(result: AuthorizationResult): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer { inv =>
      val actions = inv.getArgument(1, classOf[util.List[Action]])
      util.Collections.nCopies(actions.size, result)
    }
    authorizer
  }

  private def stubThrottle(throttleTimeMs: Int): Unit = {
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)
  }

  private def stubElectLeadersInvoke(electionResults: Map[TopicPartition, ApiError]): Unit = {
    doAnswer { inv =>
      val callback = inv.getArgument(3).asInstanceOf[Map[TopicPartition, ApiError] => Unit]
      callback(electionResults)
      null
    }.when(replicaManager).electLeaders(
      any[KafkaController](),
      any(),
      any(),
      any(),
      anyInt()
    )
  }

  private def safeTopicLabel(raw: String, fallbackSuffix: Int): String = {
    val base = if (raw == null || raw.isEmpty) s"fuzz-el-$fallbackSuffix" else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
  }

  private def partitionErrorCodes(response: ElectLeadersResponse): Map[TopicPartition, Short] = {
    response.data.replicaElectionResults.iterator.asScala.flatMap { topicResult =>
      topicResult.partitionResult.iterator.asScala.map { partitionResult =>
        new TopicPartition(topicResult.topic, partitionResult.partitionId) -> partitionResult.errorCode
      }
    }.toMap
  }

  /** Top-level `ErrorCode` exists only on ElectLeaders response v1+. */
  private def assertTopLevelErrorIfPresent(
    requestVersion: Short,
    response: ElectLeadersResponse,
    expected: Errors
  ): Unit = {
    if (requestVersion >= 1)
      assertEquals(expected.code, response.data.errorCode)
  }

  /** API v0 only allows `PREFERRED`; unclean elections need v1+. */
  private def requestVersionAndElectionType(
    requestVersion: Short,
    preferUncleanElection: Boolean
  ): (Short, ElectionType) = {
    if (requestVersion == 0)
      (0, ElectionType.PREFERRED)
    else if (preferUncleanElection)
      (requestVersion, ElectionType.UNCLEAN)
    else
      (requestVersion, ElectionType.PREFERRED)
  }

  /**
   * Denied `ALTER` on `CLUSTER` with an explicit partition list: every
   * listed partition receives `CLUSTER_AUTHORIZATION_FAILED`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersClusterAuthDeniedExplicitPartitions(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)
    val topicName1 = safeTopicLabel(data.consumeString(64), 1)
    val topicName2 = safeTopicLabel(data.consumeString(64), 2)
    val partitionId1 = data.consumeInt(0, 64)
    val partitionId2 = data.consumeInt(0, 64)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val topicPartition1 = new TopicPartition(topicName1, partitionId1)
    val topicPartition2 = new TopicPartition(topicName2, partitionId2)
    val requestedPartitions = util.Arrays.asList(topicPartition1, topicPartition2)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType, requestedPartitions, electionTimeoutMs).build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)

    val deniedAuthorizer = authorizerUniform(AuthorizationResult.DENIED)
    val kafkaApis = createKafkaApis(authorizer = Some(deniedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.CLUSTER_AUTHORIZATION_FAILED)
      val errorsByPartition = partitionErrorCodes(response)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, errorsByPartition(topicPartition1))
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, errorsByPartition(topicPartition2))
    } finally kafkaApis.close()
  }

  /**
   * Same denial path when the client asks for elections across all partitions
   * (`topicPartitions == null`): the top-level error is still set while the
   * per-partition map is empty.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersClusterAuthDeniedAllPartitions(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val electLeadersRequest = new ElectLeadersRequest.Builder(electionType, null, electionTimeoutMs)
      .build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)

    val deniedAuthorizer = authorizerUniform(AuthorizationResult.DENIED)
    val kafkaApis = createKafkaApis(authorizer = Some(deniedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.CLUSTER_AUTHORIZATION_FAILED)
      assertTrue(response.data.replicaElectionResults.isEmpty)
    } finally kafkaApis.close()
  }

  /**
   * Authorized, elect-all mode: partition set comes from `MetadataCache`; the
   * response builder drops `ELECTION_NOT_NEEDED` when the request used a null
   * topic-partition list.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedAllPartitionsFiltersNotNeeded(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)
    val topicName1 = safeTopicLabel(data.consumeString(64), 1)
    val topicName2 = safeTopicLabel(data.consumeString(64), 2)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val electLeadersRequest = new ElectLeadersRequest.Builder(electionType, null, electionTimeoutMs)
      .build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    addTopicToMetadataCache(topicName1, numPartitions = 2, numBrokers = 2)
    addTopicToMetadataCache(topicName2, numPartitions = 2, numBrokers = 2)
    val cachedTopicPartition1 = new TopicPartition(topicName1, 0)
    val cachedTopicPartition2 = new TopicPartition(topicName2, 1)
    val electionCallbackResults = Map(
      cachedTopicPartition1 -> new ApiError(Errors.ELECTION_NOT_NEEDED),
      cachedTopicPartition2 -> new ApiError(Errors.NONE)
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(electionCallbackResults)

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      val errorsByPartition = partitionErrorCodes(response)
      assertFalse(errorsByPartition.contains(cachedTopicPartition1))
      assertEquals(Errors.NONE.code, errorsByPartition(cachedTopicPartition2))
    } finally kafkaApis.close()
  }

  /**
   * Explicit partition list: `ELECTION_NOT_NEEDED` is not filtered from the
   * callback results (only the elect-all path applies the filter).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedExplicitKeepsNotNeeded(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)
    val topicName1 = safeTopicLabel(data.consumeString(64), 1)
    val topicName2 = safeTopicLabel(data.consumeString(64), 2)
    val partitionId1 = data.consumeInt(0, 32)
    val partitionId2 = data.consumeInt(0, 32)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val topicPartition1 = new TopicPartition(topicName1, partitionId1)
    val topicPartition2 = new TopicPartition(topicName2, partitionId2)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType,
      util.Arrays.asList(topicPartition1, topicPartition2),
      electionTimeoutMs
    ).build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    val electionCallbackResults = Map(
      topicPartition1 -> new ApiError(Errors.ELECTION_NOT_NEEDED),
      topicPartition2 -> new ApiError(Errors.NOT_LEADER_OR_FOLLOWER, "fuzz-msg")
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(electionCallbackResults)

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      val errorsByPartition = partitionErrorCodes(response)
      assertEquals(Errors.ELECTION_NOT_NEEDED.code, errorsByPartition(topicPartition1))
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, errorsByPartition(topicPartition2))
    } finally kafkaApis.close()
  }

  /**
   * Authorized with an empty explicit partition collection: `ReplicaManager`
   * still receives an empty set.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedEmptyExplicitPartitions(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType,
      Collections.emptyList(),
      electionTimeoutMs
    ).build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(Map.empty)

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      assertTrue(partitionErrorCodes(response).isEmpty)
    } finally kafkaApis.close()
  }

  /**
   * Non-zero request quota throttle time is threaded through
   * `sendResponseMaybeThrottle` on the success path.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersThrottled(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(1, 500)
    val topicName1 = safeTopicLabel(data.consumeString(64), 1)
    val partitionId1 = data.consumeInt(0, 16)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val topicPartition1 = new TopicPartition(topicName1, partitionId1)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType,
      Collections.singletonList(topicPartition1),
      electionTimeoutMs
    ).build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(Map(topicPartition1 -> ApiError.NONE))

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      assertEquals(Errors.NONE.code, partitionErrorCodes(response)(topicPartition1))
    } finally kafkaApis.close()
  }

  /**
   * Forwarded inner requests skip channel throttling in `sendResponseMaybeThrottle`
   * while still recording throttle time on the response.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersForwardedSkipsChannelThrottle(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(1, 500)
    val topicName1 = safeTopicLabel(data.consumeString(64), 1)
    val partitionId1 = data.consumeInt(0, 16)

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val topicPartition1 = new TopicPartition(topicName1, partitionId1)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType,
      Collections.singletonList(topicPartition1),
      electionTimeoutMs
    ).build(requestVersion)
    val request = buildForwardedRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(Map(topicPartition1 -> ApiError.NONE))

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      assertEquals(Errors.NONE.code, partitionErrorCodes(response)(topicPartition1))
    } finally kafkaApis.close()
  }

  /**
   * Fuzz-derived topic strings via `consumeRemainingAsBytes` (must be the last
   * `FuzzedDataProvider` use). Exercises multi-topic grouping in the response.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedExplicitFromRemainingBytes(data: FuzzedDataProvider): Unit = {
    val requestVersionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUncleanElection = data.consumeBoolean()
    val electionTimeoutMs = data.consumeInt(0, 120_000)
    val throttleTimeMs = data.consumeInt(0, 100)
    val firstTopicBytesLength = data.consumeInt(1, 32)
    val secondTopicBytesLength = data.consumeInt(1, 32)
    val partitionId1 = data.consumeInt(0, 8)
    val partitionId2 = data.consumeInt(0, 8)
    val remainingRawBytes = data.consumeRemainingAsBytes()

    val (requestVersion, electionType) = requestVersionAndElectionType(requestVersionRaw, preferUncleanElection)
    val (firstTopicBytes, restAfterFirst) =
      remainingRawBytes.splitAt(firstTopicBytesLength min remainingRawBytes.length)
    val (secondTopicBytes, _) =
      restAfterFirst.splitAt(secondTopicBytesLength min restAfterFirst.length)
    val topicName1 = safeTopicLabel(
      new String(firstTopicBytes, java.nio.charset.StandardCharsets.UTF_8), 1)
    val topicName2 = safeTopicLabel(
      new String(secondTopicBytes, java.nio.charset.StandardCharsets.UTF_8), 2)
    val topicPartition1 = new TopicPartition(topicName1, partitionId1)
    val topicPartition2 = new TopicPartition(topicName2, partitionId2)
    val electLeadersRequest = new ElectLeadersRequest.Builder(
      electionType,
      util.Arrays.asList(topicPartition1, topicPartition2),
      electionTimeoutMs
    ).build(requestVersion)
    val request = buildRequest(electLeadersRequest)

    resetZkMetadataToLatestTesting()
    val electionCallbackResults = Map(
      topicPartition1 -> new ApiError(Errors.KAFKA_STORAGE_ERROR),
      topicPartition2 -> ApiError.NONE
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleTimeMs)
    stubElectLeadersInvoke(electionCallbackResults)

    val allowedAuthorizer = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowedAuthorizer))
    try {
      kafkaApis.handleElectLeaders(request)
      val response = verifyNoThrottling[ElectLeadersResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertTopLevelErrorIfPresent(requestVersion, response, Errors.NONE)
      val errorsByPartition = partitionErrorCodes(response)
      assertEquals(Errors.KAFKA_STORAGE_ERROR.code, errorsByPartition(topicPartition1))
      assertEquals(Errors.NONE.code, errorsByPartition(topicPartition2))
    } finally kafkaApis.close()
  }
}
