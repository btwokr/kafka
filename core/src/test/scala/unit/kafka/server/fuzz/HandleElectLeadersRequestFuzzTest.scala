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
import org.apache.kafka.common.requests.{ApiError, ElectLeadersRequest}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{doAnswer, mock, reset, when}

import java.util
import java.util.Collections

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

  private def stubThrottle(throttleMs: Int): Unit = {
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
  }

  private def stubElectLeadersInvoke(results: Map[TopicPartition, ApiError]): Unit = {
    doAnswer { inv =>
      val callback = inv.getArgument(3).asInstanceOf[Map[TopicPartition, ApiError] => Unit]
      callback(results)
      null
    }.when(replicaManager).electLeaders(
      any[KafkaController](),
      any(),
      any(),
      any(),
      anyInt()
    )
  }

  private def safeTopicLabel(raw: String, idx: Int): String = {
    val base = if (raw == null || raw.isEmpty) s"fuzz-el-$idx" else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
  }

  /** API v0 only allows `PREFERRED`; unclean elections need v1+. */
  private def versionAndElectionType(
    version: Short,
    preferUnclean: Boolean
  ): (Short, ElectionType) = {
    if (version == 0)
      (0, ElectionType.PREFERRED)
    else if (preferUnclean)
      (version, ElectionType.UNCLEAN)
    else
      (version, ElectionType.PREFERRED)
  }

  /**
   * Denied `ALTER` on `CLUSTER` with an explicit partition list: every
   * listed partition receives `CLUSTER_AUTHORIZATION_FAILED`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersClusterAuthDeniedExplicitPartitions(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)
    val t1 = safeTopicLabel(data.consumeString(64), 1)
    val t2 = safeTopicLabel(data.consumeString(64), 2)
    val p1 = data.consumeInt(0, 64)
    val p2 = data.consumeInt(0, 64)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val tpA = new TopicPartition(t1, p1)
    val tpB = new TopicPartition(t2, p2)
    val partitions = util.Arrays.asList(tpA, tpB)
    val electReq = new ElectLeadersRequest.Builder(electionType, partitions, timeoutMs).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)

    val denied = authorizerUniform(AuthorizationResult.DENIED)
    val kafkaApis = createKafkaApis(authorizer = Some(denied))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Same denial path when the client asks for elections across all partitions
   * (`topicPartitions == null`): the top-level error is still set while the
   * per-partition map is empty.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersClusterAuthDeniedAllPartitions(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val electReq = new ElectLeadersRequest.Builder(electionType, null, timeoutMs).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)

    val denied = authorizerUniform(AuthorizationResult.DENIED)
    val kafkaApis = createKafkaApis(authorizer = Some(denied))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Authorized, elect-all mode: partition set comes from `MetadataCache`; the
   * response builder drops `ELECTION_NOT_NEEDED` when the request used a null
   * topic-partition list.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedAllPartitionsFiltersNotNeeded(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)
    val t1 = safeTopicLabel(data.consumeString(64), 1)
    val t2 = safeTopicLabel(data.consumeString(64), 2)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val electReq = new ElectLeadersRequest.Builder(electionType, null, timeoutMs).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    addTopicToMetadataCache(t1, numPartitions = 2, numBrokers = 2)
    addTopicToMetadataCache(t2, numPartitions = 2, numBrokers = 2)
    val tpA = new TopicPartition(t1, 0)
    val tpB = new TopicPartition(t2, 1)
    val callbackMap = Map(
      tpA -> new ApiError(Errors.ELECTION_NOT_NEEDED),
      tpB -> new ApiError(Errors.NONE)
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(callbackMap)

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Explicit partition list: `ELECTION_NOT_NEEDED` is not filtered from the
   * callback results (only the elect-all path applies the filter).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedExplicitKeepsNotNeeded(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)
    val t1 = safeTopicLabel(data.consumeString(64), 1)
    val t2 = safeTopicLabel(data.consumeString(64), 2)
    val p1 = data.consumeInt(0, 32)
    val p2 = data.consumeInt(0, 32)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val tpA = new TopicPartition(t1, p1)
    val tpB = new TopicPartition(t2, p2)
    val electReq = new ElectLeadersRequest.Builder(
      electionType,
      util.Arrays.asList(tpA, tpB),
      timeoutMs
    ).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    val callbackMap = Map(
      tpA -> new ApiError(Errors.ELECTION_NOT_NEEDED),
      tpB -> new ApiError(Errors.NOT_LEADER_OR_FOLLOWER, "fuzz-msg")
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(callbackMap)

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Authorized with an empty explicit partition collection: `ReplicaManager`
   * still receives an empty set.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedEmptyExplicitPartitions(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val electReq = new ElectLeadersRequest.Builder(
      electionType,
      Collections.emptyList(),
      timeoutMs
    ).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(Map.empty)

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Non-zero request quota throttle time is threaded through
   * `sendResponseMaybeThrottle` on the success path.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersThrottled(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(1, 500)
    val t1 = safeTopicLabel(data.consumeString(64), 1)
    val p1 = data.consumeInt(0, 16)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val tp = new TopicPartition(t1, p1)
    val electReq = new ElectLeadersRequest.Builder(
      electionType,
      Collections.singletonList(tp),
      timeoutMs
    ).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(Map(tp -> ApiError.NONE))

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Forwarded inner requests skip channel throttling in `sendResponseMaybeThrottle`
   * while still recording throttle time on the response.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersForwardedSkipsChannelThrottle(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(1, 500)
    val t1 = safeTopicLabel(data.consumeString(64), 1)
    val p1 = data.consumeInt(0, 16)

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val tp = new TopicPartition(t1, p1)
    val electReq = new ElectLeadersRequest.Builder(
      electionType,
      Collections.singletonList(tp),
      timeoutMs
    ).build(version)
    val request = buildForwardedRequest(electReq)

    resetZkMetadataToLatestTesting()
    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(Map(tp -> ApiError.NONE))

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }

  /**
   * Fuzz-derived topic strings via `consumeRemainingAsBytes` (must be the last
   * `FuzzedDataProvider` use). Exercises multi-topic grouping in the response.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestElectLeadersAuthorizedExplicitFromRemainingBytes(data: FuzzedDataProvider): Unit = {
    val versionRaw = data.consumeShort(
      ApiKeys.ELECT_LEADERS.oldestVersion(),
      ApiKeys.ELECT_LEADERS.latestVersion()
    )
    val preferUnclean = data.consumeBoolean()
    val timeoutMs = data.consumeInt(0, 120_000)
    val throttleMs = data.consumeInt(0, 100)
    val splitA = data.consumeInt(1, 32)
    val splitB = data.consumeInt(1, 32)
    val p1 = data.consumeInt(0, 8)
    val p2 = data.consumeInt(0, 8)
    val rawBytes = data.consumeRemainingAsBytes()

    val (version, electionType) = versionAndElectionType(versionRaw, preferUnclean)
    val (s1, rest1) = rawBytes.splitAt(splitA min rawBytes.length)
    val (s2, _) = rest1.splitAt(splitB min rest1.length)
    val t1 = safeTopicLabel(new String(s1, java.nio.charset.StandardCharsets.UTF_8), 1)
    val t2 = safeTopicLabel(new String(s2, java.nio.charset.StandardCharsets.UTF_8), 2)
    val tpA = new TopicPartition(t1, p1)
    val tpB = new TopicPartition(t2, p2)
    val electReq = new ElectLeadersRequest.Builder(
      electionType,
      util.Arrays.asList(tpA, tpB),
      timeoutMs
    ).build(version)
    val request = buildRequest(electReq)

    resetZkMetadataToLatestTesting()
    val callbackMap = Map(
      tpA -> new ApiError(Errors.KAFKA_STORAGE_ERROR),
      tpB -> ApiError.NONE
    )

    reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    stubThrottle(throttleMs)
    stubElectLeadersInvoke(callbackMap)

    val allowed = authorizerUniform(AuthorizationResult.ALLOWED)
    val kafkaApis = createKafkaApis(authorizer = Some(allowed))
    try kafkaApis.handleElectLeaders(request)
    finally kafkaApis.close()
  }
}
