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
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.message.ShareAcknowledgeRequestData
import org.apache.kafka.common.message.ShareAcknowledgeRequestData.{AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareAcknowledgeRequest, ShareAcknowledgeResponse}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

import java.{lang => jl}
import java.util

/**
 * Jazzer fuzz tests for `KafkaApis.handleShareAcknowledgeRequest`.
 *
 * The handler is currently a stub: it reads [[ShareAcknowledgeRequest]] from the
 * body, responds with `getErrorResponse(Errors.UNSUPPORTED_VERSION.exception)`,
 * and forwards the result through `RequestHandlerHelper.sendMaybeThrottle`.
 * These tests therefore cover the stub path on both ZK- and KRaft-backed broker
 * harnesses, client throttling, forwarded inner requests, and a verified
 * top-level `UNSUPPORTED_VERSION` response while fuzzing arbitrary wire-shaped
 * request payloads (group metadata, topic IDs, partitions, acknowledgement
 * batches).
 */
class HandleShareAcknowledgeRequestFuzzTest extends KafkaApisTest {

  private def resetZkHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def resetKRaftHarness(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
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

  private def safeIdString(data: FuzzedDataProvider, maxLen: Int, fallback: String): String = {
    val s = data.consumeString(maxLen)
    if (s == null || s.isEmpty) fallback
    else s
  }

  /**
   * Builds arbitrary (but structurally valid) [[ShareAcknowledgeRequestData]] so
   * serialization and `request.body[ShareAcknowledgeRequest]` parsing stay in range.
   */
  private def fuzzShareAcknowledgeRequestData(data: FuzzedDataProvider): ShareAcknowledgeRequestData = {
    val groupId = safeIdString(data, 64, "fuzz-share-ack-group")
    val memberId = safeIdString(data, 128, "fuzz-share-ack-member")
    val shareSessionEpoch = data.consumeInt(-1, Int.MaxValue)
    val numTopics = data.consumeInt(0, 8)

    val requestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setShareSessionEpoch(shareSessionEpoch)

    var ti = 0
    while (ti < numTopics) {
      val topicId = uuidFromTwoLongs(data)
      val numPartitions = data.consumeInt(0, 6)
      val topic = new AcknowledgeTopic().setTopicId(topicId)
      var pi = 0
      while (pi < numPartitions) {
        val partitionIndex = data.consumeInt(0, 32)
        val numBatches = data.consumeInt(0, 6)
        val partition = new AcknowledgePartition().setPartitionIndex(partitionIndex)
        var bi = 0
        while (bi < numBatches) {
          val firstOffset = data.consumeLong(0L, 1_000_000L)
          val lastOffset = data.consumeLong(0L, 1_000_000L)
          val numTypes = data.consumeInt(0, 5)
          val types = new util.ArrayList[jl.Byte]()
          var ki = 0
          while (ki < numTypes) {
            types.add(jl.Byte.valueOf(data.consumeByte(0, 3)))
            ki += 1
          }
          partition.acknowledgementBatches().add(
            new AcknowledgementBatch()
              .setFirstOffset(firstOffset)
              .setLastOffset(lastOffset)
              .setAcknowledgeTypes(types))
          bi += 1
        }
        topic.partitions().add(partition)
        pi += 1
      }
      requestData.topics().add(topic)
      ti += 1
    }
    requestData
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareAcknowledgeKRaftUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.SHARE_ACKNOWLEDGE.oldestVersion(),
      ApiKeys.SHARE_ACKNOWLEDGE.latestVersion(true))
    val requestData = fuzzShareAcknowledgeRequestData(data)
    val built = new ShareAcknowledgeRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try kafkaApis.handleShareAcknowledgeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareAcknowledgeZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.SHARE_ACKNOWLEDGE.oldestVersion(),
      ApiKeys.SHARE_ACKNOWLEDGE.latestVersion(true))
    val requestData = fuzzShareAcknowledgeRequestData(data)
    val built = new ShareAcknowledgeRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleShareAcknowledgeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareAcknowledgeVerifiedUnsupportedVersionResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.SHARE_ACKNOWLEDGE.oldestVersion(),
      ApiKeys.SHARE_ACKNOWLEDGE.latestVersion(true))
    val requestData = fuzzShareAcknowledgeRequestData(data)
    val built = new ShareAcknowledgeRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleShareAcknowledgeRequest(request)
      val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareAcknowledgeThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.SHARE_ACKNOWLEDGE.oldestVersion(),
      ApiKeys.SHARE_ACKNOWLEDGE.latestVersion(true))
    val throttleMs = data.consumeInt(1, 500)
    val requestData = fuzzShareAcknowledgeRequestData(data)
    val built = new ShareAcknowledgeRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleShareAcknowledgeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareAcknowledgeForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.SHARE_ACKNOWLEDGE.oldestVersion(),
      ApiKeys.SHARE_ACKNOWLEDGE.latestVersion(true))
    val requestThrottleMs = data.consumeInt(0, 200)
    val requestData = fuzzShareAcknowledgeRequestData(data)
    val built = new ShareAcknowledgeRequest.Builder(requestData, true).build(version)
    val request = buildForwardedRequest(built)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleShareAcknowledgeRequest(request)
    finally kafkaApis.close()
  }
}
