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
import org.apache.kafka.common.message.ShareFetchRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareFetchRequest, ShareFetchResponse}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests for `KafkaApis.handleShareFetchRequest`.
 *
 * The handler is currently a stub: it always responds with
 * `UNSUPPORTED_VERSION` via `getErrorResponse` and `sendMaybeThrottle`. These
 * tests cover that path with and without client throttling, forwarded requests,
 * and fuzzed `ShareFetchRequest` payloads (topics, partitions, forgotten
 * topics). ZooKeeper and KRaft harnesses are both exercised; the handler does
 * not branch on metadata mode.
 */
class HandleShareFetchRequestFuzzTest extends KafkaApisTest {

  private def resetZkHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def resetKRaftHarness(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
  }

  private def groupIdSafe(data: FuzzedDataProvider, fallback: String): String = {
    val s = data.consumeString(96)
    if (s == null || s.isEmpty) fallback
    else s.replace('\n', ' ').replace('\r', ' ').take(256)
  }

  private def buildRequestData(data: FuzzedDataProvider): ShareFetchRequestData = {
    val d = new ShareFetchRequestData()
      .setGroupId(groupIdSafe(data, "fuzz-share-fetch-group"))
      .setMemberId(groupIdSafe(data, "fuzz-share-fetch-member"))
      .setShareSessionEpoch(data.consumeInt(-2, 100))
      .setMaxWaitMs(data.consumeInt(0, 500))
      .setMinBytes(data.consumeInt(0, 10_000))
      .setMaxBytes(data.consumeInt(1000, 50_000_000))
    val numTopics = data.consumeInt(0, 3)
    var ti = 0
    while (ti < numTopics) {
      val ft = new ShareFetchRequestData.FetchTopic().setTopicId(uuidFromTwoLongs(data))
      val np = data.consumeInt(0, 4)
      var pi = 0
      while (pi < np) {
        ft.partitions().add(new ShareFetchRequestData.FetchPartition()
          .setPartitionIndex(data.consumeInt(0, 16))
          .setPartitionMaxBytes(data.consumeInt(1, 1024 * 1024)))
        pi += 1
      }
      d.topics().add(ft)
      ti += 1
    }
    if (data.consumeBoolean()) {
      val forgotten = new ShareFetchRequestData.ForgottenTopic().setTopicId(uuidFromTwoLongs(data))
      forgotten.partitions().add(data.consumeInt(0, 7))
      d.forgottenTopicsData().add(forgotten)
    }
    d
  }

  private def buildShareFetchRequest(data: FuzzedDataProvider): ShareFetchRequest = {
    val version = ApiKeys.SHARE_FETCH.latestVersion()
    new ShareFetchRequest.Builder(buildRequestData(data), true).build(version)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareFetchUnsupportedVersionKRaft(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareFetchRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareFetchRequest(request)
      val resp = verifyNoThrottling[ShareFetchResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(Errors.UNSUPPORTED_VERSION.message, Errors.forCode(resp.data().errorCode).message)
      assertEquals(0, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareFetchUnsupportedVersionZk(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareFetchRequest(data)
    val request = buildRequest(shareReq)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleShareFetchRequest(request)
      val resp = verifyNoThrottling[ShareFetchResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(Errors.UNSUPPORTED_VERSION.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareFetchUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareFetchRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareFetchRequest(request)
      val resp = verifyNoThrottling[ShareFetchResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareFetchUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val shareReq = buildShareFetchRequest(data)
    val request = buildForwardedRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareFetchRequest(request)
      val resp = verifyNoThrottling[ShareFetchResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }
}
