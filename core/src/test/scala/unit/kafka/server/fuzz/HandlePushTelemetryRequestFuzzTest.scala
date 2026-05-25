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
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.PushTelemetryRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{PushTelemetryRequest, PushTelemetryResponse, RequestContext}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests for `KafkaApis.handlePushTelemetryRequest`.
 *
 * Covers the ZooKeeper path (`clientMetricsManager == None`, unsupported
 * version error via `getErrorResponse`), the KRaft path (`Some(manager)` with
 * `processPushTelemetryRequest` success, non-throwing error responses from
 * the manager, and the broad `catch` that maps any exception to
 * `INVALID_REQUEST`), plus `sendMaybeThrottle` throttling and forwarded
 * requests.
 */
class HandlePushTelemetryRequestFuzzTest extends KafkaApisTest {

  private def resetZkHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def resetKRaftTelemetryHarness(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
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

  private def stubClientMetricsSuccess(): Unit = {
    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
  }

  /**
   * Compression ids must stay in `CompressionType.forId` range so nothing on
   * the request path throws before `KafkaApis` runs.
   */
  private def buildRequestData(data: FuzzedDataProvider): PushTelemetryRequestData = {
    val metricsLen = data.consumeInt(0, 512)
    new PushTelemetryRequestData()
      .setClientInstanceId(uuidFromTwoLongs(data))
      .setSubscriptionId(data.consumeInt(0, Int.MaxValue))
      .setTerminating(data.consumeBoolean())
      .setCompressionType(data.consumeInt(0, 4).toByte)
      .setMetrics(data.consumeBytes(metricsLen))
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(0, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryZkUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val throttleMs = data.consumeInt(1, 100)
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryZkUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val throttleMs = data.consumeInt(0, 100)
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildForwardedRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryKRaftSuccess(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    val responseData = new org.apache.kafka.common.message.PushTelemetryResponseData()
      .setErrorCode(Errors.NONE.code)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processPushTelemetryRequest(
      any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(responseData))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryKRaftProcessThrowsMapsToInvalidRequest(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processPushTelemetryRequest(
      any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("fuzz-push-telemetry-process"))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.INVALID_REQUEST.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryKRaftProcessThrowsInvalidRequestException(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val detail = data.consumeString(48)
    val safeDetail = if (detail == null || detail.isEmpty) "fuzz-invalid-detail" else detail.replace('\n', ' ').take(80)
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processPushTelemetryRequest(
      any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new InvalidRequestException(s"PushTelemetryRequest: $safeDetail"))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(Errors.INVALID_REQUEST.code, resp.data().errorCode)
      assertEquals(Errors.INVALID_REQUEST.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryKRaftManagerReturnsErrorCode(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val error = Errors.values()(data.consumeInt(0, Errors.values().length - 1))
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    val responseData = new org.apache.kafka.common.message.PushTelemetryResponseData()
      .setErrorCode(error.code)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processPushTelemetryRequest(
      any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(responseData))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(error.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestPushTelemetryKRaftForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.PUSH_TELEMETRY.latestVersion()
    val throttleMs = data.consumeInt(1, 100)
    val requestData = buildRequestData(data)
    val built = new PushTelemetryRequest.Builder(requestData, true).build(version)
    val request = buildForwardedRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processPushTelemetryRequest(
      any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(
        new org.apache.kafka.common.message.PushTelemetryResponseData()))
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handlePushTelemetryRequest(request)
      val resp = verifyNoThrottling[PushTelemetryResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }
}
