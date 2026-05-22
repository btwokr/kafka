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
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse, RequestContext}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleGetTelemetrySubscriptionsRequest`.
 *
 * Covers the ZooKeeper path (`clientMetricsManager == None`, unsupported
 * version error via `getErrorResponse`), the KRaft path (`Some(manager)` with
 * `processGetTelemetrySubscriptionRequest` success, non-throwing error
 * responses from the manager, and the broad `catch` that maps any
 * exception to `INVALID_REQUEST`), plus `sendMaybeThrottle` throttling and
 * forwarded requests. Wire-level `UnsupportedVersionException` from
 * `AbstractRequest` construction is validated by message substring, matching
 * other fuzz tests that rethrow when the message is unexpected.
 */
class HandleGetTelemetrySubscriptionsRequestFuzzTest extends KafkaApisTest {

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

  private def validateUnsupportedVersionExceptionMessage(e: UnsupportedVersionException): Unit = {
    if (!e.getMessage.contains(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.toString)) throw e
  }

  private def stubClientMetricsSuccess(): Unit = {
    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
  }

  private def buildRequestData(data: FuzzedDataProvider): GetTelemetrySubscriptionsRequestData = {
    new GetTelemetrySubscriptionsRequestData()
      .setClientInstanceId(uuidFromTwoLongs(data))
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(0, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsZkUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val throttleMs = data.consumeInt(1, 100)
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsZkUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val throttleMs = data.consumeInt(0, 100)
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildForwardedRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsKRaftSuccess(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val subscriptionId = data.consumeInt(1, Int.MaxValue)
    val pushInterval = data.consumeInt(1, 600_000)
    val telemetryMaxBytes = data.consumeInt(1024, 8 * 1024 * 1024)
    val delta = data.consumeBoolean()
    val numMetricPrefixes = data.consumeInt(0, 4)
    val metrics = new java.util.ArrayList[String]()
    var i = 0
    while (i < numMetricPrefixes) {
      metrics.add(data.consumeString(32))
      i += 1
    }

    val responseData = new org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData()
      .setErrorCode(Errors.NONE.code)
      .setSubscriptionId(subscriptionId)
      .setPushIntervalMs(pushInterval)
      .setTelemetryMaxBytes(telemetryMaxBytes)
      .setDeltaTemporality(delta)
    responseData.requestedMetrics().addAll(metrics)

    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(
      any[GetTelemetrySubscriptionsRequest](), any[RequestContext]()))
      .thenReturn(new GetTelemetrySubscriptionsResponse(responseData))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      assertEquals(subscriptionId, resp.data().subscriptionId)
      assertEquals(pushInterval, resp.data().pushIntervalMs)
      assertEquals(telemetryMaxBytes, resp.data().telemetryMaxBytes)
      assertEquals(delta, resp.data().deltaTemporality)
      assertEquals(metrics.asScala.toSeq, resp.data().requestedMetrics().asScala.toSeq)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsKRaftProcessThrowsMapsToInvalidRequest(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(
      any[GetTelemetrySubscriptionsRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("fuzz-get-telemetry-process"))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.INVALID_REQUEST.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsKRaftProcessThrowsInvalidRequestException(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val detail = data.consumeString(48)
    val safeDetail = if (detail == null || detail.isEmpty) "fuzz-invalid-detail" else detail.replace('\n', ' ').take(80)
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(
      any[GetTelemetrySubscriptionsRequest](), any[RequestContext]()))
      .thenThrow(new InvalidRequestException(s"GetTelemetrySubscriptionsRequest: $safeDetail"))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(Errors.INVALID_REQUEST.code, resp.data().errorCode)
      assertEquals(Errors.INVALID_REQUEST.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsKRaftManagerReturnsErrorCode(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val error = Errors.values()(data.consumeInt(0, Errors.values().length - 1))
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildRequest(built)

    val responseData = new org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData()
      .setErrorCode(error.code)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(
      any[GetTelemetrySubscriptionsRequest](), any[RequestContext]()))
      .thenReturn(new GetTelemetrySubscriptionsResponse(responseData))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(error.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsKRaftForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val version = ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.latestVersion()
    val throttleMs = data.consumeInt(1, 100)
    val requestData = buildRequestData(data)
    val built = new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(version)
    val request = buildForwardedRequest(built)

    resetKRaftTelemetryHarness()
    stubClientMetricsSuccess()
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(
      any[GetTelemetrySubscriptionsRequest](), any[RequestContext]()))
      .thenReturn(new GetTelemetrySubscriptionsResponse(
        new org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData()))
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleGetTelemetrySubscriptionsRequest(request)
      val resp = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  /**
   * `AbstractRequest` rejects unsupported wire versions before `KafkaApis`
   * runs; match the exception wording used by
   * `AbstractRequest` constructors.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestGetTelemetrySubscriptionsBuilderUnsupportedWireVersion(data: FuzzedDataProvider): Unit = {
    val requestData = buildRequestData(data)
    val invalidVersion = data.consumeShort(1, 5).toShort
    try {
      new GetTelemetrySubscriptionsRequest.Builder(requestData, true).build(invalidVersion)
      throw new AssertionError("expected UnsupportedVersionException")
    } catch {
      case e: UnsupportedVersionException => validateUnsupportedVersionExceptionMessage(e)
    }
  }
}
