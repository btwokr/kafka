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
import kafka.server.KafkaApisTest
import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleApiVersionsRequest`.
 *
 * The handler chooses between `UNSUPPORTED_VERSION`, `INVALID_REQUEST` (v3+
 * client software fields failing validation), or a normal
 * `apiVersionManager.apiVersionResponse` (with `request.header.apiVersion() < 4`
 * controlling `alterFeatureLevel0`), then sends via `sendResponseMaybeThrottle`.
 */
class HandleApiVersionsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  /**
   * Supported wire version; default builder data satisfies `isValid` for v3+.
   * Covers `alterFeatureLevel0` true vs false via header/body version &lt; 4 vs &ge; 4.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestApiVersionsSuccessPath(data: FuzzedDataProvider): Unit = {
    val bodyVersion = data.consumeInt(
      ApiKeys.API_VERSIONS.oldestVersion().toInt,
      ApiKeys.API_VERSIONS.latestVersion().toInt).toShort

    val built = new ApiVersionsRequest.Builder().build(bodyVersion)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleApiVersionsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * `ApiVersionsRequest.isValid` is false for v3+ when client software name/version
   * do not match the expected pattern (e.g. empty strings).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestApiVersionsInvalidClientSoftware(data: FuzzedDataProvider): Unit = {
    val bodyVersion = data.consumeInt(3, ApiKeys.API_VERSIONS.latestVersion().toInt).toShort
    val useEmptyName = data.consumeBoolean()
    val useEmptyVersion = data.consumeBoolean()
    val fuzzName = safeString(data, "n")
    val fuzzVersion = safeString(data, "v")

    val reqData = new ApiVersionsRequestData()
    if (useEmptyName)
      reqData.setClientSoftwareName("")
    else
      reqData.setClientSoftwareName(fuzzName)
    if (useEmptyVersion)
      reqData.setClientSoftwareVersion("")
    else
      reqData.setClientSoftwareVersion(fuzzVersion)

    val built = new ApiVersionsRequest.Builder(
      reqData,
      ApiKeys.API_VERSIONS.oldestVersion(),
      ApiKeys.API_VERSIONS.latestVersion()
    ).build(bodyVersion)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleApiVersionsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Wire header declares an unsupported `ApiVersions` protocol version so
   * `hasUnsupportedRequestVersion` is true (`UNSUPPORTED_VERSION` response).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestApiVersionsUnsupportedWireHeader(data: FuzzedDataProvider): Unit = {
    val bodyWireVersion = data.consumeInt(
      ApiKeys.API_VERSIONS.oldestVersion().toInt,
      ApiKeys.API_VERSIONS.latestVersion().toInt).toShort
    val headerDelta = data.consumeInt(1, 1000)
    val headerApiVersion = (ApiKeys.API_VERSIONS.latestVersion().toLong + headerDelta).toShort

    val request = buildApiVersionsRequestWithMismatchedWireHeader(bodyWireVersion, headerApiVersion)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleApiVersionsRequest(request)
    finally kafkaApis.close()
  }

  /** Non-zero request-quota throttle time is threaded into the response callback. */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestApiVersionsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val bodyVersion = data.consumeInt(
      ApiKeys.API_VERSIONS.oldestVersion().toInt,
      ApiKeys.API_VERSIONS.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)

    val built = new ApiVersionsRequest.Builder().build(bodyVersion)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleApiVersionsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Forwarded inner request: `sendResponseMaybeThrottle` records throttle time but
   * skips channel throttling when `request.isForwarded` is true.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestApiVersionsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val bodyVersion = data.consumeInt(
      ApiKeys.API_VERSIONS.oldestVersion().toInt,
      ApiKeys.API_VERSIONS.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(0, 200)

    val built = new ApiVersionsRequest.Builder().build(bodyVersion)
    val request = buildForwardedRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleApiVersionsRequest(request)
    finally kafkaApis.close()
  }
}
