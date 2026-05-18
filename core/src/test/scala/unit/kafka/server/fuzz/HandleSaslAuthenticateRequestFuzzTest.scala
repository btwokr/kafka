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
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{SaslAuthenticateRequest, SaslAuthenticateResponse}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests for `KafkaApis.handleSaslAuthenticateRequest`.
 *
 * This handler intentionally ignores the request body and always responds with
 * [[Errors.ILLEGAL_SASL_STATE]] and a fixed error message. On the broker, SASL
 * authentication for the Kafka protocol is driven during connection setup (before
 * requests are dispatched to `KafkaApis`). If a `SaslAuthenticate` request reaches
 * `KafkaApis`, the connection is already past that stage, so the broker treats it
 * as an illegal SASL state (the same pattern as `handleSaslHandshakeRequest`, which
 * also returns `ILLEGAL_SASL_STATE` without inspecting the handshake payload).
 *
 * Fuzz coverage therefore targets wire versions and `authBytes` parsing, plus
 * `sendResponseMaybeThrottle` for throttling and forwarded inner requests.
 */
class HandleSaslAuthenticateRequestFuzzTest extends KafkaApisTest {

  private def resetHarness(): Unit = {
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

  private def stubClientThrottle(throttleTimeMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslAuthenticateIllegalStateRandomVersionAndAuthBytes(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
      ApiKeys.SASL_AUTHENTICATE.latestVersion())
    val authBytesMaxLength = data.consumeInt(0, 256)
    val authBytes = data.consumeBytes(authBytesMaxLength)

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslAuthenticateRequestData().setAuthBytes(authBytes)
    val built = new SaslAuthenticateRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslAuthenticateRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslAuthenticateIllegalStateEmptyAuthBytes(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
      ApiKeys.SASL_AUTHENTICATE.latestVersion())

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslAuthenticateRequestData().setAuthBytes(new Array[Byte](0))
    val built = new SaslAuthenticateRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslAuthenticateRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslAuthenticateVerifiedIllegalStateResponse(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
      ApiKeys.SASL_AUTHENTICATE.latestVersion())
    val authBytesMaxLength = data.consumeInt(0, 64)
    val authBytes = data.consumeBytes(authBytesMaxLength)

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslAuthenticateRequestData().setAuthBytes(authBytes)
    val built = new SaslAuthenticateRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleSaslAuthenticateRequest(request)
      val response = verifyNoThrottling[SaslAuthenticateResponse](request)
      assertEquals(Errors.ILLEGAL_SASL_STATE.code, response.data.errorCode)
      assertEquals(
        "SaslAuthenticate request received after successful authentication",
        response.data.errorMessage)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslAuthenticateThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
      ApiKeys.SASL_AUTHENTICATE.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val authBytesMaxLength = data.consumeInt(0, 128)
    val authBytes = data.consumeBytes(authBytesMaxLength)

    resetHarness()
    stubClientThrottle(throttleTimeMs)

    val requestData = new SaslAuthenticateRequestData().setAuthBytes(authBytes)
    val built = new SaslAuthenticateRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslAuthenticateRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslAuthenticateForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.SASL_AUTHENTICATE.oldestVersion(),
      ApiKeys.SASL_AUTHENTICATE.latestVersion())
    val throttleTimeMs = data.consumeInt(0, 200)
    val authBytesMaxLength = data.consumeInt(0, 128)
    val authBytes = data.consumeBytes(authBytesMaxLength)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)

    val requestData = new SaslAuthenticateRequestData().setAuthBytes(authBytes)
    val built = new SaslAuthenticateRequest.Builder(requestData).build(requestVersion)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslAuthenticateRequest(request)
    finally kafkaApis.close()
  }
}
