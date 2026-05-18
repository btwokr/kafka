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
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{SaslHandshakeRequest, SaslHandshakeResponse}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests for `KafkaApis.handleSaslHandshakeRequest`.
 *
 * The handler ignores the client mechanism and always responds with
 * [[Errors.ILLEGAL_SASL_STATE]]. The `SaslHandshake` response schema has no
 * top-level error message field (only `errorCode` and `mechanisms`), so the
 * verified test asserts the error code and an empty mechanisms list after
 * the broker builds the response. Coverage matches `HandleSaslAuthenticateRequestFuzzTest`:
 * random wire input, throttling, and forwarded inner requests.
 */
class HandleSaslHandshakeRequestFuzzTest extends KafkaApisTest {

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
  def fuzzTestSaslHandshakeIllegalStateRandomVersionAndMechanism(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.SASL_HANDSHAKE.oldestVersion(),
      ApiKeys.SASL_HANDSHAKE.latestVersion())
    val mechanismNameMaxLength = data.consumeInt(0, 128)
    val clientMechanismName = data.consumeString(mechanismNameMaxLength)

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslHandshakeRequestData().setMechanism(clientMechanismName)
    val built = new SaslHandshakeRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslHandshakeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslHandshakeIllegalStateEmptyMechanism(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.SASL_HANDSHAKE.oldestVersion(),
      ApiKeys.SASL_HANDSHAKE.latestVersion())

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslHandshakeRequestData().setMechanism("")
    val built = new SaslHandshakeRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslHandshakeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslHandshakeVerifiedIllegalStateResponse(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.SASL_HANDSHAKE.oldestVersion(),
      ApiKeys.SASL_HANDSHAKE.latestVersion())
    val mechanismNameMaxLength = data.consumeInt(0, 64)
    val clientMechanismName = data.consumeString(mechanismNameMaxLength)

    resetHarness()
    stubNoThrottle()

    val requestData = new SaslHandshakeRequestData().setMechanism(clientMechanismName)
    val built = new SaslHandshakeRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleSaslHandshakeRequest(request)
      val response = verifyNoThrottling[SaslHandshakeResponse](request)
      assertEquals(Errors.ILLEGAL_SASL_STATE.code, response.data.errorCode)
      val enabledMechanisms = response.data.mechanisms
      assertTrue(enabledMechanisms == null || enabledMechanisms.isEmpty)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslHandshakeThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.SASL_HANDSHAKE.oldestVersion(),
      ApiKeys.SASL_HANDSHAKE.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val mechanismNameMaxLength = data.consumeInt(0, 128)
    val clientMechanismName = data.consumeString(mechanismNameMaxLength)

    resetHarness()
    stubClientThrottle(throttleTimeMs)

    val requestData = new SaslHandshakeRequestData().setMechanism(clientMechanismName)
    val built = new SaslHandshakeRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslHandshakeRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSaslHandshakeForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.SASL_HANDSHAKE.oldestVersion(),
      ApiKeys.SASL_HANDSHAKE.latestVersion())
    val requestThrottleMs = data.consumeInt(0, 200)
    val mechanismNameMaxLength = data.consumeInt(0, 128)
    val clientMechanismName = data.consumeString(mechanismNameMaxLength)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val requestData = new SaslHandshakeRequestData().setMechanism(clientMechanismName)
    val built = new SaslHandshakeRequest.Builder(requestData).build(requestWireVersion)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSaslHandshakeRequest(request)
    finally kafkaApis.close()
  }
}
