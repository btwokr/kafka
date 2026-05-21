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
import kafka.server.{DelegationTokenManager, KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.RenewDelegationTokenRequestData
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{RenewDelegationTokenRequest, RenewDelegationTokenResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.net.InetAddress
import java.util.Optional

/**
 * Jazzer fuzz tests for `KafkaApis.handleRenewTokenRequest` / `handleRenewTokenRequestZk`.
 *
 * Covers `allowTokenRequests` rejection on `PLAINTEXT`, successful and failing
 * `DelegationTokenManager#renewToken` callbacks, ZooKeeper migration with an inactive
 * controller (`NOT_CONTROLLER`), client throttling, and forwarded inner requests.
 * `RenewDelegationTokenResponse` exposes only error code, expiry timestamp, and throttle
 * time; there is no separate error message field.
 */
class HandleRenewTokenRequestFuzzTest extends KafkaApisTest {

  private val zkMigrationBrokerOverrides: Map[String, String] = Map(
    KRaftConfigs.MIGRATION_ENABLED_CONFIG -> "true",
    QuorumConfig.QUORUM_VOTERS_CONFIG -> "3000@localhost:9093",
    KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG -> "CONTROLLER",
    ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG -> MetadataVersion.IBP_3_7_IV1.version()
  )

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

  private def buildRenewRequestWithPlaintextSecurityProtocol(
      built: RenewDelegationTokenRequest
  ): RequestChannel.Request = {
    val requestHeader = new RequestHeader(built.apiKey, built.version, clientId, 0)
    val buffer = built.serializeWithHeader(requestHeader)
    val header = RequestHeader.parse(buffer)
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val context = new RequestContext(
      header,
      "1",
      InetAddress.getLocalHost,
      Optional.empty[java.lang.Integer],
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"),
      listenerName,
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false,
      Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(
      processor = 1,
      context = context,
      startTimeNanos = 0,
      memoryPool = MemoryPool.NONE,
      buffer = buffer,
      metrics = requestChannelMetrics,
      envelope = None)
  }

  private def stubDelegationTokenManagerRenewSuccess(
    delegationTokenManager: DelegationTokenManager,
    renewedExpiryTimestampMs: Long
  ): Unit = {
    when(delegationTokenManager.renewToken(any(), any(), anyLong(), any()))
      .thenAnswer(invocation => {
        val callback = invocation.getArgument(3).asInstanceOf[(Errors, Long) => Unit]
        callback.apply(Errors.NONE, renewedExpiryTimestampMs)
      })
  }

  private def stubDelegationTokenManagerRenewError(
    delegationTokenManager: DelegationTokenManager,
    error: Errors,
    expiryTimestampMs: Long
  ): Unit = {
    when(delegationTokenManager.renewToken(any(), any(), anyLong(), any()))
      .thenAnswer(invocation => {
        val callback = invocation.getArgument(3).asInstanceOf[(Errors, Long) => Unit]
        callback.apply(error, expiryTimestampMs)
      })
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestNotAllowedPlaintext(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRenewRequestWithPlaintextSecurityProtocol(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleRenewTokenRequest(request)
      val response = verifyNoThrottling[RenewDelegationTokenResponse](request)
      assertEquals(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, response.error())
      assertEquals(DelegationTokenManager.ErrorTimestamp, response.data.expiryTimestampMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestZkDelegationTokenManagerSuccess(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)
    val renewedExpiryTimestampMs = data.consumeLong(1L, Long.MaxValue / 4)

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(true)

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubDelegationTokenManagerRenewSuccess(delegationTokenManager, renewedExpiryTimestampMs)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleRenewTokenRequest(request)
      val response = verifyNoThrottling[RenewDelegationTokenResponse](request)
      assertEquals(Errors.NONE, response.error())
      assertEquals(renewedExpiryTimestampMs, response.data.expiryTimestampMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestZkDelegationTokenManagerReturnsError(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)
    val useExpiredError = data.consumeBoolean()
    val managerError =
      if (useExpiredError) Errors.DELEGATION_TOKEN_EXPIRED
      else Errors.DELEGATION_TOKEN_NOT_FOUND
    val errorExpiryTimestampMs =
      if (useExpiredError) DelegationTokenManager.ErrorTimestamp
      else data.consumeLong(0L, Long.MaxValue / 8)

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(true)

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubDelegationTokenManagerRenewError(delegationTokenManager, managerError, errorExpiryTimestampMs)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleRenewTokenRequest(request)
      val response = verifyNoThrottling[RenewDelegationTokenResponse](request)
      assertEquals(managerError, response.error())
      assertEquals(errorExpiryTimestampMs, response.data.expiryTimestampMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestZkMigrationInactiveController(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(false)

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      overrideProperties = zkMigrationBrokerOverrides,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleRenewTokenRequest(request)
      val response = verifyNoThrottling[RenewDelegationTokenResponse](request)
      assertEquals(Errors.NOT_CONTROLLER, response.error())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubClientThrottle(throttleTimeMs)
    when(controller.isActive).thenReturn(true)

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubDelegationTokenManagerRenewSuccess(delegationTokenManager, 9_999_888_777L)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try kafkaApis.handleRenewTokenRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRenewTokenRequestForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.RENEW_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.RENEW_DELEGATION_TOKEN.latestVersion())
    val requestThrottleMs = data.consumeInt(0, 200)
    val hmacLength = data.consumeInt(0, 128)
    val hmacBytes = data.consumeBytes(hmacLength).clone()
    val renewPeriodMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)
    when(controller.isActive).thenReturn(true)

    val requestData = new RenewDelegationTokenRequestData()
      .setHmac(hmacBytes)
      .setRenewPeriodMs(renewPeriodMs)
    val built = new RenewDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildForwardedRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubDelegationTokenManagerRenewSuccess(delegationTokenManager, 42L)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try kafkaApis.handleRenewTokenRequest(request)
    finally kafkaApis.close()
  }
}
