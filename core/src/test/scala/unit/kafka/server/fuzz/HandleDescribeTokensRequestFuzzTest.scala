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
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.DelegationTokenManagerConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, never, reset, verify, when}

import java.net.InetAddress
import java.util
import java.util.Optional

/**
 * Jazzer fuzz tests for `KafkaApis.handleDescribeTokensRequest`.
 *
 * Covers `allowTokenRequests` rejection on `PLAINTEXT`, delegation token auth disabled,
 * the short-circuit when the wire request carries an explicit empty owner list (no
 * `getTokens` call), `owners == null` vs non-empty owner filters (both call `getTokens`),
 * client throttling, and forwarded inner requests.
 */
class HandleDescribeTokensRequestFuzzTest extends KafkaApisTest {

  private val tokenAuthEnabledOverrides: Map[String, String] = Map(
    DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG -> "1234567890")

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

  private def buildDescribeRequestWithPlaintextSecurityProtocol(
      built: DescribeDelegationTokenRequest
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

  private def fuzzedDelegationToken(data: FuzzedDataProvider): DelegationToken = {
    val ownerName = data.consumeString(32)
    val owner = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ownerName)
    val renewers = util.Collections.singletonList(owner)
    val tokenId = data.consumeString(24)
    val issueTs = data.consumeLong(1L, 1_000_000_000L)
    val maxTs = data.consumeLong(issueTs + 1, issueTs + 86_400_000_000L)
    val expiryTs = data.consumeLong(issueTs, maxTs)
    val info = new TokenInformation(
      tokenId,
      owner,
      owner,
      renewers,
      issueTs,
      maxTs,
      expiryTs)
    val hmac = data.consumeBytes(data.consumeInt(0, 64)).clone()
    new DelegationToken(info, hmac)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestNotAllowedPlaintext(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val useEmptyOwners = data.consumeBoolean()
    val built =
      if (useEmptyOwners)
        new DescribeDelegationTokenRequest.Builder(util.Collections.emptyList[KafkaPrincipal]())
          .build(requestWireVersion)
      else {
        val owner = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, data.consumeString(16))
        new DescribeDelegationTokenRequest.Builder(util.Collections.singletonList(owner))
          .build(requestWireVersion)
      }

    resetHarness()
    stubNoThrottle()

    val request = buildDescribeRequestWithPlaintextSecurityProtocol(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleDescribeTokensRequest(request)
      val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
      assertEquals(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, response.error())
      assertEquals(0, response.tokens().size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestAuthDisabled(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val owner = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, data.consumeString(24))
    val built = new DescribeDelegationTokenRequest.Builder(util.Collections.singletonList(owner))
      .build(requestWireVersion)

    resetHarness()
    stubNoThrottle()

    val request = buildRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      overrideProperties = Map.empty,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleDescribeTokensRequest(request)
      val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
      assertEquals(Errors.DELEGATION_TOKEN_AUTH_DISABLED, response.error())
      assertEquals(0, response.tokens().size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestOwnersListExplicitlyEmpty(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val built = new DescribeDelegationTokenRequest.Builder(util.Collections.emptyList[KafkaPrincipal]())
      .build(requestWireVersion)

    resetHarness()
    stubNoThrottle()

    val request = buildRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledOverrides,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleDescribeTokensRequest(request)
      val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
      assertEquals(Errors.NONE, response.error())
      assertEquals(0, response.tokens().size)
      verify(delegationTokenManager, never()).getTokens(any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestOwnersNullGetTokens(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val built = new DescribeDelegationTokenRequest.Builder(null.asInstanceOf[util.List[KafkaPrincipal]])
      .build(requestWireVersion)

    resetHarness()
    stubNoThrottle()

    val request = buildRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    when(delegationTokenManager.getTokens(any())).thenReturn(List.empty)
    val kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledOverrides,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleDescribeTokensRequest(request)
      val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
      assertEquals(Errors.NONE, response.error())
      assertEquals(0, response.tokens().size)
      verify(delegationTokenManager).getTokens(any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestWithOwnersReturnsTokens(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(3, ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val filterOwner = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, data.consumeString(20))
    val built = new DescribeDelegationTokenRequest.Builder(util.Collections.singletonList(filterOwner))
      .build(requestWireVersion)

    resetHarness()
    stubNoThrottle()

    val request = buildRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val returned = List(fuzzedDelegationToken(data))
    when(delegationTokenManager.getTokens(any())).thenReturn(returned)
    val kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledOverrides,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleDescribeTokensRequest(request)
      val response = verifyNoThrottling[DescribeDelegationTokenResponse](request)
      assertEquals(Errors.NONE, response.error())
      assertEquals(1, response.tokens().size)
      assertEquals(returned.head.tokenInfo().tokenId(), response.tokens().get(0).tokenInfo().tokenId())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val built = new DescribeDelegationTokenRequest.Builder(util.Collections.emptyList[KafkaPrincipal]())
      .build(requestWireVersion)

    resetHarness()
    stubClientThrottle(throttleTimeMs)

    val request = buildRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledOverrides,
      tokenManager = delegationTokenManager)
    try kafkaApis.handleDescribeTokensRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTokensRequestForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.DESCRIBE_DELEGATION_TOKEN.latestVersion())
    val requestThrottleMs = data.consumeInt(0, 200)
    val built = new DescribeDelegationTokenRequest.Builder(util.Collections.emptyList[KafkaPrincipal]())
      .build(requestWireVersion)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val request = buildForwardedRequest(built)
    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      overrideProperties = tokenAuthEnabledOverrides,
      tokenManager = delegationTokenManager)
    try kafkaApis.handleDescribeTokensRequest(request)
    finally kafkaApis.close()
  }
}
