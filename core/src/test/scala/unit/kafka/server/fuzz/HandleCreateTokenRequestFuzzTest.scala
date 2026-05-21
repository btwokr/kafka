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
import kafka.server.{CreateTokenResult, DelegationTokenManager, KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.CreateDelegationTokenRequestData
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{CreateDelegationTokenRequest, CreateDelegationTokenResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.net.InetAddress
import java.util
import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleCreateTokenRequest` / `handleCreateTokenRequestZk`.
 *
 * Covers `allowTokenRequests` rejection on `PLAINTEXT`, `CREATE_TOKENS` authorization when the
 * declared owner differs from the requester, invalid renewer principal types, successful and
 * failing `DelegationTokenManager#createToken` callbacks, ZooKeeper migration mode with an
 * inactive controller (`NOT_CONTROLLER`), client throttling, and forwarded inner requests.
 * `CreateDelegationTokenResponse` carries no separate error message field; assertions use
 * [[Errors]] codes (and principal fields where the response always includes them).
 */
class HandleCreateTokenRequestFuzzTest extends KafkaApisTest {

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

  /**
   * Builds a request whose [[RequestContext.securityProtocol]] is `PLAINTEXT`, so
   * `KafkaApis.allowTokenRequests` returns false (delegation token APIs are disallowed on
   * plaintext channels).
   */
  private def buildCreateTokenRequestWithPlaintextSecurityProtocol(
      built: CreateDelegationTokenRequest
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

  private def authorizerDenyCreateTokensOnUserResource(): Authorizer = {
    val denyCreateTokensOnUser = mock(classOf[Authorizer])
    when(denyCreateTokensOnUser.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val results = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { action =>
        val denied = action.operation == AclOperation.CREATE_TOKENS &&
          action.resourcePattern.resourceType == ResourceType.USER
        results.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      results
    })
    denyCreateTokensOnUser
  }

  private def safePrincipalNameSegment(data: FuzzedDataProvider, fallback: String): String = {
    val raw = data.consumeString(24).replaceAll("[^a-zA-Z0-9._-]", "_").take(64)
    val base = if (raw.isEmpty) fallback else raw
    if (base == "Alice") fallback + "-not-alice" else base
  }

  private def stubTokenManagerSuccess(
    tokenManager: DelegationTokenManager,
    issueTimestampMs: Long,
    expiryTimestampMs: Long,
    maxTimestampMs: Long,
    tokenId: String,
    hmacBytes: Array[Byte]
  ): Unit = {
    when(tokenManager.createToken(any(), any(), any(), anyLong(), any()))
      .thenAnswer(invocation => {
        val ownerPrincipal = invocation.getArgument(0, classOf[KafkaPrincipal])
        val requesterPrincipal = invocation.getArgument(1, classOf[KafkaPrincipal])
        val callback = invocation.getArgument(4, classOf[CreateTokenResult => Unit])
        callback.apply(CreateTokenResult(
          ownerPrincipal,
          requesterPrincipal,
          issueTimestampMs,
          expiryTimestampMs,
          maxTimestampMs,
          tokenId,
          hmacBytes,
          Errors.NONE))
      })
  }

  private def stubTokenManagerWithError(tokenManager: DelegationTokenManager, error: Errors): Unit = {
    when(tokenManager.createToken(any(), any(), any(), anyLong(), any()))
      .thenAnswer(invocation => {
        val ownerPrincipal = invocation.getArgument(0, classOf[KafkaPrincipal])
        val requesterPrincipal = invocation.getArgument(1, classOf[KafkaPrincipal])
        val callback = invocation.getArgument(4, classOf[CreateTokenResult => Unit])
        callback.apply(CreateTokenResult(
          ownerPrincipal,
          requesterPrincipal,
          DelegationTokenManager.ErrorTimestamp,
          DelegationTokenManager.ErrorTimestamp,
          DelegationTokenManager.ErrorTimestamp,
          "",
          Array.emptyByteArray,
          error))
      })
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestNotAllowedPlaintext(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildCreateTokenRequestWithPlaintextSecurityProtocol(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, response.error())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestAuthorizationFailedDifferentOwner(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(3, ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val differentOwnerName = safePrincipalNameSegment(data, "fuzz-token-owner-bob")
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      .setOwnerPrincipalName(differentOwnerName)
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(
      authorizer = Some(authorizerDenyCreateTokensOnUserResource()),
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, response.error())
      val expectedOwner = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, differentOwnerName)
      assertEquals(expectedOwner.getPrincipalType, response.data.principalType)
      assertEquals(expectedOwner.getName, response.data.principalName)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestInvalidRenewerPrincipalType(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val nonUserRenewerType = data.consumeString(12)
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(if (nonUserRenewerType == KafkaPrincipal.USER_TYPE) "Group" else nonUserRenewerType)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(Errors.INVALID_PRINCIPAL_TYPE, response.error())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestZkDelegationTokenManagerSuccess(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)
    val issueTimestampMs = data.consumeLong(1L, Long.MaxValue / 4)
    val expiryTimestampMs = data.consumeLong(issueTimestampMs, Long.MaxValue / 2)
    val maxTimestampMs = data.consumeLong(expiryTimestampMs, Long.MaxValue / 2)
    val tokenId = data.consumeString(22)
    val hmacLength = data.consumeInt(0, 64)
    val hmacBytes = data.consumeBytes(hmacLength)

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(true)

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubTokenManagerSuccess(delegationTokenManager, issueTimestampMs, expiryTimestampMs, maxTimestampMs, tokenId, hmacBytes)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(Errors.NONE, response.error())
      assertEquals(issueTimestampMs, response.data.issueTimestampMs)
      assertEquals(expiryTimestampMs, response.data.expiryTimestampMs)
      assertEquals(maxTimestampMs, response.data.maxTimestampMs)
      assertEquals(tokenId, response.data.tokenId)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestZkDelegationTokenManagerReturnsError(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)
    val useDelegationTokenDisabled = data.consumeBoolean()
    val managerError =
      if (useDelegationTokenDisabled) Errors.DELEGATION_TOKEN_AUTH_DISABLED
      else Errors.CLUSTER_AUTHORIZATION_FAILED

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(true)

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubTokenManagerWithError(delegationTokenManager, managerError)
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(managerError, response.error())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestZkMigrationInactiveController(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubNoThrottle()
    when(controller.isActive).thenReturn(false)

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    val migrationOverrides = Map(
      KRaftConfigs.MIGRATION_ENABLED_CONFIG -> "true",
      QuorumConfig.QUORUM_VOTERS_CONFIG -> "3000@localhost:9093",
      KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG -> "CONTROLLER",
      ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG -> MetadataVersion.IBP_3_7_IV1.version()
    )
    val kafkaApis = createKafkaApis(
      overrideProperties = migrationOverrides,
      tokenManager = delegationTokenManager)
    try {
      kafkaApis.handleCreateTokenRequest(request)
      val response = verifyNoThrottling[CreateDelegationTokenResponse](request)
      assertEquals(Errors.NOT_CONTROLLER, response.error())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    stubClientThrottle(throttleTimeMs)
    when(controller.isActive).thenReturn(true)

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubTokenManagerSuccess(delegationTokenManager, 1L, 2L, 3L, "fuzz-token-id", Array[Byte](1, 2, 3))
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try kafkaApis.handleCreateTokenRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateTokenRequestForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestWireVersion = data.consumeShort(
      ApiKeys.CREATE_DELEGATION_TOKEN.oldestVersion(),
      ApiKeys.CREATE_DELEGATION_TOKEN.latestVersion())
    val requestThrottleMs = data.consumeInt(0, 200)
    val renewerPrincipalName = data.consumeString(32)
    val maxLifetimeMs = data.consumeLong(0L, 86_400_000L)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)
    when(controller.isActive).thenReturn(true)

    val renewerEntry = new CreatableRenewers()
      .setPrincipalType(KafkaPrincipal.USER_TYPE)
      .setPrincipalName(renewerPrincipalName)
    val requestData = new CreateDelegationTokenRequestData()
      .setRenewers(List(renewerEntry).asJava)
      .setMaxLifetimeMs(maxLifetimeMs)
    if (requestWireVersion >= 3) {
      requestData.setOwnerPrincipalType(KafkaPrincipal.USER_TYPE)
      requestData.setOwnerPrincipalName("Alice")
    }
    val built = new CreateDelegationTokenRequest.Builder(requestData).build(requestWireVersion)
    val request = buildForwardedRequest(built)

    val delegationTokenManager = mock(classOf[DelegationTokenManager])
    stubTokenManagerSuccess(delegationTokenManager, 10L, 20L, 30L, "fuzz-forward-token", Array[Byte](9))
    val kafkaApis = createKafkaApis(tokenManager = delegationTokenManager)
    try kafkaApis.handleCreateTokenRequest(request)
    finally kafkaApis.close()
  }
}
