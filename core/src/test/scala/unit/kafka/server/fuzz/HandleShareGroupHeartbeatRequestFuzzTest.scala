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
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{RequestContext, ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.KRaftVersion
import org.apache.kafka.server.config.ShareGroupConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.concurrent.CompletableFuture

/**
 * Jazzer fuzz tests for `KafkaApis.handleShareGroupHeartbeat` (via
 * `KafkaApis.handle`, matching production wiring and async completion).
 *
 * Covers share protocol disabled (`UNSUPPORTED_VERSION`), `READ` on `GROUP`
 * denied (`GROUP_AUTHORIZATION_FAILED`), coordinator success, and coordinator
 * failure (`getErrorResponse` from an `ApiException` or generic
 * `RuntimeException` → `UNKNOWN_SERVER_ERROR`), plus throttling and forwarded
 * requests.
 */
class HandleShareGroupHeartbeatRequestFuzzTest extends KafkaApisTest {

  private val shareEnabledOverrides: Map[String, String] = Map(
    GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "true",
    ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")

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
    val s = data.consumeString(128)
    if (s == null || s.isEmpty) fallback
    else s.replace('\n', ' ').replace('\r', ' ').take(256)
  }

  private def buildRequestData(data: FuzzedDataProvider): ShareGroupHeartbeatRequestData = {
    val d = new ShareGroupHeartbeatRequestData()
      .setGroupId(groupIdSafe(data, "fuzz-share-group"))
      .setMemberId(groupIdSafe(data, "fuzz-share-member"))
      .setMemberEpoch(data.consumeInt(-2, Int.MaxValue))
    if (data.consumeBoolean()) {
      val n = data.consumeInt(0, 4)
      val topics = new java.util.ArrayList[String]()
      var i = 0
      while (i < n) {
        topics.add(groupIdSafe(data, s"topic-$i"))
        i += 1
      }
      d.setSubscribedTopicNames(topics)
    } else {
      d.setSubscribedTopicNames(null)
    }
    if (data.consumeBoolean())
      d.setRackId(groupIdSafe(data, "rack"))
    else
      d.setRackId(null)
    d
  }

  private def buildShareGroupHeartbeatRequest(data: FuzzedDataProvider): ShareGroupHeartbeatRequest = {
    val version = ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion()
    new ShareGroupHeartbeatRequest.Builder(buildRequestData(data), true).build(version)
  }

  private def authorizerDenyAll(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.Collections.singletonList(AuthorizationResult.DENIED))
    authorizer
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatShareDisabledUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(Errors.UNSUPPORTED_VERSION.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatShareDisabledUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatShareDisabledUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildForwardedRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatAuthDenied(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatAuthDeniedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatAuthDeniedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildForwardedRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatCoordinatorSuccess(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    val memberIdOut = groupIdSafe(data, "out-member")
    val memberEpochOut = data.consumeInt(0, Int.MaxValue)
    val hbInterval = data.consumeInt(1, 600_000)
    val responseData = new org.apache.kafka.common.message.ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId(memberIdOut)
      .setMemberEpoch(memberEpochOut)
      .setHeartbeatIntervalMs(hbInterval)

    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[org.apache.kafka.common.message.ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(any[RequestContext], org.mockito.ArgumentMatchers.any(classOf[ShareGroupHeartbeatRequestData]))).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      future.complete(responseData)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      assertEquals(memberIdOut, resp.data().memberId)
      assertEquals(memberEpochOut, resp.data().memberEpoch)
      assertEquals(hbInterval, resp.data().heartbeatIntervalMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatCoordinatorCompletesExceptionallyApiError(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    val apiErrors = Array(
      Errors.FENCED_MEMBER_EPOCH,
      Errors.NOT_COORDINATOR,
      Errors.INVALID_REQUEST,
      Errors.UNKNOWN_MEMBER_ID)
    val err = apiErrors(data.consumeInt(0, apiErrors.length - 1))

    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[org.apache.kafka.common.message.ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(any[RequestContext], org.mockito.ArgumentMatchers.any(classOf[ShareGroupHeartbeatRequestData]))).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      future.completeExceptionally(err.exception())
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(err.code, resp.data().errorCode)
      assertEquals(err.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatCoordinatorCompletesExceptionallyRuntime(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[org.apache.kafka.common.message.ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(any[RequestContext], org.mockito.ArgumentMatchers.any(classOf[ShareGroupHeartbeatRequestData]))).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      future.completeExceptionally(new RuntimeException(s"fuzz-share-hb-${data.consumeInt(0, Int.MaxValue)}"))
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, resp.data().errorCode)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatCoordinatorSuccessThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildRequest(shareReq)

    val responseData = new org.apache.kafka.common.message.ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMemberId("m")
      .setMemberEpoch(1)
      .setHeartbeatIntervalMs(3000)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val future = new CompletableFuture[org.apache.kafka.common.message.ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(any[RequestContext], org.mockito.ArgumentMatchers.any(classOf[ShareGroupHeartbeatRequestData]))).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      future.complete(responseData)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupHeartbeatCoordinatorSuccessForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupHeartbeatRequest(data)
    val request = buildForwardedRequest(shareReq)

    val responseData = new org.apache.kafka.common.message.ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.NONE.code)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val future = new CompletableFuture[org.apache.kafka.common.message.ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(any[RequestContext], org.mockito.ArgumentMatchers.any(classOf[ShareGroupHeartbeatRequestData]))).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      future.complete(responseData)
      val resp = verifyNoThrottling[ShareGroupHeartbeatResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }
}
