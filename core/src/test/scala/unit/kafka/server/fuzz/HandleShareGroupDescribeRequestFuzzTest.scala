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
import org.apache.kafka.common.message.ShareGroupDescribeRequestData
import org.apache.kafka.common.message.ShareGroupDescribeResponseData.DescribedGroup
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{RequestContext, ShareGroupDescribeRequest, ShareGroupDescribeResponse}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.KRaftVersion
import org.apache.kafka.server.config.ShareGroupConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleShareGroupDescribe`.
 *
 * Covers share protocol disabled (`UNSUPPORTED_VERSION` per requested group),
 * `DESCRIBE` on `GROUP` denied for all or a subset of ids, successful describe
 * with coordinator results (including an empty group-id list), coordinator
 * failure (`getErrorResponse` from `ApiException` or `RuntimeException`), plus
 * throttling and forwarded requests.
 */
class HandleShareGroupDescribeRequestFuzzTest extends KafkaApisTest {

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
    val s = data.consumeString(96)
    if (s == null || s.isEmpty) fallback
    else s.replace('\n', ' ').replace('\r', ' ').take(256)
  }

  private def buildRequestData(data: FuzzedDataProvider): ShareGroupDescribeRequestData = {
    val d = new ShareGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(data.consumeBoolean())
    val n = data.consumeInt(0, 5)
    var i = 0
    while (i < n) {
      d.groupIds().add(groupIdSafe(data, s"fuzz-share-desc-$i"))
      i += 1
    }
    d
  }

  private def buildShareGroupDescribeRequest(data: FuzzedDataProvider): ShareGroupDescribeRequest = {
    val version = ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion()
    new ShareGroupDescribeRequest.Builder(buildRequestData(data), true).build(version)
  }

  private def buildShareGroupDescribeRequestNonEmpty(data: FuzzedDataProvider): ShareGroupDescribeRequest = {
    val d = new ShareGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(data.consumeBoolean())
    val n = data.consumeInt(1, 5)
    var i = 0
    while (i < n) {
      d.groupIds().add(groupIdSafe(data, s"fuzz-share-desc-ne-$i"))
      i += 1
    }
    new ShareGroupDescribeRequest.Builder(d, true).build(ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion())
  }


  private def authorizerDenyAll(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.DENIED))
    authorizer
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeShareDisabledUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupDescribeRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareGroupDescribe(request).join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      val groups = resp.data().groups()
      val ids = shareReq.data().groupIds().asScala.toSeq
      assertEquals(ids.size, groups.size)
      var j = 0
      while (j < groups.size) {
        assertEquals(Errors.UNSUPPORTED_VERSION.code, groups.get(j).errorCode)
        assertEquals(Errors.UNSUPPORTED_VERSION.message, Errors.forCode(groups.get(j).errorCode).message)
        j += 1
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeShareDisabledUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupDescribeRequest(data)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareGroupDescribe(request).join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      val groups = resp.data().groups()
      groups.forEach(g => assertEquals(Errors.UNSUPPORTED_VERSION.code, g.errorCode))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeShareDisabledUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val shareReq = buildShareGroupDescribeRequest(data)
    val request = buildForwardedRequest(shareReq)

    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleShareGroupDescribe(request).join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      resp.data().groups().forEach(g => assertEquals(Errors.UNSUPPORTED_VERSION.code, g.errorCode))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeAuthDeniedAllGroups(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(Collections.emptyList())
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      resp.data().groups().forEach { g =>
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, g.errorCode)
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.message, Errors.forCode(g.errorCode).message)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeAuthDeniedAllThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(Collections.emptyList())
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      resp.data().groups().forEach(g => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, g.errorCode))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeAuthDeniedAllForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildForwardedRequest(shareReq)
    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizerDenyAll()),
      raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(Collections.emptyList())
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeMixedAuthOneDeniedOneAllowed(data: FuzzedDataProvider): Unit = {
    val deniedId = groupIdSafe(data, "denied-share-g")
    val allowedId = groupIdSafe(data, "allowed-share-g")
    val d = new ShareGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(false)
    d.groupIds().add(deniedId)
    d.groupIds().add(allowedId)
    val version = ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion()
    val shareReq = new ShareGroupDescribeRequest.Builder(d, true).build(version)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(
        Collections.singletonList(AuthorizationResult.DENIED),
        Collections.singletonList(AuthorizationResult.ALLOWED))

    val coordResult = Collections.singletonList(
      new DescribedGroup().setGroupId(allowedId).setErrorCode(Errors.NONE.code))

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(
      overrideProperties = shareEnabledOverrides,
      authorizer = Some(authorizer),
      raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(coordResult)
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      val groups = resp.data().groups()
      assertEquals(2, groups.size)
      assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, groups.get(0).errorCode)
      assertEquals(Errors.NONE.code, groups.get(1).errorCode)
      assertEquals(allowedId, groups.get(1).groupId)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeCoordinatorSuccess(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    val ids = shareReq.data().groupIds().asScala.toSeq
    resetKRaftHarness()
    stubNoThrottle()

    val coordList = new util.ArrayList[DescribedGroup]()
    ids.foreach { id =>
      coordList.add(new DescribedGroup()
        .setGroupId(id)
        .setErrorCode(Errors.NONE.code)
        .setGroupState(groupIdSafe(data, "Stable")))
    }

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(coordList)
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(ids.size, resp.data().groups().size)
      resp.data().groups().asScala.foreach(g =>
        assertEquals(Errors.NONE.code, g.errorCode))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeEmptyGroupList(data: FuzzedDataProvider): Unit = {
    val d = new ShareGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(data.consumeBoolean())
    val version = ApiKeys.SHARE_GROUP_DESCRIBE.latestVersion()
    val shareReq = new ShareGroupDescribeRequest.Builder(d, true).build(version)
    val request = buildRequest(shareReq)

    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(Collections.emptyList())
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(0, resp.data().groups().size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeCoordinatorCompletesExceptionallyApiError(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    val err = Errors.NOT_COORDINATOR

    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.completeExceptionally(err.exception())
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      val groups = resp.data().groups()
      groups.forEach { g =>
        assertEquals(err.code, g.errorCode)
        assertEquals(err.message, Errors.forCode(g.errorCode).message)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeCoordinatorCompletesExceptionallyRuntime(data: FuzzedDataProvider): Unit = {
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    resetKRaftHarness()
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.completeExceptionally(new RuntimeException(s"fuzz-share-desc-${data.consumeInt(0, Int.MaxValue)}"))
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      resp.data().groups().asScala.foreach { g =>
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, g.errorCode)
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.message, Errors.forCode(g.errorCode).message)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeCoordinatorSuccessThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildRequest(shareReq)
    val ids = shareReq.data().groupIds().asScala.toSeq
    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val coordList = new util.ArrayList[DescribedGroup]()
    ids.foreach(id => coordList.add(new DescribedGroup().setGroupId(id).setErrorCode(Errors.NONE.code)))

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(coordList)
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestShareGroupDescribeCoordinatorSuccessForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val shareReq = buildShareGroupDescribeRequestNonEmpty(data)
    val request = buildForwardedRequest(shareReq)
    val ids = shareReq.data().groupIds().asScala.toSeq
    resetKRaftHarness()
    stubClientThrottle(throttleMs)

    val coordList = new util.ArrayList[DescribedGroup]()
    ids.foreach(id => coordList.add(new DescribedGroup().setGroupId(id).setErrorCode(Errors.NONE.code)))

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(any[RequestContext], any[util.List[String]])).thenReturn(future)

    val kafkaApis = createKafkaApis(overrideProperties = shareEnabledOverrides, raftSupport = true)
    try {
      val hb = kafkaApis.handleShareGroupDescribe(request)
      future.complete(coordList)
      hb.join()
      val resp = verifyNoThrottling[ShareGroupDescribeResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }
}
