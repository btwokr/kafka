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
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.ListClientMetricsResourcesRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ListClientMetricsResourcesRequest, ListClientMetricsResourcesResponse}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleListClientMetricsResources`.
 *
 * Covers `DESCRIBE_CONFIGS` on `CLUSTER` denial (`CLUSTER_AUTHORIZATION_FAILED`),
 * the ZooKeeper path (`clientMetricsManager == None`, `UNSUPPORTED_VERSION`),
 * and the KRaft path listing resources (including empty), `listClientMetricsResources`
 * throwing (surfaced as `UNKNOWN_SERVER_ERROR`), plus throttling and forwarded
 * requests.
 */
class HandleListClientMetricsResourcesRequestFuzzTest extends KafkaApisTest {

  private def resetZkHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def resetKRaftHarness(): Unit = {
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

  /** Denies `DESCRIBE_CONFIGS` on the `CLUSTER` resource only. */
  private def authorizerDenyClusterDescribeConfigs(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        val denied = action.operation == AclOperation.DESCRIBE_CONFIGS &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER &&
          CLUSTER_NAME == action.resourcePattern.name
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    auth
  }

  private def buildListRequest(): ListClientMetricsResourcesRequest = {
    val version = ApiKeys.LIST_CLIENT_METRICS_RESOURCES.latestVersion()
    new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build(version)
  }

  private def resourceName(data: FuzzedDataProvider, fallback: String): String = {
    val s = data.consumeString(40)
    if (s == null || s.isEmpty) fallback
    else s.replace('\n', ' ').replace('\r', ' ').take(128)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesClusterDescribeConfigsDenied(data: FuzzedDataProvider): Unit = {
    val built = buildListRequest()
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribeConfigs()))
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesClusterDescribeConfigsDeniedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val built = buildListRequest()
    val request = buildRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribeConfigs()))
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesClusterDescribeConfigsDeniedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val built = buildListRequest()
    val request = buildForwardedRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribeConfigs()))
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val built = buildListRequest()
    val request = buildRequest(built)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(0, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesZkUnsupportedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val built = buildListRequest()
    val request = buildRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesZkUnsupportedForwarded(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 100)
    val built = buildListRequest()
    val request = buildForwardedRequest(built)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, resp.data().errorCode)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesKRaftReturnsResources(data: FuzzedDataProvider): Unit = {
    val built = buildListRequest()
    val request = buildRequest(built)

    val numNames = data.consumeInt(0, 6)
    val names = (0 until numNames).map(i => resourceName(data, s"res-$i"))
    val resources = new java.util.LinkedHashSet[String]()
    names.foreach(n => resources.add(n))

    resetKRaftHarness()
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      val got = resp.data().clientMetricsResources().asScala.map(_.name).toSeq
      assertEquals(names, got)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesKRaftEmptyResources(data: FuzzedDataProvider): Unit = {
    val built = buildListRequest()
    val request = buildRequest(built)

    val resources = new java.util.LinkedHashSet[String]()

    resetKRaftHarness()
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
      assertEquals(0, resp.data().clientMetricsResources().size())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesKRaftListThrowsUnknownServerError(data: FuzzedDataProvider): Unit = {
    val built = buildListRequest()
    val request = buildRequest(built)

    resetKRaftHarness()
    when(clientMetricsManager.listClientMetricsResources).thenThrow(
      new RuntimeException(s"fuzz-list-client-metrics-${data.consumeInt(0, Int.MaxValue)}"))
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.NoCaching)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, resp.data().errorCode)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.message, Errors.forCode(resp.data().errorCode).message)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListClientMetricsResourcesKRaftForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(1, 100)
    val built = buildListRequest()
    val request = buildForwardedRequest(built)

    val resources = new java.util.LinkedHashSet[String]()
    resources.add(resourceName(data, "fwd-res"))

    resetKRaftHarness()
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources)
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleListClientMetricsResources(request)
      val resp = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      assertEquals(Errors.NONE.code, resp.data().errorCode)
    } finally kafkaApis.close()
  }
}
