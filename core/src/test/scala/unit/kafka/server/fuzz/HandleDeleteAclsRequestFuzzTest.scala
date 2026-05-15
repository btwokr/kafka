/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the License); you may not use this file except in compliance with
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
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.message.DeleteAclsRequestData
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.DeleteAclsRequest
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.server.authorizer.{AclDeleteResult, Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{doAnswer, mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDeleteAcls` (ZK path with
 * `requireZkOrThrow`), delegating to `AclApis.handleDeleteAcls`.
 *
 * Covers Raft `shouldAlwaysForward` guard, cluster ALTER denial, the
 * no-authorizer branch (`SecurityDisabledException`), successful and failing
 * `Authorizer#deleteAcls` completion stages (including per-binding delete
 * results), multiple filters, throttling, forwarded inner requests,
 * `DeleteAclsRequest` wire validation failures, and `DeleteAclsResponse`
 * validation when wire version 0 receives non-literal pattern metadata or
 * UNKNOWN elements in matching ACLs from the authorizer path.
 */
class HandleDeleteAclsRequestFuzzTest extends KafkaApisTest {

  private def validateClusterAuthorizationExceptionMessage(e: ClusterAuthorizationException): Unit = {
    val message = e.getMessage
    if (message == null || !message.startsWith("Request ") || !message.endsWith(" is not authorized."))
      throw e
  }

  private def validateDeleteAclsRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val m = e.getMessage
    if (m == null || !m.startsWith(prefix)) throw e
  }

  private def validateDeleteAclsFiltersUnknownElementsMessage(e: IllegalArgumentException): Unit = {
    val m = e.getMessage
    if (m == null || !m.startsWith("Filters contain UNKNOWN elements, filters: ")) throw e
  }

  private def validateDeleteAclsV0UnsupportedPatternMessage(e: UnsupportedVersionException, patternType: PatternType): Unit = {
    val expected =
      s"Version 0 does not support pattern type $patternType (only LITERAL and ANY are supported)"
    if (e.getMessage != expected) throw e
  }

  private def validateDeleteAclsResponseV0NonLiteralMessage(e: UnsupportedVersionException): Unit = {
    val expected = "Version 0 only supports literal resource pattern types"
    if (e.getMessage != expected) throw e
  }

  private def validateDeleteAclsResponseUnknownMatchingElementsMessage(e: IllegalArgumentException): Unit = {
    val expected = "DeleteAclsMatchingAcls contain UNKNOWN elements"
    if (e.getMessage != expected) throw e
  }

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(64)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetZkDeleteAclsHarness(): Unit = {
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

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
  }

  private def authorizerDenyClusterAlter(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val a = actions.get(0)
      val denied = a.operation == AclOperation.ALTER &&
        a.resourcePattern.resourceType == ResourceType.CLUSTER
      Collections.singletonList(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
    })
    mockAuthorizer
  }

  private def authorizerAllowClusterStubDeleteAcls(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    doAnswer(invocation => {
      val filters = invocation.getArgument(1, classOf[util.List[org.apache.kafka.common.acl.AclBindingFilter]])
      val out = new util.ArrayList[java.util.concurrent.CompletionStage[AclDeleteResult]]()
      for (_ <- 0 until filters.size) {
        out.add(CompletableFuture.completedFuture(
          new AclDeleteResult(new util.ArrayList[AclDeleteResult.AclBindingDeleteResult]())))
      }
      out
    }).when(auth).deleteAcls(any(), any())
    auth
  }

  private def authorizerAllowClusterStubDeleteAclsWith(
    results: Seq[CompletableFuture[AclDeleteResult]]
  ): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    val out = new util.ArrayList[java.util.concurrent.CompletionStage[AclDeleteResult]]()
    results.foreach(f => out.add(f))
    doAnswer(_ => out).when(auth).deleteAcls(any(), any())
    auth
  }

  private def deleteFilter(
    resourceType: ResourceType,
    resourceName: String,
    patternType: PatternType,
    principal: String,
    host: String,
    operation: AclOperation,
    permission: AclPermissionType
  ): DeleteAclsFilter = {
    val f = new DeleteAclsFilter()
      .setResourceTypeFilter(resourceType.code())
      .setResourceNameFilter(resourceName)
      .setPrincipalFilter(principal)
      .setHostFilter(host)
      .setOperation(operation.code())
      .setPermissionType(permission.code())
      .setPatternTypeFilter(patternType.code())
    f
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsRaftAlwaysForwardUnsupported(data: FuzzedDataProvider): Unit = {
    val v0 = data.consumeShort(ApiKeys.DELETE_ACLS.oldestVersion(), ApiKeys.DELETE_ACLS.latestVersion())
    val v1 = data.consumeShort(ApiKeys.DELETE_ACLS.oldestVersion(), ApiKeys.DELETE_ACLS.latestVersion())
    val useFirst = data.consumeBoolean()
    val version = if (useFirst) v0 else v1
    val topicName = topicSafe(data, "fuzz-dacl-raft-topic")
    val principal = safeString(data, "User:Alice")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    stubNoThrottle()

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleDeleteAcls(request)
      catch {
        case e: UnsupportedVersionException => validateDeleteAclsRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsSecurityDisabledNoAuthorizer(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_ACLS.oldestVersion(), ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-sec-topic")
    val principal = safeString(data, "User:Bob")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsClusterAlterDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DELETE_ACLS.oldestVersion(), ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-deny-topic")
    val principal = safeString(data, "User:Carol")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterAlter()))
    try {
      try kafkaApis.handleDeleteAcls(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthorizationExceptionMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsAuthorizerDeleteEmptyResults(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-empty-topic")
    val principal = safeString(data, "User:Dave")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAcls()))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsAuthorizerDeleteOneBindingSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-one-topic")
    val principal = safeString(data, "User:Eve")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val binding = new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW))
    val deleteResult = new AclDeleteResult(
      Collections.singletonList(new AclDeleteResult.AclBindingDeleteResult(binding)))
    val fut = CompletableFuture.completedFuture(deleteResult)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(fut))))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsAuthorizerBindingDeleteError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-bind-err")
    val principal = safeString(data, "User:Owen")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val binding = new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW))
    val deleteResult = new AclDeleteResult(
      Collections.singletonList(
        new AclDeleteResult.AclBindingDeleteResult(binding, Errors.NOT_LEADER_OR_FOLLOWER.exception())))
    val fut = CompletableFuture.completedFuture(deleteResult)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(fut))))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsAuthorizerDeleteFilterError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-ferr-topic")
    val principal = safeString(data, "User:Frank")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val errFut = CompletableFuture.completedFuture(new AclDeleteResult(Errors.INVALID_REQUEST.exception()))

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(errFut))))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsTwoFiltersBothSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicA = topicSafe(data, "fuzz-dacl-fa")
    val topicB = topicSafe(data, "fuzz-dacl-fb")
    val principal = safeString(data, "User:Grace")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val f1 = deleteFilter(ResourceType.TOPIC, topicA, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val f2 = deleteFilter(ResourceType.TOPIC, topicB, PatternType.LITERAL,
      principal, host, AclOperation.WRITE, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(List(f1, f2).asJava)
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val empty = CompletableFuture.completedFuture(
      new AclDeleteResult(new util.ArrayList[AclDeleteResult.AclBindingDeleteResult]()))
    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(empty, empty))))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val topicName = topicSafe(data, "fuzz-dacl-thr-topic")
    val principal = safeString(data, "User:Heidi")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubClientThrottle(throttleMs)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAcls()))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(0, 90)
    val topicName = topicSafe(data, "fuzz-dacl-fwd-topic")
    val principal = safeString(data, "User:Ivan")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubClientThrottle(throttleMs)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAcls()))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsWireVersion0PatternAnyNormalized(data: FuzzedDataProvider): Unit = {
    val topicName = topicSafe(data, "fuzz-dacl-v0any-topic")
    val principal = safeString(data, "User:Judy")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.ANY,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(0.toShort)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAcls()))
    try kafkaApis.handleDeleteAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsRequestValidateUnknownElementsThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val name = topicSafe(data, "fuzz-dacl-unkn")
    val principal = safeString(data, "User:Ken")
    val host = safeString(data, "*")

    val flt = deleteFilter(ResourceType.UNKNOWN, name, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))

    try new DeleteAclsRequest.Builder(reqData).build(version)
    catch {
      case e: IllegalArgumentException => validateDeleteAclsFiltersUnknownElementsMessage(e)
    }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsRequestValidateV0UnsupportedPatternThrows(data: FuzzedDataProvider): Unit = {
    val useMatch = data.consumeBoolean()
    val patternType = if (useMatch) PatternType.MATCH else PatternType.PREFIXED
    val topicName = topicSafe(data, "fuzz-dacl-v0bad")
    val principal = safeString(data, "User:Leo")
    val host = safeString(data, "*")

    val flt = deleteFilter(ResourceType.TOPIC, topicName, patternType,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))

    try new DeleteAclsRequest.Builder(reqData).build(0.toShort)
    catch {
      case e: UnsupportedVersionException => validateDeleteAclsV0UnsupportedPatternMessage(e, patternType)
    }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsResponseV0NonLiteralMatchingAclThrows(data: FuzzedDataProvider): Unit = {
    val useMatch = data.consumeBoolean()
    val patternType = if (useMatch) PatternType.MATCH else PatternType.PREFIXED
    val topicName = topicSafe(data, "fuzz-dacl-resv0")
    val principal = safeString(data, "User:Mia")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val binding = new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, topicName, patternType),
      new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW))
    val deleteResult = new AclDeleteResult(
      Collections.singletonList(new AclDeleteResult.AclBindingDeleteResult(binding)))
    val fut = CompletableFuture.completedFuture(deleteResult)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(0.toShort)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(fut))))
    try {
      try kafkaApis.handleDeleteAcls(request)
      catch {
        case e: UnsupportedVersionException => validateDeleteAclsResponseV0NonLiteralMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteAclsResponseUnknownMatchingAclThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DELETE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-resunk")
    val principal = safeString(data, "User:Nina")
    val host = safeString(data, "*")

    resetZkDeleteAclsHarness()
    stubNoThrottle()

    val binding = new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      new AccessControlEntry(principal, host, AclOperation.UNKNOWN, AclPermissionType.ALLOW))
    val deleteResult = new AclDeleteResult(
      Collections.singletonList(new AclDeleteResult.AclBindingDeleteResult(binding)))
    val fut = CompletableFuture.completedFuture(deleteResult)

    val flt = deleteFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ANY)
    val reqData = new DeleteAclsRequestData().setFilters(Collections.singletonList(flt))
    val built = new DeleteAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubDeleteAclsWith(Seq(fut))))
    try {
      try kafkaApis.handleDeleteAcls(request)
      catch {
        case e: IllegalArgumentException => validateDeleteAclsResponseUnknownMatchingElementsMessage(e)
      }
    } finally kafkaApis.close()
  }
}
