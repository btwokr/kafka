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
import org.apache.kafka.common.acl.{AclBinding, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.message.CreateAclsRequestData
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.CreateAclsRequest
import org.apache.kafka.common.resource.{PatternType, Resource, ResourceType}
import org.apache.kafka.server.authorizer.{AclCreateResult, Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{doAnswer, mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleCreateAcls` (ZK metadata path with
 * `requireZkOrThrow`), delegating to `AclApis.handleCreateAcls`.
 *
 * Covers Raft `shouldAlwaysForward` guard, cluster ALTER denial, the no-authorizer
 * branch (`SecurityDisabledException`), static validation of ACL creations (invalid
 * CLUSTER resource name, empty resource name, mixed valid and invalid bindings),
 * successful and failing `Authorizer#createAcls` completion stages, throttling,
 * forwarded inner requests, and `CreateAclsRequest` wire validation failures
 * (`IllegalArgumentException` / `UnsupportedVersionException` with message checks)
 * before the handler runs.
 */
class HandleCreateAclsRequestFuzzTest extends KafkaApisTest {

  private def validateClusterAuthorizationExceptionMessage(e: ClusterAuthorizationException): Unit = {
    val message = e.getMessage
    if (message == null || !message.startsWith("Request ") || !message.endsWith(" is not authorized."))
      throw e
  }

  private def validateCreateAclsRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val m = e.getMessage
    if (m == null || !m.startsWith(prefix)) throw e
  }

  private def validateCreatableAclsUnknownElementsMessage(e: IllegalArgumentException): Unit = {
    val m = e.getMessage
    if (m == null || !m.startsWith("CreatableAcls contain unknown elements: ")) throw e
  }

  private def validateCreateAclsV0NonLiteralPatternMessage(e: UnsupportedVersionException): Unit = {
    val expected = "Version 0 only supports literal resource pattern types"
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

  private def resetZkCreateAclsHarness(): Unit = {
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

  private def authorizerAllowClusterStubCreateAcls(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    doAnswer(invocation => {
      val bindings = invocation.getArgument(1, classOf[util.List[AclBinding]])
      val out = new util.ArrayList[java.util.concurrent.CompletionStage[AclCreateResult]]()
      var i = 0
      while (i < bindings.size) {
        out.add(CompletableFuture.completedFuture(AclCreateResult.SUCCESS))
        i += 1
      }
      out
    }).when(auth).createAcls(any(), any())
    auth
  }

  private def authorizerAllowClusterStubCreateAclsWith(
    results: Seq[CompletableFuture[AclCreateResult]]
  ): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    val out = new util.ArrayList[java.util.concurrent.CompletionStage[AclCreateResult]]()
    results.foreach(f => out.add(f))
    doAnswer(_ => out).when(auth).createAcls(any(), any())
    auth
  }

  private def aclCreation(
    resourceType: ResourceType,
    resourceName: String,
    patternType: PatternType,
    principal: String,
    host: String,
    operation: AclOperation,
    permission: AclPermissionType,
    requestVersion: Short
  ): AclCreation = {
    val c = new AclCreation()
      .setResourceType(resourceType.code())
      .setResourceName(resourceName)
      .setPrincipal(principal)
      .setHost(host)
      .setOperation(operation.code())
      .setPermissionType(permission.code())
    if (requestVersion >= 1)
      c.setResourcePatternType(patternType.code())
    c
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsRaftAlwaysForwardUnsupported(data: FuzzedDataProvider): Unit = {
    val v0 = data.consumeShort(ApiKeys.CREATE_ACLS.oldestVersion(), ApiKeys.CREATE_ACLS.latestVersion())
    val v1 = data.consumeShort(ApiKeys.CREATE_ACLS.oldestVersion(), ApiKeys.CREATE_ACLS.latestVersion())
    val useFirst = data.consumeBoolean()
    val version = if (useFirst) v0 else v1
    val topicName = topicSafe(data, "fuzz-cacl-raft-topic")
    val principal = safeString(data, "User:Alice")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val createData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(createData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleCreateAcls(request)
      catch {
        case e: UnsupportedVersionException => validateCreateAclsRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsSecurityDisabledNoAuthorizer(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_ACLS.oldestVersion(), ApiKeys.CREATE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-cacl-sec-topic")
    val principal = safeString(data, "User:Bob")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsClusterAlterDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.CREATE_ACLS.oldestVersion(), ApiKeys.CREATE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-cacl-deny-topic")
    val principal = safeString(data, "User:Carol")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterAlter()))
    try {
      try kafkaApis.handleCreateAcls(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthorizationExceptionMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsInvalidClusterResourceName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val badClusterName = topicSafe(data, "fuzz-not-kafka-cluster")
    val principal = safeString(data, "User:Dave")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.CLUSTER, badClusterName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsInvalidEmptyResourceName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val principal = safeString(data, "User:Eve")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, "", PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsMixedInvalidClusterAndValidTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val badClusterName = topicSafe(data, "fuzz-cacl-badcluster")
    val topicName = topicSafe(data, "fuzz-cacl-goodtopic")
    val principal = safeString(data, "User:Frank")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val bad = aclCreation(ResourceType.CLUSTER, badClusterName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val good = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.WRITE, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(List(bad, good).asJava)
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsAuthorizerCreateSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-cacl-ok-topic")
    val principal = safeString(data, "User:Grace")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsAuthorizerCreateReturnsError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-cacl-err-topic")
    val principal = safeString(data, "User:Heidi")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val errFuture = CompletableFuture.completedFuture(
      new AclCreateResult(Errors.INVALID_REQUEST.exception()))
    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAclsWith(Seq(errFuture))))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsTwoCreationsBothSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val topicA = topicSafe(data, "fuzz-cacl-ta")
    val topicB = topicSafe(data, "fuzz-cacl-tb")
    val principal = safeString(data, "User:Ivan")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val c1 = aclCreation(ResourceType.TOPIC, topicA, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val c2 = aclCreation(ResourceType.TOPIC, topicB, PatternType.LITERAL,
      principal, host, AclOperation.WRITE, AclPermissionType.DENY, version)
    val reqData = new CreateAclsRequestData().setCreations(List(c1, c2).asJava)
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val topicName = topicSafe(data, "fuzz-cacl-thr-topic")
    val principal = safeString(data, "User:Judy")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubClientThrottle(throttleMs)

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(0, 90)
    val topicName = topicSafe(data, "fuzz-cacl-fwd-topic")
    val principal = safeString(data, "User:Ken")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubClientThrottle(throttleMs)

    val creation = aclCreation(ResourceType.TOPIC, topicName, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsLiteralClusterNameSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val principal = safeString(data, "User:Leo")
    val host = safeString(data, "*")
    val perm = if (data.consumeBoolean()) AclPermissionType.ALLOW else AclPermissionType.DENY

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL,
      principal, host, AclOperation.ALTER, perm, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))
    val built = new CreateAclsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubCreateAcls()))
    try kafkaApis.handleCreateAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsRequestValidateUnknownElementsThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.CREATE_ACLS.latestVersion())
    val name = topicSafe(data, "fuzz-cacl-unkn")
    val principal = safeString(data, "User:Mia")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = aclCreation(ResourceType.UNKNOWN, name, PatternType.LITERAL,
      principal, host, AclOperation.READ, AclPermissionType.ALLOW, version)
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))

    try {
      try new CreateAclsRequest.Builder(reqData).build(version)
      catch {
        case e: IllegalArgumentException => validateCreatableAclsUnknownElementsMessage(e)
      }
    } finally { }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestCreateAclsRequestValidateV0NonLiteralPatternThrows(data: FuzzedDataProvider): Unit = {
    val topicName = topicSafe(data, "fuzz-cacl-v0pre")
    val principal = safeString(data, "User:Nina")
    val host = safeString(data, "*")

    resetZkCreateAclsHarness()
    stubNoThrottle()

    val creation = new AclCreation()
      .setResourceType(ResourceType.TOPIC.code())
      .setResourceName(topicName)
      .setResourcePatternType(PatternType.PREFIXED.code())
      .setPrincipal(principal)
      .setHost(host)
      .setOperation(AclOperation.READ.code())
      .setPermissionType(AclPermissionType.ALLOW.code())
    val reqData = new CreateAclsRequestData().setCreations(Collections.singletonList(creation))

    try {
      try new CreateAclsRequest.Builder(reqData).build(0.toShort)
      catch {
        case e: UnsupportedVersionException => validateCreateAclsV0NonLiteralPatternMessage(e)
      }
    } finally { }
  }
}
