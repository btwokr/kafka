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
import org.apache.kafka.common.acl.{
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclOperation,
  AclPermissionType
}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.DescribeAclsRequest
import org.apache.kafka.common.resource.{
  PatternType,
  Resource,
  ResourcePattern,
  ResourcePatternFilter,
  ResourceType
}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDescribeAcls`, which delegates to
 * `AclApis.handleDescribeAcls`.
 *
 * Covers cluster DESCRIBE authorization failure, the no-authorizer branch
 * (`SECURITY_DISABLED` with fixed broker message), the authorizer-present path
 * with empty and non-empty `Authorizer#acls` results (including multiple
 * resource patterns for `DescribeAclsResponse.aclsResources`), client throttling,
 * forwarded inner requests, wire version 0 pattern normalization for `ANY`,
 * `DescribeAclsResponse` validation on wire version 0 when `Authorizer#acls`
 * returns non-literal pattern types (`UnsupportedVersionException` with message check),
 * `DescribeAclsRequest` construction failure (`IllegalArgumentException` with message check),
 * and mixed wire versions with literal filters.
 */
class HandleDescribeAclsRequestFuzzTest extends KafkaApisTest {

  /** Matches `AuthHelper.authorizeClusterOperation` when cluster describe is denied. */
  private def validateClusterAuthorizationExceptionMessage(e: ClusterAuthorizationException): Unit = {
    val message = e.getMessage
    if (message == null || !message.startsWith("Request ") || !message.endsWith(" is not authorized."))
      throw e
  }

  private def validateDescribeAclsV0NonLiteralPatternMessage(e: UnsupportedVersionException): Unit = {
    val expected = "Version 0 only supports literal resource pattern types"
    if (e.getMessage != expected) throw e
  }

  private def validateDescribeAclsUnknownElementsMessage(e: IllegalArgumentException): Unit = {
    val m = e.getMessage
    if (m == null || !m.startsWith("DescribeAclsRequest contains UNKNOWN elements: ")) throw e
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

  private def resetDescribeAclsHarness(): Unit = {
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

  /** Denies only cluster DESCRIBE; other actions default to allowed (unused in these tests). */
  private def authorizerDenyClusterDescribe(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val a = actions.get(0)
      val denied = a.operation == AclOperation.DESCRIBE &&
        a.resourcePattern.resourceType == ResourceType.CLUSTER
      Collections.singletonList(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
    })
    mockAuthorizer
  }

  private def authorizerAllowClusterStubAcls(bindings: util.List[AclBinding]): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    when(auth.acls(any())).thenReturn(bindings)
    auth
  }

  private def bindingTopic(
    topicName: String,
    principalSuffix: String,
    host: String,
    operation: AclOperation,
    permission: AclPermissionType
  ): AclBinding = {
    val principal = if (principalSuffix.startsWith("User:")) principalSuffix else s"User:$principalSuffix"
    new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      new AccessControlEntry(principal, host, operation, permission))
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsSecurityDisabledNoAuthorizer(data: FuzzedDataProvider): Unit = {
    val v0 = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val v1 = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val useFirst = data.consumeBoolean()
    val version = if (useFirst) v0 else v1
    val topicName = topicSafe(data, "fuzz-dacl-topic")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsClusterDescribeDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-deny-topic")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribe()))
    try {
      try kafkaApis.handleDescribeAcls(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthorizationExceptionMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsAuthorizerReturnsEmpty(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dacl-empty-topic")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(Collections.emptyList())))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsAuthorizerReturnsBindings(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DESCRIBE_ACLS.latestVersion())
    val topicA = topicSafe(data, "fuzz-dacl-ta")
    val topicB = topicSafe(data, "fuzz-dacl-tb")
    val suffixA = safeString(data, "Alice")
    val suffixB = safeString(data, "Bob")
    val hostA = safeString(data, "*")
    val hostB = safeString(data, "*")
    val opBranch = data.consumeBoolean()
    val permBranch = data.consumeBoolean()
    val opA = if (opBranch) AclOperation.READ else AclOperation.WRITE
    val opB = if (opBranch) AclOperation.WRITE else AclOperation.READ
    val permA = if (permBranch) AclPermissionType.ALLOW else AclPermissionType.DENY
    val permB = if (permBranch) AclPermissionType.DENY else AclPermissionType.ALLOW

    resetDescribeAclsHarness()
    stubNoThrottle()

    val b1 = bindingTopic(topicA, suffixA, hostA, opA, permA)
    val b2 = bindingTopic(topicB, suffixB, hostB, opB, permB)
    val bindings = util.Arrays.asList(b1, b2)

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(bindings)))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsAuthorizerReturnsClusterBinding(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val principalSuffix = safeString(data, "ClusterAdmin")
    val host = safeString(data, "*")
    val perm = if (data.consumeBoolean()) AclPermissionType.ALLOW else AclPermissionType.DENY

    resetDescribeAclsHarness()
    stubNoThrottle()

    val principal = if (principalSuffix.startsWith("User:")) principalSuffix else s"User:$principalSuffix"
    val clusterBinding = new AclBinding(
      new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      new AccessControlEntry(principal, host, AclOperation.DESCRIBE, perm))
    val bindings = Collections.singletonList(clusterBinding)

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(bindings)))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val topicName = topicSafe(data, "fuzz-dacl-thr-topic")

    resetDescribeAclsHarness()
    stubClientThrottle(throttleMs)

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(Collections.emptyList())))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val throttleMs = data.consumeInt(0, 90)
    val topicName = topicSafe(data, "fuzz-dacl-fwd-topic")

    resetDescribeAclsHarness()
    stubClientThrottle(throttleMs)

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(Collections.emptyList())))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsWireVersion0PatternAnyNormalized(data: FuzzedDataProvider): Unit = {
    val topicName = topicSafe(data, "fuzz-dacl-v0any-topic")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.ANY),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(0.toShort)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(Collections.emptyList())))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsWireVersion0NonLiteralPatternThrows(data: FuzzedDataProvider): Unit = {
    val filterTopicName = topicSafe(data, "fuzz-dacl-v0nl-filter")
    val bindingTopicName = topicSafe(data, "fuzz-dacl-v0nl-acl")
    val useMatchPattern = data.consumeBoolean()
    val principalSuffix = safeString(data, "Alice")
    val host = safeString(data, "*")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val patternType = if (useMatchPattern) PatternType.MATCH else PatternType.PREFIXED
    val principal = if (principalSuffix.startsWith("User:")) principalSuffix else s"User:$principalSuffix"
    val binding = new AclBinding(
      new ResourcePattern(ResourceType.TOPIC, bindingTopicName, patternType),
      new AccessControlEntry(principal, host, AclOperation.READ, AclPermissionType.ALLOW))
    val bindings = Collections.singletonList(binding)

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.TOPIC, filterTopicName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(0.toShort)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(bindings)))
    try {
      try kafkaApis.handleDescribeAcls(request)
      catch {
        case e: UnsupportedVersionException => validateDescribeAclsV0NonLiteralPatternMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsUnknownFilterElementsThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1.toShort, ApiKeys.DESCRIBE_ACLS.latestVersion())
    val name = topicSafe(data, "fuzz-dacl-unkn")

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(ResourceType.UNKNOWN, name, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)

    try {
      try new DescribeAclsRequest.Builder(filter).build(version)
      catch {
        case e: IllegalArgumentException => validateDescribeAclsUnknownElementsMessage(e)
      }
    } finally { }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeAclsMixedWireVersionLiteralFilter(data: FuzzedDataProvider): Unit = {
    val vLow = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val vHigh = data.consumeShort(ApiKeys.DESCRIBE_ACLS.oldestVersion(), ApiKeys.DESCRIBE_ACLS.latestVersion())
    val useLow = data.consumeBoolean()
    val version = if (useLow) vLow else vHigh
    val topicName = topicSafe(data, "fuzz-dacl-mix-topic")
    val groupName = topicSafe(data, "fuzz-dacl-mix-group")
    val rtPick = data.consumeInt(0, 1)
    val resourceType = if (rtPick == 0) ResourceType.TOPIC else ResourceType.GROUP
    val resourceName = if (resourceType == ResourceType.TOPIC) topicName else groupName

    resetDescribeAclsHarness()
    stubNoThrottle()

    val filter = new AclBindingFilter(
      new ResourcePatternFilter(resourceType, resourceName, PatternType.LITERAL),
      AccessControlEntryFilter.ANY)
    val built = new DescribeAclsRequest.Builder(filter).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterStubAcls(Collections.emptyList())))
    try kafkaApis.handleDescribeAcls(request)
    finally kafkaApis.close()
  }
}
