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
import kafka.server.metadata.{MockConfigRepository, ZkMetadataCache}
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.DescribeConfigsRequestData
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.DescribeConfigsRequest
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.metrics.ClientMetricsTestUtils
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleDescribeConfigsRequest`, which delegates to
 * `ConfigHelper.handleDescribeConfigsRequest` and `RequestHandlerHelper.sendResponseMaybeThrottle`.
 *
 * Covers empty resource lists, topic paths (known / unknown / authorization / invalid name /
 * configuration-key filtering with synonyms and documentation flags), broker dynamic defaults and
 * per-broker configs, broker id validation (wrong id, non-integer), broker logger validation and
 * success, client metrics empty name and populated repository reads, mixed topic authorization,
 * request-quota throttling, forwarded inner requests, and unknown `ConfigResource.Type` values
 * that fail during request partitioning.
 */
class HandleDescribeConfigsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val consumedString = data.consumeString(48)
    if (consumedString == null || consumedString.isEmpty) fallback
    else consumedString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetDescribeConfigsHarness(): Unit = {
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

  private def authorizerAllowAll(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      (0 until actions.size()).foreach(_ => out.add(AuthorizationResult.ALLOWED))
      out
    })
    auth
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

  /** Denies `DESCRIBE_CONFIGS` on one topic name; other actions are allowed. */
  private def authorizerDenyDescribeConfigsOnTopic(deniedTopic: String): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        if (action.operation == AclOperation.DESCRIBE_CONFIGS &&
          action.resourcePattern.resourceType == ResourceType.TOPIC) {
          out.add(
            if (action.resourcePattern.name == deniedTopic) AuthorizationResult.DENIED
            else AuthorizationResult.ALLOWED)
        } else {
          out.add(AuthorizationResult.ALLOWED)
        }
      }
      out
    })
    auth
  }

  private def describeConfigsRequestData(
    resources: util.List[DescribeConfigsResource],
    includeSynonyms: Boolean,
    includeDocumentation: Boolean
  ): DescribeConfigsRequestData =
    new DescribeConfigsRequestData()
      .setIncludeSynonyms(includeSynonyms)
      .setIncludeDocumentation(includeDocumentation)
      .setResources(resources)

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsEmptyResources(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(Collections.emptyList(), includeSynonyms, includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsTopicAuthorizedKnown(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val useConfigurationKeys = data.consumeBoolean()
    val configKeyName = safeString(data, "retention.ms")
    val configValue = safeString(data, "1000")
    val topicName = topicSafe(data, "fuzz-dc-known-topic")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(topicName)).thenReturn(true)
    metadataCache = zkCache

    val repo = new MockConfigRepository()
    repo.setTopicConfig(topicName, configKeyName, configValue)

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(topicName)
    if (useConfigurationKeys)
      resource.setConfigurationKeys(Collections.singletonList(configKeyName))

    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()), configRepository = repo)
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsTopicAuthorizedUnknown(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val topicName = topicSafe(data, "fuzz-dc-unknown-topic")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(topicName)).thenReturn(false)
    metadataCache = zkCache

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(topicName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsTopicAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val deniedTopic = topicSafe(data, "fuzz-dc-denied-topic")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(deniedTopic)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyDescribeConfigsOnTopic(deniedTopic)))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsTopicInvalidNameHandled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName("")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsClusterDeniedForBroker(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName("")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribeConfigs()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerClusterWideAndPerBroker(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val brokerClusterWideResource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName("")
    val perBrokerConfigResource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName(brokerId.toString)
    val resources = new util.ArrayList[DescribeConfigsResource]()
    resources.add(brokerClusterWideResource)
    resources.add(perBrokerConfigResource)

    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(resources, includeSynonyms, includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerWrongId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName("999")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerNonIntegerId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER.id)
      .setResourceName("not-an-integer")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerLoggerEmptyName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER_LOGGER.id)
      .setResourceName("")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerLoggerWrongBrokerId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER_LOGGER.id)
      .setResourceName("999")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsBrokerLoggerMatchingBrokerId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.BROKER_LOGGER.id)
      .setResourceName(brokerId.toString)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsClientMetricsEmptyName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)
      .setResourceName("")
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsClientMetricsWithRepository(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val subscriptionName = topicSafe(data, "fuzz-dc-cm-sub")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val repo = new MockConfigRepository()
    ClientMetricsTestUtils.defaultProperties().asScala.foreach { case (propertyName, propertyValue) =>
      repo.setConfig(
        new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName),
        propertyName,
        String.valueOf(propertyValue))
    }

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)
      .setResourceName(subscriptionName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()), configRepository = repo)
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsMixedTopicDeniedAndAllowed(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val deniedTopic = topicSafe(data, "fuzz-dc-mix-denied")
    val allowedTopic = topicSafe(data, "fuzz-dc-mix-allowed")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(allowedTopic)).thenReturn(false)
    metadataCache = zkCache

    val deniedTopicResource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(deniedTopic)
    val allowedTopicResource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(allowedTopic)
    val resources = new util.ArrayList[DescribeConfigsResource]()
    resources.add(deniedTopicResource)
    resources.add(allowedTopicResource)

    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(resources, includeSynonyms, includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyDescribeConfigsOnTopic(deniedTopic)))
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val throttleMs = data.consumeInt(1, 500)
    val topicName = topicSafe(data, "fuzz-dc-throttle-topic")

    resetDescribeConfigsHarness()
    stubClientThrottle(throttleMs)

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(topicName)).thenReturn(true)
    metadataCache = zkCache

    val repo = new MockConfigRepository()
    repo.setTopicConfig(topicName, "retention.ms", "1000")

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(topicName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()), configRepository = repo)
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val throttleMs = data.consumeInt(0, 200)
    val topicName = topicSafe(data, "fuzz-dc-fwd-topic")

    resetDescribeConfigsHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(topicName)).thenReturn(true)
    metadataCache = zkCache

    val repo = new MockConfigRepository()
    repo.setTopicConfig(topicName, "retention.ms", "1000")

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(topicName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()), configRepository = repo)
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsUnknownResourceTypeThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val topicName = topicSafe(data, "fuzz-dc-unknown-type")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.UNKNOWN.id)
      .setResourceName(topicName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      try kafkaApis.handleDescribeConfigsRequest(request)
      catch {
        case e: InvalidRequestException =>
          val msg = e.getMessage
          if (msg == null || !msg.contains("Unexpected resource type")) throw e
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeConfigsNoAuthorizerSecurityDisabled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion())
    val includeSynonyms = data.consumeBoolean()
    val includeDocumentation = data.consumeBoolean()
    val topicName = topicSafe(data, "fuzz-dc-noauth-topic")

    resetDescribeConfigsHarness()
    stubNoThrottle()

    val zkCache = mock(classOf[ZkMetadataCache])
    when(zkCache.contains(topicName)).thenReturn(true)
    metadataCache = zkCache

    val repo = new MockConfigRepository()
    repo.setTopicConfig(topicName, "retention.ms", "1000")

    val resource = new DescribeConfigsResource()
      .setResourceType(ConfigResource.Type.TOPIC.id)
      .setResourceName(topicName)
    val built = new DescribeConfigsRequest.Builder(
      describeConfigsRequestData(
        Collections.singletonList(resource),
        includeSynonyms,
        includeDocumentation)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(configRepository = repo)
    try kafkaApis.handleDescribeConfigsRequest(request)
    finally kafkaApis.close()
  }
}
