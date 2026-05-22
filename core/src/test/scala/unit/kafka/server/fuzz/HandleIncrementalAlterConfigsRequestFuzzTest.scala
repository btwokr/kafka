/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import kafka.server.{KafkaApisTest, KRaftCachedControllerId, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import kafka.utils.Log4jController
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{
  AlterableConfigCollection,
  AlterConfigsResource,
  AlterConfigsResourceCollection
}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, ApiError, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse}
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, spy, verify, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleIncrementalAlterConfigsRequest`:
 * `ConfigAdminManager.preprocess` (with `ALTER_CONFIGS` authorization for
 * `BROKER_LOGGER`), empty-remaining short-circuit, forwarding when the ZK cache
 * reports a KRaft controller (or on a KRaft broker), local persistence with
 * `ZkAdminManager.incrementalAlterConfigs`, mixed topic authorization,
 * `configsAuthorizationApiError` when `CLIENT_METRICS` is unauthorized, and
 * throttling.
 */
class HandleIncrementalAlterConfigsRequestFuzzTest extends KafkaApisTest {

  private def resetZkIncrementalHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
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

  private def safeConfigString(data: FuzzedDataProvider, fallback: String): String = {
    val raw = data.consumeString(48)
    if (raw == null || raw.isEmpty) fallback else raw
  }

  private def topicNameSafe(data: FuzzedDataProvider, fallback: String): String = {
    safeConfigString(data, fallback).replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  /** Sanitize a topic name not sourced from [[FuzzedDataProvider]] (e.g. UTF-8 bytes). */
  private def topicNameFromRawString(rawTopic: String, fallback: String): String = {
    val base = if (rawTopic == null || rawTopic.isEmpty) fallback else rawTopic
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def errorsByResourceName(response: IncrementalAlterConfigsResponse): Map[String, Errors] = {
    response.data.responses.iterator.asScala.map { resourceResponse =>
      resourceResponse.resourceName -> Errors.forCode(resourceResponse.errorCode)
    }.toMap
  }

  private def allPartitionErrors(response: IncrementalAlterConfigsResponse): Seq[Errors] =
    response.data.responses.iterator.asScala.map(r => Errors.forCode(r.errorCode)).toSeq

  private def authorizerAlterConfigsTopicsAllowDeny(allowedTopic: String, deniedTopic: String): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val results = new util.ArrayList[AuthorizationResult]()
      actions.iterator.asScala.foreach { action =>
        if (action.operation == AclOperation.ALTER_CONFIGS &&
          action.resourcePattern.resourceType == ResourceType.TOPIC) {
          val topicName = action.resourcePattern.name
          val decision =
            if (topicName == allowedTopic) AuthorizationResult.ALLOWED
            else if (topicName == deniedTopic) AuthorizationResult.DENIED
            else AuthorizationResult.DENIED
          results.add(decision)
        } else {
          results.add(AuthorizationResult.ALLOWED)
        }
      }
      results
    }
    authorizer
  }

  private def authorizerDenyClusterAlterConfigs(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val results = new util.ArrayList[AuthorizationResult]()
      actions.iterator.asScala.foreach { action =>
        if (action.operation == AclOperation.ALTER_CONFIGS &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER &&
          action.resourcePattern.name == Resource.CLUSTER_NAME)
          results.add(AuthorizationResult.DENIED)
        else
          results.add(AuthorizationResult.ALLOWED)
      }
      results
    }
    authorizer
  }

  private def incrementalRequestFromJavaMap(
    resourceToOps: util.Map[ConfigResource, util.Collection[AlterConfigOp]],
    validateOnly: Boolean,
    requestVersion: Short
  ): RequestChannel.Request = {
    val built = new IncrementalAlterConfigsRequest.Builder(resourceToOps, validateOnly).build(requestVersion)
    buildRequest(built)
  }

  private def singleTopicIncrementalRequest(
    topicName: String,
    configName: String,
    configValue: String,
    validateOnly: Boolean,
    requestVersion: Short,
    opType: OpType
  ): RequestChannel.Request = {
    val topicResource = new ConfigResource(Type.TOPIC, topicName)
    val alterOp = new AlterConfigOp(new ConfigEntry(configName, configValue), opType)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(topicResource, Collections.singletonList(alterOp))
    incrementalRequestFromJavaMap(javaMap, validateOnly, requestVersion)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsEmptyResources(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val built = new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(new IncrementalAlterConfigsRequestData(), built.data())
      assertTrue(response.data.responses.isEmpty)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsBrokerLoggerValidateOnlyPreprocessed(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val logLevel = safeConfigString(data, "INFO")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName(Log4jController.ROOT_LOGGER)
        .setValue(logLevel)
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceName(brokerId.toString)
      .setResourceType(Type.BROKER_LOGGER.id())
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData()
      .setValidateOnly(true)
      .setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(1, response.data.responses.size)
      val resourceResponse = response.data.responses.iterator.next()
      assertEquals(Errors.NONE, Errors.forCode(resourceResponse.errorCode))
      assertEquals(Type.BROKER_LOGGER.id, resourceResponse.resourceType)
      assertEquals(brokerId.toString, resourceResponse.resourceName)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsPreprocessDuplicateResources(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-dup-topic")
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "1000000")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configCollection = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName(configName)
        .setValue(configValue)
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resourceDuplicateA = new AlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topicName)
      .setConfigs(configCollection)
    val resourceDuplicateB = new AlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topicName)
      .setConfigs(new AlterableConfigCollection(Collections.singletonList(
        new IncrementalAlterConfigsRequestData.AlterableConfig()
          .setName(configName)
          .setValue(configValue)
          .setConfigOperation(OpType.SET.id())
      ).iterator()))
    val coll = new AlterConfigsResourceCollection()
    coll.add(resourceDuplicateA)
    coll.add(resourceDuplicateB)
    val requestData = new IncrementalAlterConfigsRequestData().setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      val errors = allPartitionErrors(response)
      assertEquals(2, errors.size)
      assertTrue(errors.forall(_ == Errors.INVALID_REQUEST))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsPreprocessNullConfigValue(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-null-topic")
    val configName = safeConfigString(data, "retention.ms")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName(configName)
        .setValue(null)
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topicName)
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData().setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.INVALID_REQUEST, errorsByResourceName(response)(topicName))
      assertTrue(response.data.responses.iterator.next().errorMessage.startsWith("Null value not supported for : "))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsPreprocessUnknownResourceType(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val unknownResourceTypeId = data.consumeByte()
    val resourceName = topicNameSafe(data, "fuzz-iac-unknown")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName("k")
        .setValue("v")
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceType(unknownResourceTypeId)
      .setResourceName(resourceName)
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData().setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.INVALID_REQUEST, errorsByResourceName(response)(resourceName))
      assertTrue(response.data.responses.iterator.next().errorMessage.startsWith("Unknown resource type "))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsPreprocessDuplicateConfigKeys(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-dupk-topic")
    val value1 = safeConfigString(data, "1")
    val value2 = safeConfigString(data, "2")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(util.Arrays.asList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName("dup")
        .setValue(value1)
        .setConfigOperation(OpType.SET.id()),
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName("dup")
        .setValue(value2)
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topicName)
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData().setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.INVALID_REQUEST, errorsByResourceName(response)(topicName))
      assertTrue(response.data.responses.iterator.next().errorMessage.contains("duplicate config keys"))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsPreprocessBrokerWrongNodeId(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val wrongBrokerName = safeConfigString(data, "999999")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName("log.segment.bytes")
        .setValue("1000")
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceType(Type.BROKER.id())
      .setResourceName(wrongBrokerName)
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData().setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.INVALID_REQUEST, errorsByResourceName(response)(wrongBrokerName))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsZkTopicMixedAuthorization(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val allowedTopic = topicNameSafe(data, "fuzz-iac-ok-topic")
    val deniedTopic = topicNameSafe(data, "fuzz-iac-deny-topic")
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "1000000")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val allowedResource = new ConfigResource(Type.TOPIC, allowedTopic)
    val deniedResource = new ConfigResource(Type.TOPIC, deniedTopic)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(allowedResource, Collections.singletonList(
      new AlterConfigOp(new ConfigEntry(configName, configValue), OpType.SET)))
    javaMap.put(deniedResource, Collections.singletonList(
      new AlterConfigOp(new ConfigEntry(configName, configValue), OpType.SET)))
    val request = incrementalRequestFromJavaMap(javaMap, validateOnly = false, requestVersion)

    when(adminManager.incrementalAlterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(allowedResource -> ApiError.NONE))

    val authorizer = authorizerAlterConfigsTopicsAllowDeny(allowedTopic, deniedTopic)
    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      val byName = errorsByResourceName(response)
      assertEquals(Errors.NONE, byName(allowedTopic))
      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, byName(deniedTopic))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsBrokerLoggerClusterAuthDenied(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val logLevel = safeConfigString(data, "WARN")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val configs = new AlterableConfigCollection(Collections.singletonList(
      new IncrementalAlterConfigsRequestData.AlterableConfig()
        .setName(Log4jController.ROOT_LOGGER)
        .setValue(logLevel)
        .setConfigOperation(OpType.SET.id())
    ).iterator())
    val resource = new AlterConfigsResource()
      .setResourceName(brokerId.toString)
      .setResourceType(Type.BROKER_LOGGER.id())
      .setConfigs(configs)
    val coll = new AlterConfigsResourceCollection(Collections.singletonList(resource).iterator())
    val requestData = new IncrementalAlterConfigsRequestData()
      .setValidateOnly(true)
      .setResources(coll)
    val built = new IncrementalAlterConfigsRequest(requestData, requestVersion)
    val request = buildRequest(built)

    val deniedCluster = authorizerDenyClusterAlterConfigs()
    val kafkaApis = createKafkaApis(authorizer = Some(deniedCluster))
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED, errorsByResourceName(response)(brokerId.toString))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsZkForwardWhenControllerIsKRaft(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-fwdzk-topic")
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "1000000")

    val baseCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    metadataCache = spy(baseCache)
    org.mockito.Mockito.doReturn(Some(KRaftCachedControllerId(2))).when(metadataCache).getControllerId
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    when(controller.isActive).thenReturn(false)
    stubNoThrottle()

    val request = singleTopicIncrementalRequest(topicName, configName, configValue,
      validateOnly = false, requestVersion, OpType.SET)
    val built = request.body[IncrementalAlterConfigsRequest]

    val captor = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    val kafkaApis = createKafkaApis(enableForwarding = true)
    try {
      kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
      verify(forwardingManager).forwardRequest(any(), any(), captor.capture())
      val errorResponse = built.getErrorResponse(Errors.NOT_CONTROLLER.exception())
      captor.getValue.apply(Some(errorResponse))
      verifyNoThrottling[AbstractResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsKRaftBrokerForwards(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-kraft-fwd-topic")
    val configName = safeConfigString(data, "flush.ms")
    val configValue = safeConfigString(data, "12345")

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val request = singleTopicIncrementalRequest(topicName, configName, configValue,
      validateOnly = false, requestVersion, OpType.SET)
    val built = request.body[IncrementalAlterConfigsRequest]

    val captor = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    val kafkaApis = createKafkaApis(enableForwarding = true, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
      verify(forwardingManager).forwardRequest(any(), any(), captor.capture())
      val errorResponse = built.getErrorResponse(Errors.NOT_CONTROLLER.exception())
      captor.getValue.apply(Some(errorResponse))
      verifyNoThrottling[AbstractResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsForwardedInnerProcessesLocally(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-inner-topic")
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "2000000")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val topicResource = new ConfigResource(Type.TOPIC, topicName)
    val alterOp = new AlterConfigOp(new ConfigEntry(configName, configValue), OpType.SET)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(topicResource, Collections.singletonList(alterOp))
    val built = new IncrementalAlterConfigsRequest.Builder(javaMap, false).build(requestVersion)
    val request = buildForwardedRequest(built)

    when(adminManager.incrementalAlterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(topicResource -> ApiError.NONE))

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.NONE, errorsByResourceName(response)(topicName))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val topicName = topicNameSafe(data, "fuzz-iac-throttle-topic")
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "1000000")
    val throttleTimeMs = data.consumeInt(1, 500)

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubClientThrottle(throttleTimeMs)

    val request = singleTopicIncrementalRequest(topicName, configName, configValue,
      validateOnly = false, requestVersion, OpType.SET)
    val topicResource = new ConfigResource(Type.TOPIC, topicName)

    when(adminManager.incrementalAlterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(topicResource -> ApiError.NONE))

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
      assertEquals(Errors.NONE, errorsByResourceName(response)(topicName))
    } finally kafkaApis.close()
  }


  /**
   * When `ALTER_CONFIGS` on the cluster is denied, a `CLIENT_METRICS` resource is
   * classified as unauthorized and `configsAuthorizationApiError` runs. That helper
   * does not handle `CLIENT_METRICS`, so it throws `InvalidRequestException` with the
   * message produced next to `processIncrementalAlterConfigsRequest` in `KafkaApis`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsClientMetricsClusterDeniedUnexpectedResourceType(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val subscriptionName = safeConfigString(data, "fuzz-iac-cm-deny-sub")
    val entryName = safeConfigString(data, "metrics")
    val entryValue = safeConfigString(data, "foo.bar")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val clientMetricsResource = new ConfigResource(Type.CLIENT_METRICS, subscriptionName)
    val alterOp = new AlterConfigOp(new ConfigEntry(entryName, entryValue), OpType.SET)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(clientMetricsResource, Collections.singletonList(alterOp))
    val request = incrementalRequestFromJavaMap(javaMap, validateOnly = false, requestVersion)

    val deniedClusterAuthorizer = authorizerDenyClusterAlterConfigs()
    val kafkaApis = createKafkaApis(authorizer = Some(deniedClusterAuthorizer))
    try {
      val thrown = assertThrows(classOf[InvalidRequestException], () =>
        kafkaApis.handleIncrementalAlterConfigsRequest(request))
      val expectedMessage = s"Unexpected resource type ${Type.CLIENT_METRICS} for resource $subscriptionName"
      assertEquals(expectedMessage, thrown.getMessage)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsClientMetricsAuthorized(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val subscriptionName = safeConfigString(data, "fuzz-iac-client-metrics-sub")
    val entryName = safeConfigString(data, "metrics")
    val entryValue = safeConfigString(data, "foo.bar")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val clientMetricsResource = new ConfigResource(Type.CLIENT_METRICS, subscriptionName)
    val alterOp = new AlterConfigOp(new ConfigEntry(entryName, entryValue), OpType.SET)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(clientMetricsResource, Collections.singletonList(alterOp))
    val request = incrementalRequestFromJavaMap(javaMap, validateOnly = false, requestVersion)

    when(adminManager.incrementalAlterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(clientMetricsResource -> ApiError.NONE))

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      assertEquals(Errors.NONE, errorsByResourceName(response)(subscriptionName))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestIncrementalAlterConfigsTopicNamesFromRemainingBytes(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.oldestVersion(),
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion()
    )
    val firstTopicSliceLength = data.consumeInt(1, 32)
    val secondTopicSliceLength = data.consumeInt(1, 32)
    val configName = safeConfigString(data, "segment.bytes")
    val configValue = safeConfigString(data, "1000000")
    val remainingRawBytes = data.consumeRemainingAsBytes()

    val (firstSlice, afterFirst) = remainingRawBytes.splitAt(firstTopicSliceLength min remainingRawBytes.length)
    val (secondSlice, _) = afterFirst.splitAt(secondTopicSliceLength min afterFirst.length)
    val topicAllowed = topicNameFromRawString(new String(firstSlice, java.nio.charset.StandardCharsets.UTF_8), "fuzz-iac-rem-a")
    val topicDenied = topicNameFromRawString(new String(secondSlice, java.nio.charset.StandardCharsets.UTF_8), "fuzz-iac-rem-b")

    resetZkIncrementalHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val allowedResource = new ConfigResource(Type.TOPIC, topicAllowed)
    val deniedResource = new ConfigResource(Type.TOPIC, topicDenied)
    val javaMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    javaMap.put(allowedResource, Collections.singletonList(
      new AlterConfigOp(new ConfigEntry(configName, configValue), OpType.SET)))
    javaMap.put(deniedResource, Collections.singletonList(
      new AlterConfigOp(new ConfigEntry(configName, configValue), OpType.SET)))
    val request = incrementalRequestFromJavaMap(javaMap, validateOnly = false, requestVersion)

    when(adminManager.incrementalAlterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(allowedResource -> ApiError.NONE))

    val authorizer = authorizerAlterConfigsTopicsAllowDeny(topicAllowed, topicDenied)
    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    try {
      kafkaApis.handleIncrementalAlterConfigsRequest(request)
      val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
      val byName = errorsByResourceName(response)
      assertEquals(Errors.NONE, byName(topicAllowed))
      assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, byName(topicDenied))
    } finally kafkaApis.close()
  }
}
