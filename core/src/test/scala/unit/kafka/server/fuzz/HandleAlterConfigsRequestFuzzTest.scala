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
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AlterConfigsRequestData
import org.apache.kafka.common.message.AlterConfigsRequestData.{
  AlterConfigsResource => LAlterConfigsResource,
  AlterConfigsResourceCollection => LAlterConfigsResourceCollection,
  AlterableConfig => LAlterableConfig,
  AlterableConfigCollection => LAlterableConfigCollection
}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractResponse, AlterConfigsRequest, AlterConfigsResponse, ApiError}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, verify, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleAlterConfigsRequest`: `ConfigAdminManager`
 * preprocessing, empty remaining short-circuit, KRaft forwarding vs
 * `processLegacyAlterConfigsRequest` (ZK / forwarded-on-Raft), authorization,
 * `ZkAdminManager#alterConfigs`, and client throttling.
 */
class HandleAlterConfigsRequestFuzzTest extends KafkaApisTest {

  private def validateAlterConfigsRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val m = e.getMessage
    if (m == null || !m.startsWith(prefix)) throw e
  }

  private def validateNullConfigValueResponseMessage(msg: String): Unit = {
    if (msg == null || !msg.startsWith("Null value not supported for : ")) {
      throw new AssertionError(s"unexpected error message: $msg")
    }
  }

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(48)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetZkAlterConfigsHarness(): Unit = {
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

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
  }

  private def authorizerAllowClusterAlterConfigs(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.ALLOWED))
    auth
  }

  private def authorizerAlterConfigsTopicsAllowDeny(allowTopic: String, denyTopic: String): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { a =>
        if (a.operation == AclOperation.ALTER_CONFIGS &&
          a.resourcePattern.resourceType == ResourceType.TOPIC) {
          out.add(
            if (a.resourcePattern.name == allowTopic) AuthorizationResult.ALLOWED
            else AuthorizationResult.DENIED)
        } else {
          out.add(AuthorizationResult.ALLOWED)
        }
      }
      out
    })
    auth
  }

  private def topicConfigEntry(name: String, value: String): LAlterableConfig =
    new LAlterableConfig().setName(name).setValue(value)

  private def singleTopicAlterConfigsRequest(
    topic: String,
    configName: String,
    configValue: String,
    validateOnly: Boolean,
    version: Short
  ): AlterConfigsRequest = {
    val resource = new ConfigResource(Type.TOPIC, topic)
    val configs = Map(
      resource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry(configName, configValue))))
    new AlterConfigsRequest.Builder(configs.asJava, validateOnly).build(version)
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkEmptyResources(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val built = new AlterConfigsRequest(new AlterConfigsRequestData(), version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsKRaftEmptyResources(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = new AlterConfigsRequest(new AlterConfigsRequestData(), version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsKRaftPreprocessNullValueResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val cfgName = safeString(data, "foo")

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val coll = new LAlterConfigsResourceCollection()
    coll.add(new LAlterConfigsResource()
      .setResourceName(brokerId.toString)
      .setResourceType(Type.BROKER.id())
      .setConfigs(new LAlterableConfigCollection(Collections.singletonList(
        new LAlterableConfig().setName(cfgName).setValue(null)).iterator())))
    val reqData = new AlterConfigsRequestData()
      .setValidateOnly(true)
      .setResources(coll)
    val built = new AlterConfigsRequest(reqData, version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      kafkaApis.handleAlterConfigsRequest(request)
      val response = verifyNoThrottling[AlterConfigsResponse](request)
      val first = response.data().responses().iterator().next()
      validateNullConfigValueResponseMessage(first.errorMessage())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsRaftForwardedProcessLegacyThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-raft-fwd-topic")
    val cfgName = safeString(data, "retention.ms")
    val cfgValue = safeString(data, "1000")

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleAlterConfigsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateAlterConfigsRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkTopicAuthorizedSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val validateOnly = data.consumeBoolean()
    val topic = topicSafe(data, "fuzz-ac-ok-topic")
    val cfgName = safeString(data, "cleanup.policy")
    val cfgValue = safeString(data, "delete")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.TOPIC, topic)
    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly, version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(validateOnly)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkTopicMixedAuthorization(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val allowTopic = topicSafe(data, "fuzz-ac-allow")
    val denyTopic = topicSafe(data, "fuzz-ac-deny")
    val cfgName = safeString(data, "foo")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val authorizedResource = new ConfigResource(Type.TOPIC, allowTopic)
    val unauthorizedResource = new ConfigResource(Type.TOPIC, denyTopic)
    val configs = Map(
      authorizedResource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry(cfgName, "bar"))),
      unauthorizedResource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry(cfgName + "-2", "baz")))
    ).asJava
    val built = new AlterConfigsRequest.Builder(configs, false).build(version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(authorizedResource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAlterConfigsTopicsAllowDeny(allowTopic, denyTopic)))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkBrokerIdMatchSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val validateOnly = data.consumeBoolean()
    val cfgName = safeString(data, "log.cleaner.enable")
    val cfgValue = safeString(data, "false")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.BROKER, brokerId.toString)
    val configs = Map(
      resource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry(cfgName, cfgValue)))).asJava
    val built = new AlterConfigsRequest.Builder(configs, validateOnly).build(version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(validateOnly)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkBrokerClusterWideEmptyNameSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val cfgName = safeString(data, "compression.type")
    val cfgValue = safeString(data, "producer")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.BROKER, "")
    val configs = Map(
      resource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry(cfgName, cfgValue)))).asJava
    val built = new AlterConfigsRequest.Builder(configs, false).build(version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkClientMetricsAuthorized(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val sub = topicSafe(data, "fuzz-ac-metrics-sub")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.CLIENT_METRICS, sub)
    val configs = Map(
      resource -> new AlterConfigsRequest.Config(
        Collections.singletonList(new AlterConfigsRequest.ConfigEntry("interval.ms", "5000")))).asJava
    val built = new AlterConfigsRequest.Builder(configs, false).build(version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkBrokerWrongIdPreprocessResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val wrongId = (brokerId + 1000 + data.consumeInt(1, 9000)).toString
    val cfgName = safeString(data, "num.io.threads")
    val cfgValue = safeString(data, "8")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val coll = new LAlterConfigsResourceCollection()
    coll.add(new LAlterConfigsResource()
      .setResourceType(Type.BROKER.id())
      .setResourceName(wrongId)
      .setConfigs(new LAlterableConfigCollection(
        Collections.singletonList(topicConfigEntry(cfgName, cfgValue)).iterator())))
    val built = new AlterConfigsRequest(new AlterConfigsRequestData().setResources(coll), version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleAlterConfigsRequest(request)
      val response = verifyNoThrottling[AlterConfigsResponse](request)
      val first = response.data().responses().iterator().next()
      val msg = first.errorMessage()
      if (msg == null || !msg.contains("Unexpected broker id") || !msg.contains("expected")) {
        throw new AssertionError(s"unexpected preprocess message: $msg")
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsDataDuplicateResourcesPreprocess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-dupres-topic")
    val cfgName = safeString(data, "x")
    val cfgValue = safeString(data, "y")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val r1 = new LAlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topic)
      .setConfigs(new LAlterableConfigCollection(
        Collections.singletonList(topicConfigEntry(cfgName, cfgValue)).iterator()))
    val r2 = new LAlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topic)
      .setConfigs(new LAlterableConfigCollection(
        Collections.singletonList(topicConfigEntry(cfgName + "2", cfgValue)).iterator()))
    val coll = new LAlterConfigsResourceCollection()
    coll.add(r1)
    coll.add(r2)
    val reqData = new AlterConfigsRequestData().setResources(coll).setValidateOnly(false)
    val built = new AlterConfigsRequest(reqData, version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsDataUnknownResourceTypePreprocess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val name = topicSafe(data, "fuzz-ac-unk-res")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val coll = new LAlterConfigsResourceCollection()
    coll.add(new LAlterConfigsResource()
      .setResourceType(99.toByte)
      .setResourceName(name)
      .setConfigs(new LAlterableConfigCollection(
        Collections.singletonList(topicConfigEntry("k", "v")).iterator())))
    val built = new AlterConfigsRequest(new AlterConfigsRequestData().setResources(coll), version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsDataDuplicateConfigKeysPreprocess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-dupk-topic")
    val v1 = safeString(data, "1")
    val v2 = safeString(data, "2")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val cfgColl = new LAlterableConfigCollection(util.Arrays.asList(
      topicConfigEntry("dup", v1),
      topicConfigEntry("dup", v2)).iterator())
    val coll = new LAlterConfigsResourceCollection()
    coll.add(new LAlterConfigsResource()
      .setResourceType(Type.TOPIC.id())
      .setResourceName(topic)
      .setConfigs(cfgColl))
    val built = new AlterConfigsRequest(new AlterConfigsRequestData().setResources(coll), version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkForwardingToController(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-fwd-topic")
    val cfgName = safeString(data, "segment.bytes")
    val cfgValue = safeString(data, "1000000")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(false)
    stubNoThrottle()

    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildRequest(built)

    val captor = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    val kafkaApis = createKafkaApis(enableForwarding = true)
    try {
      kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
      verify(forwardingManager).forwardRequest(any(), any(), captor.capture())
      val expected = built.getErrorResponse(Errors.NOT_CONTROLLER.exception())
      captor.getValue.apply(Some(expected))
      verifyNoThrottling[AbstractResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsKRaftForwardingToController(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-krfwd-topic")
    val cfgName = safeString(data, "flush.ms")
    val cfgValue = safeString(data, "12345")

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildRequest(built)

    val captor = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])
    val kafkaApis = createKafkaApis(enableForwarding = true, raftSupport = true)
    try {
      kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
      verify(forwardingManager).forwardRequest(any(), any(), captor.capture())
      val expected = built.getErrorResponse(Errors.NOT_CONTROLLER.exception())
      captor.getValue.apply(Some(expected))
      verifyNoThrottling[AbstractResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val topic = topicSafe(data, "fuzz-ac-thr-topic")
    val cfgName = safeString(data, "min.insync.replicas")
    val cfgValue = safeString(data, "1")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubClientThrottle(throttleMs)

    val resource = new ConfigResource(Type.TOPIC, topic)
    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkForwardedInnerEnvelope(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-env-topic")
    val cfgName = safeString(data, "file.delete.delay.ms")
    val cfgValue = safeString(data, "60000")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.TOPIC, topic)
    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildForwardedRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> ApiError.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterConfigsZkAdminManagerReturnsError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion())
    val topic = topicSafe(data, "fuzz-ac-am-err")
    val cfgName = safeString(data, "message.max.bytes")
    val cfgValue = safeString(data, "1000")

    resetZkAlterConfigsHarness()
    when(controller.isActive).thenReturn(true)
    stubNoThrottle()

    val resource = new ConfigResource(Type.TOPIC, topic)
    val built = singleTopicAlterConfigsRequest(topic, cfgName, cfgValue, validateOnly = false, version)
    val request = buildRequest(built)

    when(adminManager.alterConfigs(any(), org.mockito.ArgumentMatchers.eq(false)))
      .thenReturn(Map(resource -> new ApiError(Errors.INVALID_CONFIG, "fuzz-invalid-config")))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowClusterAlterConfigs()))
    try kafkaApis.handleAlterConfigsRequest(request)
    finally kafkaApis.close()
  }
}
