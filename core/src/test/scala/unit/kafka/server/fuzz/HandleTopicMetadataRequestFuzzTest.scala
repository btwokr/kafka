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
import kafka.server.{KafkaApisTest, MetadataCache, UnboundedControllerMutationQuota, ZkBrokerEpochManager}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.resource.{ResourceType}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ServerLogConfigs
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleTopicMetadataRequest`.
 *
 * Covers `InvalidRequestException` guards for metadata API versions 10–11 (so
 * the bad request still round-trips through `buildRequest`), all-topics and
 * explicit-topic queries, unknown topic IDs (v12+), DESCRIBE authorization
 * filtering by topic name (including id-based requests resolved to names), client throttling, forwarded inner
 * requests, include-authorized-operations flags, version 0 empty-topic
 * all-topics semantics, and the auto-topic-creation path with
 * `AutoTopicCreationManager`.
 */
class HandleTopicMetadataRequestFuzzTest extends KafkaApisTest {

  private def validateInvalidRequestNullTopicName(e: InvalidRequestException, version: Short): Unit = {
    val expectedMessage = s"Topic name can not be null for version $version"
    if (e.getMessage != expectedMessage) throw e
  }

  private def validateInvalidRequestTopicIdsNotSupported(e: InvalidRequestException, version: Short): Unit = {
    val expectedMessage = s"Topic IDs are not supported in requests for version $version"
    if (e.getMessage != expectedMessage) throw e
  }

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val raw = data.consumeString(48)
    val base = if (raw == null || raw.isEmpty) fallback else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
  }

  private def resetTopicMetadataHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, clientControllerQuotaManager,
      requestChannel, txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any(), ArgumentMatchers.anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong)).thenReturn(0)
  }

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any(), ArgumentMatchers.anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any(), anyLong)).thenReturn(throttleMs)
  }

  private def authorizerDescribeAllowOnly(allowedTopicNames: Set[String]): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val allowed = act.operation == AclOperation.DESCRIBE &&
          act.resourcePattern.resourceType == ResourceType.TOPIC &&
          allowedTopicNames.contains(act.resourcePattern.name)
        out.add(if (allowed) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED)
      }
      out
    })
    mockAuthorizer
  }

  private def authorizerDenyClusterCreateAllowDescribeAndTopicCreate(topicAllowCreate: String): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val res =
          if (act.operation == AclOperation.DESCRIBE && act.resourcePattern.resourceType == ResourceType.TOPIC)
            AuthorizationResult.ALLOWED
          else if (act.operation == AclOperation.CREATE && act.resourcePattern.resourceType == ResourceType.CLUSTER)
            AuthorizationResult.DENIED
          else if (act.operation == AclOperation.CREATE && act.resourcePattern.resourceType == ResourceType.TOPIC)
            if (act.resourcePattern.name == topicAllowCreate) AuthorizationResult.ALLOWED
            else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        out.add(res)
      }
      out
    })
    mockAuthorizer
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataInvalidNullTopicName(data: FuzzedDataProvider): Unit = {
    // Versions 10–11 match `KafkaApisTest.testInvalidMetadataRequestReturnsError`: the request must
    // round-trip through `buildRequest` serialization (flexible encoding); older wire formats NPE on null names.
    val version = data.consumeShort(10, 11)
    val topicEntry = new MetadataRequestTopic().setName(null).setTopicId(Uuid.ZERO_UUID)
    val metadataRequestData = new MetadataRequestData()
      .setTopics(Collections.singletonList(topicEntry))
      .setAllowAutoTopicCreation(true)
    val built = new MetadataRequest(metadataRequestData, version)
    val request = buildRequest(built)

    resetTopicMetadataHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleTopicMetadataRequest(request)
      catch {
        case e: InvalidRequestException => validateInvalidRequestNullTopicName(e, version)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataInvalidTopicIdPreV12(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(10, 11)
    val topicId = Uuid.randomUuid()
    val topicEntry = new MetadataRequestTopic().setName("fuzz-md-tid").setTopicId(topicId)
    val metadataRequestData = new MetadataRequestData()
      .setTopics(Collections.singletonList(topicEntry))
      .setAllowAutoTopicCreation(true)
    val built = new MetadataRequest(metadataRequestData, version)
    val request = buildRequest(built)

    resetTopicMetadataHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleTopicMetadataRequest(request)
      catch {
        case e: InvalidRequestException => validateInvalidRequestTopicIdsNotSupported(e, version)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataAllTopics(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.METADATA.latestVersion())

    resetTopicMetadataHarness()
    addTopicToMetadataCache(safeTopicName(data, "fuzz-md-all-a"), numPartitions = 1, numBrokers = 2)
    stubNoThrottle()

    val built = MetadataRequest.Builder.allTopics.build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataByNameKnownTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4, ApiKeys.METADATA.latestVersion())
    val topicName = safeTopicName(data, "fuzz-md-known")

    resetTopicMetadataHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1, numBrokers = 2)
    stubNoThrottle()

    val built = new MetadataRequest.Builder(Collections.singletonList(topicName), false).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataUnknownTopicIdV12(data: FuzzedDataProvider): Unit = {
    val version = 12.toShort
    val unknownId = Uuid.randomUuid()

    resetTopicMetadataHarness()
    stubNoThrottle()

    val built = new MetadataRequest.Builder(Collections.singletonList(unknownId)).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataMixedTopicIdsAuthorized(data: FuzzedDataProvider): Unit = {
    val version = 12.toShort
    val allowedTopic = safeTopicName(data, "fuzz-md-mix-ok")
    val deniedTopic = safeTopicName(data, "fuzz-md-mix-no")
    val allowedId = Uuid.randomUuid()
    val deniedId = Uuid.randomUuid()

    resetTopicMetadataHarness()
    addTopicToMetadataCache(allowedTopic, numPartitions = 1, numBrokers = 2, topicId = allowedId)
    addTopicToMetadataCache(deniedTopic, numPartitions = 1, numBrokers = 2, topicId = deniedId)
    stubNoThrottle()

    val built = new MetadataRequest.Builder(List(allowedId, deniedId).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDescribeAllowOnly(Set(allowedTopic))))
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataDescribeDeniedByName(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4, ApiKeys.METADATA.latestVersion())
    val allowedTopic = safeTopicName(data, "fuzz-md-ok")
    val deniedTopic = safeTopicName(data, "fuzz-md-no")

    resetTopicMetadataHarness()
    addTopicToMetadataCache(allowedTopic, numPartitions = 1, numBrokers = 2)
    addTopicToMetadataCache(deniedTopic, numPartitions = 1, numBrokers = 2)
    stubNoThrottle()

    val built = new MetadataRequest.Builder(List(allowedTopic, deniedTopic).asJava, false).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDescribeAllowOnly(Set(allowedTopic))))
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4, ApiKeys.METADATA.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val topicName = safeTopicName(data, "fuzz-md-throttle")

    resetTopicMetadataHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1, numBrokers = 2)
    stubClientThrottle(throttleMs)

    val built = new MetadataRequest.Builder(Collections.singletonList(topicName), false).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4, ApiKeys.METADATA.latestVersion())
    val throttleMs = data.consumeInt(0, 80)
    val splitSize = data.consumeInt(1, 24)
    val fuzzTail = data.consumeRemainingAsBytes()
    val topicSuffix = new String(fuzzTail.take(splitSize)).replaceAll("[^a-zA-Z0-9._-]", "_").take(80)
    val topicName = s"fuzz-md-fwd-$topicSuffix".take(200)

    resetTopicMetadataHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1, numBrokers = 2)
    stubClientThrottle(throttleMs)

    val built = new MetadataRequest.Builder(Collections.singletonList(topicName), false).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataIncludeAuthorizedOperations(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(8, 10)
    val topicName = safeTopicName(data, "fuzz-md-auth-ops")
    val includeCluster = data.consumeBoolean()
    val includeTopic = data.consumeBoolean()

    resetTopicMetadataHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1, numBrokers = 2)
    stubNoThrottle()

    val metadataRequestData = new MetadataRequestData()
      .setTopics(Collections.singletonList(new MetadataRequestTopic().setName(topicName).setTopicId(Uuid.ZERO_UUID)))
      .setAllowAutoTopicCreation(false)
      .setIncludeClusterAuthorizedOperations(includeCluster)
      .setIncludeTopicAuthorizedOperations(includeTopic)

    val built = new MetadataRequest(metadataRequestData, version)
    val request = buildRequest(built)

    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach(_ => out.add(AuthorizationResult.ALLOWED))
      out
    })

    val kafkaApis = createKafkaApis(authorizer = Some(mockAuthorizer))
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataVersionZeroEmptyMeansAllTopics(data: FuzzedDataProvider): Unit = {
    val version = 0.toShort

    resetTopicMetadataHarness()
    addTopicToMetadataCache(safeTopicName(data, "fuzz-md-v0"), numPartitions = 1, numBrokers = 2)
    stubNoThrottle()

    val metadataRequestData = new MetadataRequestData()
      .setTopics(Collections.emptyList())
      .setAllowAutoTopicCreation(true)
    val built = new MetadataRequest(metadataRequestData, version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTopicMetadataAutoCreateNonExistingTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4, ApiKeys.METADATA.latestVersion())
    val newTopic = safeTopicName(data, "fuzz-md-autocreate")

    resetTopicMetadataHarness()
    stubNoThrottle()

    when(clientControllerQuotaManager.newPermissiveQuotaFor(any())).thenReturn(UnboundedControllerMutationQuota)
    when(autoTopicCreationManager.createTopics(
      ArgumentMatchers.eq(Set(newTopic)),
      ArgumentMatchers.eq(UnboundedControllerMutationQuota),
      any()
    )).thenReturn(Seq(
      new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
        .setName(newTopic)
        .setTopicId(Uuid.ZERO_UUID)
        .setIsInternal(false)
        .setPartitions(Collections.emptyList())
    ))

    val built = new MetadataRequest.Builder(Collections.singletonList(newTopic), true).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(
      authorizer = Some(authorizerDenyClusterCreateAllowDescribeAndTopicCreate(newTopic)),
      overrideProperties = Map(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> "true")
    )
    try kafkaApis.handleTopicMetadataRequest(request)
    finally kafkaApis.close()
  }
}
