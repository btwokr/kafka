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
import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.mockito.ArgumentMatchers.{any, anyInt, anyBoolean, anyDouble, anyLong}
import org.mockito.Mockito.{doReturn, mock, reset, spy, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDescribeTopicPartitionsRequest`.
 *
 * Covers the ZooKeeper broker path (`describeTopicPartitionsRequestHandler ==
 * None`), which returns `UNSUPPORTED_VERSION` for every requested topic and
 * routes the response through `RequestHandlerHelper.sendMaybeThrottle`, and
 * the KRaft broker path (`Some(handler)`), which delegates to
 * [[kafka.server.handlers.DescribeTopicPartitionsRequestHandler]] and sends
 * through `sendResponseMaybeThrottle`.
 */
class HandleDescribeTopicPartitionsRequestFuzzTest extends KafkaApisTest {

  private def resetZkHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
  }

  private def resetKRaftHarness(): KRaftMetadataCache = {
    val kraft = spy(MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0))
    metadataCache = kraft
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, autoTopicCreationManager)
    kraft
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

  /** Denies only `DESCRIBE` on `TOPIC` resources (allows other bundled actions). */
  private def authorizerDenyTopicDescribeAll(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val deny = act.operation() == AclOperation.DESCRIBE &&
          act.resourcePattern().resourceType() == ResourceType.TOPIC
        out.add(if (deny) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    auth
  }

  private def validateInvalidCursorMessage(e: InvalidRequestException): Unit = {
    if (!e.getMessage.contains("DescribeTopicPartitionsRequest")) throw e
  }

  private def mockMetadataResponse(topicName: String): DescribeTopicPartitionsResponseData = {
    val d = new DescribeTopicPartitionsResponseData()
    d.topics().add(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
      .setName(topicName)
      .setErrorCode(Errors.NONE.code)
      .setTopicId(Uuid.ZERO_UUID)
      .setIsInternal(false))
    d
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertEquals(0, resp.data().throttleTimeMs)
      resp.data().topics().asScala.foreach { t =>
        assertEquals(Errors.UNSUPPORTED_VERSION.code, t.errorCode)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestZkUnsupportedVersionForwarded(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)
    val throttleMs = data.consumeInt(0, 100)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-fwd-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(req)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      resp.data().topics().asScala.foreach { t =>
        assertEquals(Errors.UNSUPPORTED_VERSION.code, t.errorCode)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestZkUnsupportedVersionThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)
    val throttleMs = data.consumeInt(1, 100)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-throttled-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
      resp.data().topics().asScala.foreach { t =>
        assertEquals(Errors.UNSUPPORTED_VERSION.code, t.errorCode)
      }
    } finally kafkaApis.close()
  }

  /**
   * Several topic names from bounded per-topic byte draws so the
   * unsupported-version error loop exercises multiple entries with varied
   * payloads.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestZkUnsupportedVersionManyTopicsFromFuzzTail(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val numTopics = data.consumeInt(1, 12)
    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val nameLen = data.consumeInt(0, 48)
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-tail-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    resetZkHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertEquals(numTopics, resp.data().topics().size)
      resp.data().topics().asScala.foreach { t =>
        assertEquals(Errors.UNSUPPORTED_VERSION.code, t.errorCode)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftFetchAll(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val responsePartitionLimit = data.consumeInt(-500, 5000)

    val requestData = new DescribeTopicPartitionsRequestData()
      .setTopics(Collections.emptyList())
      .setResponsePartitionLimit(responsePartitionLimit)

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    val kraft = resetKRaftHarness()
    doReturn(Set("fuzz-dtp-b", "fuzz-dtp-a")).when(kraft).getAllTopics()
    doReturn(new DescribeTopicPartitionsResponseData()).when(kraft).getTopicMetadataForDescribeTopicResponse(
      any(), any(), any(), anyInt(), anyBoolean())
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      verifyNoThrottling[DescribeTopicPartitionsResponse](request)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftNamedTopics(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val topicName = {
      val raw = new String(data.consumeBytes(data.consumeInt(0, 48)))
      val base = if (raw.isEmpty) "fuzz-dtp-named" else raw
      base.replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
    }
    val responsePartitionLimit = data.consumeInt(1, 2000)

    val requestData = new DescribeTopicPartitionsRequestData()
      .setResponsePartitionLimit(responsePartitionLimit)
    requestData.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName(topicName))

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    val kraft = resetKRaftHarness()
    doReturn(mockMetadataResponse(topicName)).when(kraft).getTopicMetadataForDescribeTopicResponse(
      any(), any(), any(), anyInt(), anyBoolean())
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertTrue(resp.data().topics().asScala.exists(_.name == topicName))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftTopicDescribeDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    // Fixed literal so the authorizer mock matches `Action.resourcePattern.name` reliably.
    // Jazzer corpora can populate `cursor` / other fields that would otherwise drop fuzzed names
    // from the handler's working topic set.
    data.consumeInt(0, 1024)
    val deniedTopic = "fuzzDtpDeniedTopic"

    val requestData = new DescribeTopicPartitionsRequestData()
    requestData.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName(deniedTopic))

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    assertEquals(1, req.data().topics().size)
    val request = buildRequest(req)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerDenyTopicDescribeAll()))
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      val topics = resp.data().topics().asScala.toSeq
      val denied = topics.find(t => Errors.forCode(t.errorCode) == Errors.TOPIC_AUTHORIZATION_FAILED)
      assertTrue(denied.isDefined)
      assertEquals(deniedTopic, denied.get.name)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftInvalidCursorTopicMissing(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val requestData = new DescribeTopicPartitionsRequestData()
    requestData.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName("aaa-cursor-miss"))
    requestData.setCursor(new DescribeTopicPartitionsRequestData.Cursor()
      .setTopicName("zzz-cursor-miss")
      .setPartitionIndex(0))

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerAllowAll()))
    try {
      try kafkaApis.handleDescribeTopicPartitionsRequest(request)
      catch {
        case e: InvalidRequestException => validateInvalidCursorMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftInvalidCursorNegativePartition(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val negPartition = data.consumeInt(-1000, -1)
    val requestData = new DescribeTopicPartitionsRequestData()
    requestData.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName("fuzz-dtp-cursor-part"))
    requestData.setCursor(new DescribeTopicPartitionsRequestData.Cursor()
      .setTopicName("fuzz-dtp-cursor-part")
      .setPartitionIndex(negPartition))

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    resetKRaftHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerAllowAll()))
    try {
      try kafkaApis.handleDescribeTopicPartitionsRequest(request)
      catch {
        case e: InvalidRequestException => validateInvalidCursorMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeTopicPartitionsKRaftForwardedThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion())
    val throttleMs = data.consumeInt(1, 100)
    val topicName = "fuzz-dtp-fwd"

    val requestData = new DescribeTopicPartitionsRequestData()
    requestData.topics().add(new DescribeTopicPartitionsRequestData.TopicRequest().setName(topicName))

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(req)

    val kraft = resetKRaftHarness()
    doReturn(mockMetadataResponse(topicName)).when(kraft).getTopicMetadataForDescribeTopicResponse(
      any(), any(), any(), anyInt(), anyBoolean())
    stubClientThrottle(throttleMs)

    val kafkaApis = createKafkaApis(raftSupport = true, authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleDescribeTopicPartitionsRequest(request)
      val resp = verifyNoThrottling[DescribeTopicPartitionsResponse](request)
      assertEquals(throttleMs, resp.data().throttleTimeMs)
    } finally kafkaApis.close()
  }
}
