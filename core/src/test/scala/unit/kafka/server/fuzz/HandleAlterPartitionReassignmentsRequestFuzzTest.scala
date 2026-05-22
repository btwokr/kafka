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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.{ReassignablePartition, ReassignableTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, ApiError}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{doAnswer, mock, never, reset, verify, when}

import java.nio.charset.StandardCharsets
import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleAlterPartitionReassignmentsRequest`:
 * KRaft `requireZkOrThrow` / should-always-forward, cluster `ALTER` authorization,
 * empty and multi-topic reassignment maps (including `replicas == null` revert entries),
 * controller callback `Left` vs `Right`, throttling, and forwarded inner requests.
 */
class HandleAlterPartitionReassignmentsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val s = data.consumeString(48)
    if (s == null || s.isEmpty) fallback else s
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def topicFromRaw(raw: String, fallback: String): String = {
    val base = if (raw == null || raw.isEmpty) fallback else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetZkHarness(): Unit = {
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

  /** Denies `ALTER` on the `CLUSTER` resource only. */
  private def authorizerDenyClusterAlter(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        if (action.operation == AclOperation.ALTER &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER) {
          out.add(AuthorizationResult.DENIED)
        } else {
          out.add(AuthorizationResult.ALLOWED)
        }
      }
      out
    })
    auth
  }

  private def validateKRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val m = e.getMessage
    if (m == null || !m.startsWith(prefix)) throw e
    if (!m.contains(ApiKeys.ALTER_PARTITION_REASSIGNMENTS.toString)) throw e
  }

  private def validateClusterAuthDeniedMessage(e: ClusterAuthorizationException): Unit = {
    val m = e.getMessage
    if (m == null || !m.contains("is not authorized")) throw e
  }

  private def intReplicaList(b0: Int, b1: Int): util.List[Integer] =
    util.Arrays.asList(Integer.valueOf(b0), Integer.valueOf(b1))

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsKRaftThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateKRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsKRaftForwardedThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateKRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsClusterAuthDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())

    resetZkHarness()
    stubNoThrottle()

    val built = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterAlter()))
    try {
      try kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthDeniedMessage(e)
      }
      finally verify(controller, never()).alterPartitionReassignments(any(), any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsEmptyTopics(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Left(Map.empty))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val built = new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
      assertEquals(0, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsSinglePartitionWithReplicas(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val topic = topicSafe(data, "fuzz-apr-one-topic")
    val partition = data.consumeInt(0, 7)
    val r0 = data.consumeInt(0, 5)
    val r1 = data.consumeInt(0, 5)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val parts = invocation.getArgument(0).asInstanceOf[Map[TopicPartition, Option[Seq[Int]]]]
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      val tp = new TopicPartition(topic, partition)
      assertEquals(Some(Seq(r0, r1)), parts.get(tp).flatten)
      cb(Left(Map(tp -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(intReplicaList(r0, r1))))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(Errors.NONE.code, response.data.errorCode)
      assertEquals(1, response.data.responses.size)
      val tr = response.data.responses.get(0)
      assertEquals(topic, tr.name)
      assertEquals(1, tr.partitions.size)
      assertEquals(partition, tr.partitions.get(0).partitionIndex)
      assertEquals(Errors.NONE.code, tr.partitions.get(0).errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsRevertNullReplicas(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val topic = topicSafe(data, "fuzz-apr-revert-topic")
    val partition = data.consumeInt(0, 3)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val parts = invocation.getArgument(0).asInstanceOf[Map[TopicPartition, Option[Seq[Int]]]]
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      val tp = new TopicPartition(topic, partition)
      assertEquals(None, parts.get(tp).flatten)
      cb(Left(Map(tp -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(null)))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsTopLevelError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val topic = topicSafe(data, "fuzz-apr-top-topic")
    val partition = data.consumeInt(0, 2)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Right(new ApiError(Errors.NOT_CONTROLLER, "fuzz-controller")))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(intReplicaList(0, 1))))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(Errors.NOT_CONTROLLER.code, response.data.errorCode)
      val msg = response.data.errorMessage
      if (msg == null || !msg.contains("fuzz-controller")) {
        throw new AssertionError(s"unexpected top-level error message: $msg")
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsPerPartitionErrors(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val topicA = topicSafe(data, "fuzz-apr-err-topic-a")
    val topicB = topicSafe(data, "fuzz-apr-err-topic-b")
    val pA = data.consumeInt(0, 2)
    val pB = data.consumeInt(0, 2)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Left(Map(
        new TopicPartition(topicA, pA) -> new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION),
        new TopicPartition(topicB, pB) -> new ApiError(Errors.NOT_LEADER_OR_FOLLOWER, "fuzz-nlof")
      )))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(new ReassignableTopic()
      .setName(topicA)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(pA).setReplicas(intReplicaList(0, 1)))))
    reqData.topics.add(new ReassignableTopic()
      .setName(topicB)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(pB).setReplicas(intReplicaList(1, 0)))))

    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
      assertEquals(2, response.data.responses.size)
      val byName = response.data.responses.asScala.map(tr => tr.name -> tr).toMap
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, byName(topicA).partitions.get(0).errorCode)
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, byName(topicB).partitions.get(0).errorCode)
      val m = byName(topicB).partitions.get(0).errorMessage
      if (m == null || !m.contains("fuzz-nlof")) {
        throw new AssertionError(s"unexpected partition error message: $m")
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsMultiTopicMixedReplicas(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val topicA = topicSafe(data, "fuzz-apr-mix-topic-a")
    val topicB = topicSafe(data, "fuzz-apr-mix-topic-b")
    val pA = data.consumeInt(0, 1)
    val pB = data.consumeInt(0, 1)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val parts = invocation.getArgument(0).asInstanceOf[Map[TopicPartition, Option[Seq[Int]]]]
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      val tpa = new TopicPartition(topicA, pA)
      val tpb = new TopicPartition(topicB, pB)
      assertEquals(Some(Seq(0, 2)), parts.get(tpa).flatten)
      assertEquals(None, parts.get(tpb).flatten)
      cb(Left(Map(tpa -> ApiError.NONE, tpb -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(new ReassignableTopic()
      .setName(topicA)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(pA).setReplicas(intReplicaList(0, 2)))))
    reqData.topics.add(new ReassignableTopic()
      .setName(topicB)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(pB).setReplicas(null))))

    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
      assertEquals(2, response.data.responses.size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val throttleMs = data.consumeInt(1, 500)
    val topic = topicSafe(data, "fuzz-apr-throttle-topic")
    val partition = data.consumeInt(0, 2)

    resetZkHarness()
    stubClientThrottle(throttleMs)

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Left(Map(new TopicPartition(topic, partition) -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(intReplicaList(0, 1))))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(throttleMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsForwardedInnerThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val throttleMs = data.consumeInt(0, 200)
    val topic = topicSafe(data, "fuzz-apr-fwd-topic")
    val partition = data.consumeInt(0, 2)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Left(Map(new TopicPartition(topic, partition) -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(intReplicaList(1, 0))))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(throttleMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterPartitionReassignmentsTopicFromFuzzTail(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.ALTER_PARTITION_REASSIGNMENTS.latestVersion())
    val partition = data.consumeInt(0, 3)
    val splitSize = data.consumeInt(1, 64)
    val tailBytes = data.consumeRemainingAsBytes()
    val rawLen = if (tailBytes == null || tailBytes.isEmpty) 0 else Math.min(splitSize, tailBytes.length)
    val rawTopic =
      if (rawLen == 0) ""
      else new String(tailBytes, 0, rawLen, StandardCharsets.UTF_8)
    val topic = topicFromRaw(rawTopic, "fuzz-apr-tail-topic")

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val cb = invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ApiError], ApiError] => Unit]
      cb(Left(Map(new TopicPartition(topic, partition) -> ApiError.NONE)))
      null
    }).when(controller).alterPartitionReassignments(any(), any())

    val topicData = new ReassignableTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(
        new ReassignablePartition().setPartitionIndex(partition).setReplicas(intReplicaList(0, 1))))
    val reqData = new AlterPartitionReassignmentsRequestData()
    reqData.topics.add(topicData)
    val built = new AlterPartitionReassignmentsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleAlterPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[AlterPartitionReassignmentsResponse](request)
      assertEquals(topic, response.data.responses.get(0).name)
    } finally kafkaApis.close()
  }
}
