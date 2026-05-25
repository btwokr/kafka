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
import kafka.controller.ReplicaAssignment
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiError, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
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
 * Jazzer fuzz tests for `KafkaApis.handleListPartitionReassignmentsRequest`:
 * KRaft `requireZkOrThrow`, cluster `DESCRIBE` authorization, `topics == null` vs
 * filtered partition sets, controller callback `Left` vs `Right`, throttling, and
 * forwarded inner requests.
 */
class HandleListPartitionReassignmentsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val consumedString = data.consumeString(48)
    if (consumedString == null || consumedString.isEmpty) fallback else consumedString
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
      val authorizationResults = new util.ArrayList[AuthorizationResult]()
      (0 until actions.size()).foreach(_ => authorizationResults.add(AuthorizationResult.ALLOWED))
      authorizationResults
    })
    auth
  }

  /** Denies `DESCRIBE` on the `CLUSTER` resource only. */
  private def authorizerDenyClusterDescribe(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val authorizationResults = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        val denied = action.operation == AclOperation.DESCRIBE &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER &&
          CLUSTER_NAME == action.resourcePattern.name
        authorizationResults.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      authorizationResults
    })
    auth
  }

  private def validateKRaftAlwaysForwardMessage(e: UnsupportedVersionException): Unit = {
    val prefix = "Should always be forwarded to the Active Controller when using a Raft-based metadata quorum: "
    val exceptionMessage = e.getMessage
    if (exceptionMessage == null || !exceptionMessage.startsWith(prefix)) throw e
    if (!exceptionMessage.contains(ApiKeys.LIST_PARTITION_REASSIGNMENTS.toString)) throw e
  }

  private def validateClusterAuthDeniedMessage(e: ClusterAuthorizationException): Unit = {
    val exceptionMessage = e.getMessage
    if (exceptionMessage == null || !exceptionMessage.contains("is not authorized")) throw e
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsKRaftThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = new ListPartitionReassignmentsRequest.Builder(new ListPartitionReassignmentsRequestData()).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleListPartitionReassignmentsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateKRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsKRaftForwardedThrows(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager, forwardingManager)
    stubNoThrottle()

    val built = new ListPartitionReassignmentsRequest.Builder(new ListPartitionReassignmentsRequestData()).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleListPartitionReassignmentsRequest(request)
      catch {
        case e: UnsupportedVersionException => validateKRaftAlwaysForwardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsClusterDescribeDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())

    resetZkHarness()
    stubNoThrottle()

    val built = new ListPartitionReassignmentsRequest.Builder(new ListPartitionReassignmentsRequestData()).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribe()))
    try {
      try kafkaApis.handleListPartitionReassignmentsRequest(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthDeniedMessage(e)
      }
      finally verify(controller, never()).listPartitionReassignments(any(), any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsAllTopicsNullFilter(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val partitionsFilter = invocation.getArgument(0).asInstanceOf[Option[Set[TopicPartition]]]
      assertEquals(None, partitionsFilter)
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Left(Map.empty))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val requestData = new ListPartitionReassignmentsRequestData()
    requestData.setTopics(null)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsEmptyTopicFilter(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val partitionsFilter = invocation.getArgument(0).asInstanceOf[Option[Set[TopicPartition]]]
      assertEquals(Some(Set.empty[TopicPartition]), partitionsFilter)
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Left(Map.empty))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val requestData = new ListPartitionReassignmentsRequestData()
    requestData.setTopics(Collections.emptyList())
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsFilteredPartitions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val topicName = topicSafe(data, "fuzz-lpr-filter-topic")
    val partitionIndex = data.consumeInt(0, 7)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val partitionsFilter = invocation.getArgument(0).asInstanceOf[Option[Set[TopicPartition]]]
      val expectedTopicPartition = new TopicPartition(topicName, partitionIndex)
      assertEquals(Some(Set(expectedTopicPartition)), partitionsFilter)
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Left(Map(expectedTopicPartition -> ReplicaAssignment(Seq(0, 1, 2), Seq(2), Seq(1)))))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicName)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
      assertEquals(1, response.data.topics.size)
      val ongoingTopicReassignment = response.data.topics.get(0)
      assertEquals(topicName, ongoingTopicReassignment.name)
      assertEquals(1, ongoingTopicReassignment.partitions.size)
      val ongoingPartitionReassignment = ongoingTopicReassignment.partitions.get(0)
      assertEquals(partitionIndex, ongoingPartitionReassignment.partitionIndex)
      assertEquals(util.Arrays.asList(0, 1, 2), ongoingPartitionReassignment.replicas)
      assertEquals(util.Arrays.asList(2), ongoingPartitionReassignment.addingReplicas)
      assertEquals(util.Arrays.asList(1), ongoingPartitionReassignment.removingReplicas)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsMultiTopicFilter(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val topicA = topicSafe(data, "fuzz-lpr-multi-a")
    val topicB = topicSafe(data, "fuzz-lpr-multi-b")
    val partitionIndexA = data.consumeInt(0, 2)
    val partitionIndexB = data.consumeInt(0, 2)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val partitionsFilter = invocation.getArgument(0).asInstanceOf[Option[Set[TopicPartition]]]
      val topicPartitionA = new TopicPartition(topicA, partitionIndexA)
      val topicPartitionB = new TopicPartition(topicB, partitionIndexB)
      assertEquals(Some(Set(topicPartitionA, topicPartitionB)), partitionsFilter)
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Left(Map(
        topicPartitionA -> ReplicaAssignment(Seq(0, 1), Seq.empty, Seq.empty),
        topicPartitionB -> ReplicaAssignment(Seq(1, 0), Seq.empty, Seq.empty)
      )))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicA)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndexA))))
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicB)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndexB))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(0, response.data.errorCode)
      assertEquals(2, response.data.topics.size)
      val topicResponsesByName = response.data.topics.asScala.map(ongoing => ongoing.name -> ongoing).toMap
      assertEquals(1, topicResponsesByName(topicA).partitions.size)
      assertEquals(1, topicResponsesByName(topicB).partitions.size)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsTopLevelError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val topicName = topicSafe(data, "fuzz-lpr-err-topic")
    val partitionIndex = data.consumeInt(0, 2)

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Right(new ApiError(Errors.NOT_CONTROLLER, "fuzz-list-controller")))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicName)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(Errors.NOT_CONTROLLER.code, response.data.errorCode)
      val topLevelErrorMessage = response.data.errorMessage
      if (topLevelErrorMessage == null || !topLevelErrorMessage.contains("fuzz-list-controller")) {
        throw new AssertionError(s"unexpected top-level error message: $topLevelErrorMessage")
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val topicName = topicSafe(data, "fuzz-lpr-throttle-topic")
    val partitionIndex = data.consumeInt(0, 2)

    resetZkHarness()
    stubClientThrottle(throttleTimeMs)

    doAnswer(invocation => {
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      val topicPartition = new TopicPartition(topicName, partitionIndex)
      listReassignmentsCompletionCallback(Left(Map(topicPartition -> ReplicaAssignment(Seq(0, 1)))))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicName)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsForwardedInnerThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val throttleTimeMs = data.consumeInt(0, 200)
    val topicName = topicSafe(data, "fuzz-lpr-fwd-topic")
    val partitionIndex = data.consumeInt(0, 2)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)

    doAnswer(invocation => {
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      val topicPartition = new TopicPartition(topicName, partitionIndex)
      listReassignmentsCompletionCallback(Left(Map(topicPartition -> ReplicaAssignment(Seq(1, 0)))))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(topicName)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(throttleTimeMs, response.data.throttleTimeMs)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListPartitionReassignmentsTopicFromFuzzTail(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.oldestVersion(),
      ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion())
    val partitionIndex = data.consumeInt(0, 3)
    val splitSize = data.consumeInt(1, 64)
    val tailBytes = data.consumeRemainingAsBytes()
    val rawTopicByteLength = if (tailBytes == null || tailBytes.isEmpty) 0 else Math.min(splitSize, tailBytes.length)
    val rawTopicPrefix =
      if (rawTopicByteLength == 0) ""
      else new String(tailBytes, 0, rawTopicByteLength, StandardCharsets.UTF_8)
    val sanitizedTopicName = topicFromRaw(rawTopicPrefix, "fuzz-lpr-tail-topic")

    resetZkHarness()
    stubNoThrottle()

    doAnswer(invocation => {
      val partitionsFilter = invocation.getArgument(0).asInstanceOf[Option[Set[TopicPartition]]]
      val expectedTopicPartition = new TopicPartition(sanitizedTopicName, partitionIndex)
      assertEquals(Some(Set(expectedTopicPartition)), partitionsFilter)
      val listReassignmentsCompletionCallback =
        invocation.getArgument(1).asInstanceOf[Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit]
      listReassignmentsCompletionCallback(Left(Map(expectedTopicPartition -> ReplicaAssignment(Seq(0, 1)))))
      null
    }).when(controller).listPartitionReassignments(any(), any())

    val listTopics = new util.ArrayList[ListPartitionReassignmentsTopics]()
    listTopics.add(new ListPartitionReassignmentsTopics()
      .setName(sanitizedTopicName)
      .setPartitionIndexes(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val requestData = new ListPartitionReassignmentsRequestData().setTopics(listTopics)
    val built = new ListPartitionReassignmentsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try {
      kafkaApis.handleListPartitionReassignmentsRequest(request)
      val response = verifyNoThrottling[ListPartitionReassignmentsResponse](request)
      assertEquals(sanitizedTopicName, response.data.topics.get(0).name)
    } finally kafkaApis.close()
  }
}
