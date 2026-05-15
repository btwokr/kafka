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
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderPartition, OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util

import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleOffsetForLeaderEpochRequest`.
 *
 * Covers cluster-action authorization (implicit allow without an authorizer,
 * explicit allow with an authorizer, and deny with per-topic DESCRIBE splits),
 * `replicaManager.lastOffsetForLeaderEpoch` on authorized requests (including
 * empty and multi-partition topics), synthetic unauthorized partition errors,
 * consumer request wiring (`OffsetsForLeaderEpochRequest.Builder.forConsumer`),
 * throttling, and forwarded inner requests.
 */
class HandleOffsetForLeaderEpochRequestFuzzTest extends KafkaApisTest {

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(48)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def resetZkHarness(): Unit = {
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

  private def stubReplicaManagerEchoSuccess(): Unit = {
    when(replicaManager.lastOffsetForLeaderEpoch(ArgumentMatchers.any())).thenAnswer(invocation => {
      val requestedTopics = invocation.getArgument(0).asInstanceOf[scala.collection.Seq[OffsetForLeaderTopic]]
      requestedTopics.map { requestTopic =>
        val epochEndOffsets = requestTopic.partitions.asScala.map { partitionRequest =>
          new EpochEndOffset()
            .setPartition(partitionRequest.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(partitionRequest.leaderEpoch)
            .setEndOffset(100L + partitionRequest.partition)
        }
        new OffsetForLeaderTopicResult()
          .setTopic(requestTopic.topic)
          .setPartitions(epochEndOffsets.toList.asJava)
      }
    })
  }

  private def authorizerClusterActionAllowed(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach(_ => out.add(AuthorizationResult.ALLOWED))
      out
    })
    mockAuthorizer
  }

  private def authorizerClusterDeniedDescribeTopics(describeAllowed: Set[String]): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach { act =>
        val op = act.operation()
        val rt = act.resourcePattern.resourceType()
        val name = act.resourcePattern.name()
        val res =
          if (rt == ResourceType.CLUSTER && op == AclOperation.CLUSTER_ACTION)
            AuthorizationResult.DENIED
          else if (rt == ResourceType.TOPIC && op == AclOperation.DESCRIBE)
            if (describeAllowed.contains(name)) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          else
            AuthorizationResult.ALLOWED
        out.add(res)
      }
      out
    })
    mockAuthorizer
  }

  private def buildEpochs(
    topics: Seq[(String, Seq[(Int, Int, Int)])]
  ): OffsetForLeaderTopicCollection = {
    val topicCollection = new OffsetForLeaderTopicCollection()
    topics.foreach { case (topic, partitions) =>
      val offsetForLeaderTopic = new OffsetForLeaderTopic().setTopic(topic)
      partitions.foreach { case (partitionIndex, leaderEpoch, currentLeaderEpoch) =>
        offsetForLeaderTopic.partitions().add(new OffsetForLeaderPartition()
          .setPartition(partitionIndex)
          .setLeaderEpoch(leaderEpoch)
          .setCurrentLeaderEpoch(currentLeaderEpoch))
      }
      topicCollection.add(offsetForLeaderTopic)
    }
    topicCollection
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochNoAuthorizerClusterPath(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val topic1 = safeTopicName(data, "fuzz-ofle-nc-a")
    val topic2 = safeTopicName(data, "fuzz-ofle-nc-b")
    val partitionIndex1 = data.consumeInt(0, 7)
    val partitionIndex2 = data.consumeInt(0, 7)
    val leaderEpoch1 = data.consumeInt(0, 32)
    val leaderEpoch2 = data.consumeInt(0, 32)
    val currentLeaderEpoch1 = data.consumeInt(-1, 32)
    val currentLeaderEpoch2 = data.consumeInt(-1, 32)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (topic1, Seq((partitionIndex1, leaderEpoch1, currentLeaderEpoch1))),
      (topic2, Seq((partitionIndex2, leaderEpoch2, currentLeaderEpoch2)))
    ))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochAuthorizerClusterActionAllowed(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val topic = safeTopicName(data, "fuzz-ofle-cl-ok")
    val partitionIndex = data.consumeInt(0, 15)
    val leaderEpoch = data.consumeInt(0, 64)
    val currentLeaderEpoch = data.consumeInt(-1, 64)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((partitionIndex, leaderEpoch, currentLeaderEpoch)))))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerClusterActionAllowed()))
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochClusterDeniedMixedDescribe(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val allowedTopic = safeTopicName(data, "fuzz-ofle-mix-yes")
    val deniedTopic = safeTopicName(data, "fuzz-ofle-mix-no")
    val allowedTopicPartitionIndex = data.consumeInt(0, 7)
    val deniedTopicPartitionIndex = data.consumeInt(0, 7)
    val allowedTopicLeaderEpoch = data.consumeInt(0, 32)
    val deniedTopicLeaderEpoch = data.consumeInt(0, 32)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (allowedTopic, Seq((allowedTopicPartitionIndex, allowedTopicLeaderEpoch, RecordBatch.NO_PARTITION_LEADER_EPOCH))),
      (deniedTopic, Seq((deniedTopicPartitionIndex, deniedTopicLeaderEpoch, RecordBatch.NO_PARTITION_LEADER_EPOCH)))
    ))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(
      authorizerClusterDeniedDescribeTopics(Set(allowedTopic))))
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochClusterDeniedAllTopicsUnauthorized(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val topic1 = safeTopicName(data, "fuzz-ofle-all-a")
    val topic2 = safeTopicName(data, "fuzz-ofle-all-b")
    val partitionIndex1 = data.consumeInt(0, 3)
    val partitionIndex2 = data.consumeInt(0, 3)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (topic1, Seq((partitionIndex1, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH))),
      (topic2, Seq((partitionIndex2, 1, RecordBatch.NO_PARTITION_LEADER_EPOCH)))
    ))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerClusterDeniedDescribeTopics(Set.empty)))
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochEmptyTopics(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = new OffsetForLeaderTopicCollection()
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochMultiPartitionAuthorized(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val topic = safeTopicName(data, "fuzz-ofle-multi")
    val partitionIndex1 = data.consumeInt(0, 5)
    val partitionIndex2 = data.consumeInt(0, 5)
    val leaderEpoch1 = data.consumeInt(0, 20)
    val leaderEpoch2 = data.consumeInt(0, 20)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (topic, Seq(
        (partitionIndex1, leaderEpoch1, RecordBatch.NO_PARTITION_LEADER_EPOCH),
        (partitionIndex2, leaderEpoch2, RecordBatch.NO_PARTITION_LEADER_EPOCH)
      ))
    ))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochConsumerBuilderPath(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(3.toShort, ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val topic = safeTopicName(data, "fuzz-ofle-cons")
    val partitionIndex = data.consumeInt(0, 11)
    val leaderEpoch = data.consumeInt(0, 40)
    val currentLeaderEpoch = data.consumeInt(-1, 40)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((partitionIndex, leaderEpoch, currentLeaderEpoch)))))
    val built = OffsetsForLeaderEpochRequest.Builder.forConsumer(epochs).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(
      authorizerClusterDeniedDescribeTopics(Set(topic))))
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val throttleMs = data.consumeInt(1, 250)
    val topic = safeTopicName(data, "fuzz-ofle-thr")
    val partitionIndex = data.consumeInt(0, 4)
    val leaderEpoch = data.consumeInt(0, 16)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((partitionIndex, leaderEpoch, RecordBatch.NO_PARTITION_LEADER_EPOCH)))))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val reqThrottleMs = data.consumeInt(0, 100)
    val topic = safeTopicName(data, "fuzz-ofle-fwd")
    val partitionIndex = data.consumeInt(0, 6)
    val leaderEpoch = data.consumeInt(0, 24)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((partitionIndex, leaderEpoch, RecordBatch.NO_PARTITION_LEADER_EPOCH)))))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }
}
