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
    val s = data.consumeString(48)
    if (s == null || s.isEmpty) fallback
    else s
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
      val topics = invocation.getArgument(0).asInstanceOf[scala.collection.Seq[OffsetForLeaderTopic]]
        topics.map { ot =>
          val parts = ot.partitions.asScala.map { p =>
            new EpochEndOffset()
              .setPartition(p.partition)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(p.leaderEpoch)
              .setEndOffset(100L + p.partition)
          }
          new OffsetForLeaderTopicResult()
            .setTopic(ot.topic)
            .setPartitions(parts.toList.asJava)
        }
      })
  }

  private def authorizerClusterActionAllowed(): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size())
      actions.forEach(_ => out.add(AuthorizationResult.ALLOWED))
      out
    })
    a
  }

  private def authorizerClusterDeniedDescribeTopics(describeAllowed: Set[String]): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
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
    a
  }

  private def buildEpochs(
    topics: Seq[(String, Seq[(Int, Int, Int)])]
  ): OffsetForLeaderTopicCollection = {
    val c = new OffsetForLeaderTopicCollection()
    topics.foreach { case (topic, partitions) =>
      val t = new OffsetForLeaderTopic().setTopic(topic)
      partitions.foreach { case (partitionIndex, leaderEpoch, currentLeaderEpoch) =>
        t.partitions().add(new OffsetForLeaderPartition()
          .setPartition(partitionIndex)
          .setLeaderEpoch(leaderEpoch)
          .setCurrentLeaderEpoch(currentLeaderEpoch))
      }
      c.add(t)
    }
    c
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetForLeaderEpochNoAuthorizerClusterPath(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion())
    val replicaId = data.consumeInt(0, 10)
    val t1 = safeTopicName(data, "fuzz-ofle-nc-a")
    val t2 = safeTopicName(data, "fuzz-ofle-nc-b")
    val p0 = data.consumeInt(0, 7)
    val p1 = data.consumeInt(0, 7)
    val le0 = data.consumeInt(0, 32)
    val le1 = data.consumeInt(0, 32)
    val cle0 = data.consumeInt(-1, 32)
    val cle1 = data.consumeInt(-1, 32)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (t1, Seq((p0, le0, cle0))),
      (t2, Seq((p1, le1, cle1)))
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
    val part = data.consumeInt(0, 15)
    val le = data.consumeInt(0, 64)
    val cle = data.consumeInt(-1, 64)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((part, le, cle)))))
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
    val pA = data.consumeInt(0, 7)
    val pB = data.consumeInt(0, 7)
    val leA = data.consumeInt(0, 32)
    val leB = data.consumeInt(0, 32)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (allowedTopic, Seq((pA, leA, RecordBatch.NO_PARTITION_LEADER_EPOCH))),
      (deniedTopic, Seq((pB, leB, RecordBatch.NO_PARTITION_LEADER_EPOCH)))
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
    val t1 = safeTopicName(data, "fuzz-ofle-all-a")
    val t2 = safeTopicName(data, "fuzz-ofle-all-b")
    val p0 = data.consumeInt(0, 3)
    val p1 = data.consumeInt(0, 3)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (t1, Seq((p0, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH))),
      (t2, Seq((p1, 1, RecordBatch.NO_PARTITION_LEADER_EPOCH)))
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
    val p0 = data.consumeInt(0, 5)
    val p1 = data.consumeInt(0, 5)
    val le0 = data.consumeInt(0, 20)
    val le1 = data.consumeInt(0, 20)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq(
      (topic, Seq(
        (p0, le0, RecordBatch.NO_PARTITION_LEADER_EPOCH),
        (p1, le1, RecordBatch.NO_PARTITION_LEADER_EPOCH)
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
    val part = data.consumeInt(0, 11)
    val le = data.consumeInt(0, 40)
    val cle = data.consumeInt(-1, 40)

    resetZkHarness()
    stubNoThrottle()
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((part, le, cle)))))
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
    val part = data.consumeInt(0, 4)
    val le = data.consumeInt(0, 16)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((part, le, RecordBatch.NO_PARTITION_LEADER_EPOCH)))))
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
    val part = data.consumeInt(0, 6)
    val le = data.consumeInt(0, 24)

    resetZkHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)
    stubReplicaManagerEchoSuccess()

    val epochs = buildEpochs(Seq((topic, Seq((part, le, RecordBatch.NO_PARTITION_LEADER_EPOCH)))))
    val built = OffsetsForLeaderEpochRequest.Builder.forFollower(version, epochs, replicaId).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetForLeaderEpochRequest(request)
    finally kafkaApis.close()
  }
}
