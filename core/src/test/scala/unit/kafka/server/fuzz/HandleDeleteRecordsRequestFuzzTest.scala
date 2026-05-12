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
import kafka.server.metadata.ZkMetadataCache
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, RequestContext}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDeleteRecordsRequest`.
 *
 * The handler partitions work into unauthorized topics, unknown partitions,
 * and offsets forwarded to `replicaManager.deleteRecords`, then merges all
 * statuses in `sendResponseCallback`.
 */
class HandleDeleteRecordsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  private def resetDefaultZkMetadata(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def installMockMetadataContains(known: Set[TopicPartition]): Unit = {
    metadataCache = mock(classOf[ZkMetadataCache])
    when(metadataCache.contains(any[TopicPartition])).thenAnswer(invocation =>
      known.contains(invocation.getArgument(0)))
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  }

  private def deleteRecordsAuthorizer(allowedTopicNames: Set[String]): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer(invocation => {
        val actions = invocation.getArgument(1, classOf[util.List[Action]])
        val out = new util.ArrayList[AuthorizationResult](actions.size())
        actions.forEach { a =>
          val allowed = a.resourcePattern.resourceType == ResourceType.TOPIC &&
            a.operation == AclOperation.DELETE &&
            allowedTopicNames.contains(a.resourcePattern.name)
          out.add(if (allowed) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED)
        }
        out
      })
    authorizer
  }

  /**
   * Every requested partition is unknown to `metadataCache`; the handler
   * responds without calling `replicaManager.deleteRecords`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsUnknownPartitionsOnly(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val topic = safeString(data, "fuzz-del-unknown")
    val partition = data.consumeInt(0, 7)
    val offset = data.consumeLong(0L, 1L << 20)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition()
              .setPartitionIndex(partition)
              .setOffset(offset)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set.empty)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Authorized partitions present in metadata: `replicaManager.deleteRecords` runs
   * and the completion callback supplies per-partition results.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsReplicaManagerCallback(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val topic = safeString(data, "fuzz-del-rm")
    val partition = data.consumeInt(0, 4)
    val offset = data.consumeLong(0L, 1L << 20)
    val lowWatermark = data.consumeLong(0L, 1L << 10)
    val errorCode = Seq(
      Errors.NONE.code,
      Errors.OFFSET_OUT_OF_RANGE.code,
      Errors.KAFKA_STORAGE_ERROR.code
    )(data.consumeInt(0, 2)).toShort

    val tp = new TopicPartition(topic, partition)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition()
              .setPartitionIndex(partition)
              .setOffset(offset)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set(tp))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    when(replicaManager.deleteRecords(anyLong, any(), any())).thenAnswer(invocation => {
      val callback = invocation.getArgument(2).asInstanceOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit]
      val offsets = invocation.getArgument(1).asInstanceOf[scala.collection.Map[TopicPartition, Long]]
      val responseMap = offsets.map { case (t, _) =>
        t -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(t.partition)
          .setLowWatermark(lowWatermark)
          .setErrorCode(errorCode)
      }
      callback(responseMap.toMap)
    })

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Same topic: one partition exists in metadata and one does not, so the
   * response merges replica results with `UNKNOWN_TOPIC_OR_PARTITION` entries.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsMixedKnownAndUnknownPartitions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val topic = safeString(data, "fuzz-del-mixed")
    val offset0 = data.consumeLong(0L, 1L << 15)
    val offset1 = data.consumeLong(0L, 1L << 15)
    val lowWatermark = data.consumeLong(0L, 1L << 10)

    val tp0 = new TopicPartition(topic, 0)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition().setPartitionIndex(0).setOffset(offset0),
            new DeleteRecordsPartition().setPartitionIndex(1).setOffset(offset1)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set(tp0))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    when(replicaManager.deleteRecords(anyLong, any(), any())).thenAnswer(invocation => {
      val callback = invocation.getArgument(2).asInstanceOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit]
      val offsets = invocation.getArgument(1).asInstanceOf[scala.collection.Map[TopicPartition, Long]]
      val responseMap = offsets.map { case (t, _) =>
        t -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(t.partition)
          .setLowWatermark(lowWatermark)
          .setErrorCode(Errors.NONE.code)
      }
      callback(responseMap.toMap)
    })

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Authorizer denies DELETE on one topic while another is allowed and present
   * in metadata (replica manager path for the allowed topic only).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsTopicAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val allowedBase = safeString(data, "fuzz-del-ok")
    val deniedBase = safeString(data, "fuzz-del-deny")
    val allowedTopic = s"$allowedBase-t"
    val deniedTopic = s"$deniedBase-t"
    val offsetAllow = data.consumeLong(0L, 1L << 15)
    val offsetDeny = data.consumeLong(0L, 1L << 15)
    val lowWatermark = data.consumeLong(0L, 1L << 10)

    val tpAllow = new TopicPartition(allowedTopic, 0)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(allowedTopic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition().setPartitionIndex(0).setOffset(offsetAllow)
          ).asJava)),
        new DeleteRecordsTopic()
          .setName(deniedTopic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition().setPartitionIndex(0).setOffset(offsetDeny)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set(tpAllow, new TopicPartition(deniedTopic, 0)))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    when(replicaManager.deleteRecords(anyLong, any(), any())).thenAnswer(invocation => {
      val callback = invocation.getArgument(2).asInstanceOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit]
      val offsets = invocation.getArgument(1).asInstanceOf[scala.collection.Map[TopicPartition, Long]]
      val responseMap = offsets.map { case (t, _) =>
        t -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(t.partition)
          .setLowWatermark(lowWatermark)
          .setErrorCode(Errors.NONE.code)
      }
      callback(responseMap.toMap)
    })

    val kafkaApis = createKafkaApis(authorizer = Some(deleteRecordsAuthorizer(Set(allowedTopic))))
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }

  /** Request-quota throttle time is applied in `sendResponseMaybeThrottle`. */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val throttleMs = data.consumeInt(1, 500)
    val topic = safeString(data, "fuzz-del-throttle")
    val partition = data.consumeInt(0, 3)
    val offset = data.consumeLong(0L, 1L << 15)
    val lowWatermark = data.consumeLong(0L, 1L << 10)

    val tp = new TopicPartition(topic, partition)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition().setPartitionIndex(partition).setOffset(offset)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set(tp))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    when(replicaManager.deleteRecords(anyLong, any(), any())).thenAnswer(invocation => {
      val callback = invocation.getArgument(2).asInstanceOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit]
      val offsets = invocation.getArgument(1).asInstanceOf[scala.collection.Map[TopicPartition, Long]]
      val responseMap = offsets.map { case (t, _) =>
        t -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(t.partition)
          .setLowWatermark(lowWatermark)
          .setErrorCode(Errors.NONE.code)
      }
      callback(responseMap.toMap)
    })

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Forwarded inner request: channel throttling in `sendResponseMaybeThrottle` is
   * skipped when `request.isForwarded` is true.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteRecordsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DELETE_RECORDS.oldestVersion().toInt,
      ApiKeys.DELETE_RECORDS.latestVersion().toInt).toShort
    val timeoutMs = data.consumeInt(1000, 120_000)
    val throttleMs = data.consumeInt(0, 200)
    val topic = safeString(data, "fuzz-del-fwd")
    val partition = data.consumeInt(0, 2)
    val offset = data.consumeLong(0L, 1L << 15)
    val lowWatermark = data.consumeLong(0L, 1L << 10)

    val tp = new TopicPartition(topic, partition)

    val requestData = new DeleteRecordsRequestData()
      .setTimeoutMs(timeoutMs)
      .setTopics(new util.ArrayList[DeleteRecordsTopic](Seq(
        new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(new util.ArrayList[DeleteRecordsPartition](Seq(
            new DeleteRecordsPartition().setPartitionIndex(partition).setOffset(offset)
          ).asJava))
      ).asJava))

    val built = new DeleteRecordsRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(built)

    resetDefaultZkMetadata()
    installMockMetadataContains(Set(tp))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    when(replicaManager.deleteRecords(anyLong, any(), any())).thenAnswer(invocation => {
      val callback = invocation.getArgument(2).asInstanceOf[Map[TopicPartition, DeleteRecordsPartitionResult] => Unit]
      val offsets = invocation.getArgument(1).asInstanceOf[scala.collection.Map[TopicPartition, Long]]
      val responseMap = offsets.map { case (t, _) =>
        t -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(t.partition)
          .setLowWatermark(lowWatermark)
          .setErrorCode(Errors.NONE.code)
      }
      callback(responseMap.toMap)
    })

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDeleteRecordsRequest(request)
    finally kafkaApis.close()
  }
}
