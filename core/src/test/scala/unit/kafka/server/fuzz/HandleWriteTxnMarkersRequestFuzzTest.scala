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
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.WriteTxnMarkersRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort}
import org.mockito.Mockito.{mock, reset, when}

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleWriteTxnMarkersRequest`.
 *
 * Covers empty marker batches, cluster authorization (`ALTER` then `CLUSTER_ACTION`),
 * partition magic / hosted checks, mixed magic (append plus format error), append path with the
 * classic group coordinator, `groupCoordinator.completeTransaction` with the new group
 * coordinator, forwarded inner requests, and `ensureInterBrokerVersion` with message checks.
 */
class HandleWriteTxnMarkersRequestFuzzTest extends KafkaApisTest {

  private def validateUnsupportedVersionInterBrokerGuardMessage(e: UnsupportedVersionException): Unit = {
    val expectedMessage =
      s"metadata.version: ${MetadataVersion.IBP_0_10_2_IV0} is less than the required version: ${MetadataVersion.IBP_0_11_0_IV0}"
    if (e.getMessage != expectedMessage) throw e
  }

  private def validateClusterAuthorizationExceptionMessage(e: ClusterAuthorizationException): Unit = {
    val message = e.getMessage
    if (message == null || !message.startsWith("Request ") || !message.endsWith(" is not authorized."))
      throw e
  }

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(48)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def resetWriteTxnHarness(): Unit = {
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

  /** Denies `ALTER` on `CLUSTER` and `CLUSTER_ACTION` on `CLUSTER` (same shape as `KafkaApisTest.requiredAclsNotPresentWriteTxnMarkersThrowsAuthorizationException`). */
  private def authorizerDenyWriteTxnMarkersClusterAcls(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val alterActions = Collections.singletonList(new Action(AclOperation.ALTER, clusterResource, 1, true, false))
    val clusterActions = Collections.singletonList(new Action(AclOperation.CLUSTER_ACTION, clusterResource, 1, true, true))
    val deniedList = Collections.singletonList(AuthorizationResult.DENIED)
    when(mockAuthorizer.authorize(any(), ArgumentMatchers.eq(alterActions))).thenReturn(deniedList)
    when(mockAuthorizer.authorize(any(), ArgumentMatchers.eq(clusterActions))).thenReturn(deniedList)
    mockAuthorizer
  }

  private def buildTxnMarkersRequest(version: Short, entries: java.util.List[TxnMarkerEntry]): WriteTxnMarkersRequest =
    new WriteTxnMarkersRequest.Builder(version, entries).build()

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersEmptyMarkers(data: FuzzedDataProvider): Unit = {
    val useNoCaching = data.consumeBoolean()

    resetWriteTxnHarness()
    stubNoThrottle()

    val built = new WriteTxnMarkersRequest.Builder(
      new WriteTxnMarkersRequestData().setMarkers(new util.ArrayList())
    ).build()
    val request = buildRequest(built)
    val requestLocal =
      if (useNoCaching) RequestLocal.NoCaching
      else RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersClusterAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicName = safeTopicName(data, "fuzz-wtxn-auth")
    val partitionIndex = data.consumeInt(0, 3)
    val producerId = data.consumeLong(1L, 1L << 30)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 5)

    resetWriteTxnHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT,
      Collections.singletonList(topicPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyWriteTxnMarkersClusterAcls()))
    try {
      try kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.NoCaching)
      catch {
        case e: ClusterAuthorizationException =>
          validateClusterAuthorizationExceptionMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersUnknownTopicOrPartition(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicName = safeTopicName(data, "fuzz-wtxn-unk")
    val partitionIndex = data.consumeInt(0, 5)
    val producerId = data.consumeLong(1L, 1L << 31)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 4)

    resetWriteTxnHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.getMagic(topicPartition)).thenReturn(None)

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, TransactionResult.ABORT,
      Collections.singletonList(topicPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersUnsupportedMessageFormat(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicName = safeTopicName(data, "fuzz-wtxn-magic")
    val partitionIndex = data.consumeInt(0, 4)
    val producerId = data.consumeLong(1L, 1L << 32)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 3)

    resetWriteTxnHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.getMagic(topicPartition)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT,
      Collections.singletonList(topicPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersAppendSuccessClassicCoordinator(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicName = safeTopicName(data, "fuzz-wtxn-append")
    val partitionIndex = data.consumeInt(0, 3)
    val producerId = data.consumeLong(1L, 1L << 33)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 3)
    val transactionResult =
      if (data.consumeBoolean()) TransactionResult.COMMIT
      else TransactionResult.ABORT

    resetWriteTxnHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.getMagic(topicPartition)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, transactionResult,
      Collections.singletonList(topicPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val responseCallback = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])
    val entriesCaptor = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, MemoryRecords]])

    when(replicaManager.appendRecords(
      anyLong(),
      anyShort(),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesCaptor.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesCaptor.getValue.map { case (tp, _) => tp -> new PartitionResponse(Errors.NONE) }
      )
    }

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersNewGroupCoordinatorOffsetsTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val partitionIndex = data.consumeInt(0, 3)
    val producerId = data.consumeLong(1L, 1L << 34)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 3)
    val transactionResult =
      if (data.consumeBoolean()) TransactionResult.COMMIT
      else TransactionResult.ABORT

    resetWriteTxnHarness()
    stubNoThrottle()

    val offsetPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionIndex)
    when(replicaManager.getMagic(offsetPartition)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offsetPartition),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(producerEpoch),
      ArgumentMatchers.eq(coordinatorEpoch),
      ArgumentMatchers.eq(transactionResult),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, transactionResult,
      Collections.singletonList(offsetPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(overrideProperties = Map(
      GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "true"
    ))
    try kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersMixedMagicAppendAndError(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicBad = safeTopicName(data, "fuzz-wtxn-bad")
    val topicGood = safeTopicName(data, "fuzz-wtxn-good")
    val partitionBad = data.consumeInt(0, 1)
    val partitionGood = data.consumeInt(0, 1)
    val producerId = data.consumeLong(1L, 1L << 28)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 2)

    resetWriteTxnHarness()
    stubNoThrottle()

    val tpBad = new TopicPartition(topicBad, partitionBad)
    val tpGood = new TopicPartition(topicGood, partitionGood)
    when(replicaManager.getMagic(tpBad)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V1))
    when(replicaManager.getMagic(tpGood)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT,
      util.Arrays.asList(tpBad, tpGood))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildRequest(built)
    val requestLocal = RequestLocal.NoCaching

    val responseCallback = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])
    val entriesCaptor = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, MemoryRecords]])

    when(replicaManager.appendRecords(
      anyLong(),
      anyShort(),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesCaptor.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesCaptor.getValue.map { case (tp, _) => tp -> new PartitionResponse(Errors.NONE) }
      )
    }

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.WRITE_TXN_MARKERS.oldestVersion(), ApiKeys.WRITE_TXN_MARKERS.latestVersion())
    val topicName = safeTopicName(data, "fuzz-wtxn-fwd")
    val partitionIndex = data.consumeInt(0, 2)
    val producerId = data.consumeLong(1L, 1L << 27)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorEpoch = data.consumeInt(0, 2)

    resetWriteTxnHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.getMagic(topicPartition)).thenReturn(Some(RecordBatch.MAGIC_VALUE_V2))

    val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, TransactionResult.COMMIT,
      Collections.singletonList(topicPartition))
    val built = buildTxnMarkersRequest(version, Collections.singletonList(marker))
    val request = buildForwardedRequest(built)

    val requestLocal = RequestLocal.NoCaching
    val responseCallback = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, PartitionResponse] => Unit])
    val entriesCaptor = ArgumentCaptor.forClass(classOf[scala.collection.Map[TopicPartition, MemoryRecords]])

    when(replicaManager.appendRecords(
      anyLong(),
      anyShort(),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesCaptor.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesCaptor.getValue.map { case (tp, _) => tp -> new PartitionResponse(Errors.NONE) }
      )
    }

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestWriteTxnMarkersUnsupportedInterBrokerVersion(data: FuzzedDataProvider): Unit = {
    val useNoCaching = data.consumeBoolean()

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.IBP_0_10_2_IV0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    val requestLocal =
      if (useNoCaching) RequestLocal.NoCaching
      else RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis(MetadataVersion.IBP_0_10_2_IV0)
    try {
      try kafkaApis.handleWriteTxnMarkersRequest(null, requestLocal)
      catch {
        case e: UnsupportedVersionException =>
          validateUnsupportedVersionInterBrokerGuardMessage(e)
      }
    } finally kafkaApis.close()
  }
}
