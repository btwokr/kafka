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
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.TxnOffsetCommitRequestData
import org.apache.kafka.common.message.TxnOffsetCommitResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.TxnOffsetCommitRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleTxnOffsetCommitRequest`.
 *
 * Covers transactional-id WRITE and group READ denials, per-topic READ filtering,
 * unknown topics and partitions (including mixed valid and invalid partitions on one topic),
 * the empty-authorized-offsets path without calling the group coordinator,
 * coordinator success merge, coordinator failures, synchronous coordinator throws,
 * `COORDINATOR_LOAD_IN_PROGRESS` remapping for wire versions below 2, client throttling,
 * forwarded inner requests, wire version 3+ group metadata fields, and
 * `ensureInterBrokerVersion` (`UnsupportedVersionException` with message check).
 */
class HandleTxnOffsetCommitRequestFuzzTest extends KafkaApisTest {

  /** Matches `KafkaApis.ensureInterBrokerVersion` for this test's cache vs required `IBP_0_11_0_IV0`. */
  private def validateUnsupportedVersionInterBrokerGuardMessage(e: UnsupportedVersionException): Unit = {
    val expectedMessage =
      s"metadata.version: ${MetadataVersion.IBP_0_10_2_IV0} is less than the required version: ${MetadataVersion.IBP_0_11_0_IV0}"
    if (e.getMessage != expectedMessage) throw e
  }

  private def validateRuntimeExceptionCommitTransactionalOffsetsSyncMessage(e: RuntimeException): Unit = {
    val expectedMessage = "fuzz-txn-offset-commit-commitTransactionalOffsets-sync"
    if (e.getMessage != expectedMessage) throw e
  }

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(64)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetTxnOffsetCommitHarness(metadataVersion: MetadataVersion = MetadataVersion.latestTesting()): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, metadataVersion)
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

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
  }

  private def authorizerDenyTransactionalIdWrite(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.WRITE &&
          act.resourcePattern.resourceType == ResourceType.TRANSACTIONAL_ID
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def authorizerDenyGroupRead(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.READ &&
          act.resourcePattern.resourceType == ResourceType.GROUP
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def authorizerDenyTopicRead(deniedTopicName: String): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.READ &&
          act.resourcePattern.resourceType == ResourceType.TOPIC &&
          deniedTopicName == act.resourcePattern.name
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def txnPartition(
    partitionIndex: Int,
    committedOffset: Long,
    committedMetadata: String,
    requestVersion: Short,
    leaderEpochOrNoise: Int
  ): TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition = {
    val partition = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
      .setPartitionIndex(partitionIndex)
      .setCommittedOffset(committedOffset)
      .setCommittedMetadata(committedMetadata)
    if (requestVersion >= 2)
      partition.setCommittedLeaderEpoch(leaderEpochOrNoise)
    partition
  }

  private def baseTxnData(
    transactionalId: String,
    groupId: String,
    producerId: Long,
    producerEpoch: Short,
    requestVersion: Short,
    topics: util.List[TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic],
    memberId: String = "",
    generationId: Int = -1,
    groupInstanceId: String = null
  ): TxnOffsetCommitRequestData = {
    val data = new TxnOffsetCommitRequestData()
      .setTransactionalId(transactionalId)
      .setGroupId(groupId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setTopics(topics)
    if (requestVersion >= 3) {
      data.setMemberId(memberId)
      data.setGenerationId(generationId)
      data.setGroupInstanceId(groupInstanceId)
    }
    data
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitTransactionalIdWriteDenied(data: FuzzedDataProvider): Unit = {
    val versionLow = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val versionHigh = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val useLow = data.consumeBoolean()
    val version = if (useLow) versionLow else versionHigh
    val transactionalId = safeString(data, "fuzz-toc-txn-deny")
    val groupId = safeString(data, "fuzz-toc-g-deny")
    val producerId = data.consumeLong(1L, 1L << 40)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-topic-deny")
    val committedOffset = data.consumeLong(0L, 1L << 30)
    val leaderNoise = data.consumeInt(-5, 2000000000)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    resetTxnOffsetCommitHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTransactionalIdWrite()))
    try kafkaApis.handleTxnOffsetCommitRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitGroupReadDenied(data: FuzzedDataProvider): Unit = {
    val versionLow = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val versionHigh = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val useLow = data.consumeBoolean()
    val version = if (useLow) versionLow else versionHigh
    val transactionalId = safeString(data, "fuzz-toc-gread-txn")
    val groupId = safeString(data, "fuzz-toc-gread-g")
    val producerId = data.consumeLong(1L, 1L << 39)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-gread-topic")
    val committedOffset = data.consumeLong(0L, 1L << 28)
    val leaderNoise = data.consumeInt(-3, 2000000000)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "m", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    resetTxnOffsetCommitHarness()
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyGroupRead()))
    try kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitTopicReadDeniedMixedTopics(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-topdeny-txn")
    val groupId = safeString(data, "fuzz-toc-topdeny-g")
    val producerId = data.consumeLong(1L, 1L << 38)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val deniedTopic = topicSafe(data, "fuzz-toc-secret")
    val allowedTopic = topicSafe(data, "fuzz-toc-open")
    val offDenied = data.consumeLong(0L, 100L)
    val offAllowed = data.consumeLong(0L, 100L)
    val leaderNoiseA = data.consumeInt(-1, 7)
    val leaderNoiseB = data.consumeInt(-1, 7)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(allowedTopic, numPartitions = 1)
    addTopicToMetadataCache(deniedTopic, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(deniedTopic)
          .setPartitions(Collections.singletonList(
            txnPartition(0, offDenied, "", version, leaderNoiseA))),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(allowedTopic)
          .setPartitions(Collections.singletonList(
            txnPartition(0, offAllowed, "", version, leaderNoiseB)))).asJava)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTopicRead(deniedTopic)))
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(allowedTopic)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitUnknownTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-unk-txn")
    val groupId = safeString(data, "fuzz-toc-unk-g")
    val producerId = data.consumeLong(1L, 1L << 37)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val missingTopic = topicSafe(data, "fuzz-toc-missing")
    val committedOffset = data.consumeLong(0L, 1L << 20)
    val leaderNoise = data.consumeInt(0, 9)

    resetTxnOffsetCommitHarness()
    stubNoThrottle()

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(missingTopic)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitUnknownPartitionAndValidSameTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-badpart-txn")
    val groupId = safeString(data, "fuzz-toc-badpart-g")
    val producerId = data.consumeLong(1L, 1L << 36)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-badpart-topic")
    val badPartition = data.consumeInt(1, 16)
    val offOk = data.consumeLong(1L, 100L)
    val offBad = data.consumeLong(1L, 100L)
    val leaderNoiseOk = data.consumeInt(-1, 4)
    val leaderNoiseBad = data.consumeInt(-1, 4)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(List(
            txnPartition(0, offOk, "ok", version, leaderNoiseOk),
            txnPartition(badPartition, offBad, "bad", version, leaderNoiseBad)).asJava)))

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitOnlyInvalidPartitionsNoCoordinator(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-onlybad-txn")
    val groupId = safeString(data, "fuzz-toc-onlybad-g")
    val producerId = data.consumeLong(1L, 1L << 35)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-onlybad-topic")
    val badPartition = data.consumeInt(1, 12)
    val leaderNoise = data.consumeInt(0, 5)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any()))
      .thenThrow(new AssertionError("unexpected commitTransactionalOffsets for empty authorized offsets"))

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(badPartition, 1L, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitUnknownTopicAndValidTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-mix-txn")
    val groupId = safeString(data, "fuzz-toc-mix-g")
    val producerId = data.consumeLong(1L, 1L << 34)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val missingTopic = topicSafe(data, "fuzz-toc-mix-missing")
    val knownTopic = topicSafe(data, "fuzz-toc-mix-known")
    val offMissing = data.consumeLong(0L, 50L)
    val offKnown = data.consumeLong(0L, 50L)
    val leaderNoiseA = data.consumeInt(0, 6)
    val leaderNoiseB = data.consumeInt(0, 6)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(knownTopic, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(missingTopic)
          .setPartitions(Collections.singletonList(
            txnPartition(0, offMissing, "", version, leaderNoiseA))),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(knownTopic)
          .setPartitions(Collections.singletonList(
            txnPartition(0, offKnown, "", version, leaderNoiseB)))).asJava)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(knownTopic)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitCoordinatorSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-ok-txn")
    val groupId = safeString(data, "fuzz-toc-ok-g")
    val producerId = data.consumeLong(1L, 1L << 33)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-ok-topic")
    val committedOffset = data.consumeLong(0L, 1L << 29)
    val leaderNoise = data.consumeInt(-2, 11)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitCoordinatorCompleteExceptionally(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-ex-txn")
    val groupId = safeString(data, "fuzz-toc-ex-g")
    val producerId = data.consumeLong(1L, 1L << 32)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-ex-topic")
    val committedOffset = data.consumeLong(0L, 1L << 19)
    val leaderNoise = data.consumeInt(0, 8)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitCommitTransactionalOffsetsThrowsSync(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-sync-txn")
    val groupId = safeString(data, "fuzz-toc-sync-g")
    val producerId = data.consumeLong(1L, 1L << 31)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-sync-topic")
    val committedOffset = data.consumeLong(0L, 1L << 18)
    val leaderNoise = data.consumeInt(0, 7)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any()))
      .thenThrow(new RuntimeException("fuzz-txn-offset-commit-commitTransactionalOffsets-sync"))

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.NoCaching)
      catch {
        case e: RuntimeException => validateRuntimeExceptionCommitTransactionalOffsetsSyncMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitThrottleResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val transactionalId = safeString(data, "fuzz-toc-thr-txn")
    val groupId = safeString(data, "fuzz-toc-thr-g")
    val producerId = data.consumeLong(1L, 1L << 30)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-thr-topic")
    val committedOffset = data.consumeLong(0L, 1L << 17)
    val leaderNoise = data.consumeInt(0, 6)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubClientThrottle(throttleMs)

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val throttleMs = data.consumeInt(0, 90)
    val transactionalId = safeString(data, "fuzz-toc-fwd-txn")
    val groupId = safeString(data, "fuzz-toc-fwd-g")
    val producerId = data.consumeLong(1L, 1L << 29)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-fwd-topic")
    val committedOffset = data.consumeLong(0L, 1L << 16)
    val leaderNoise = data.consumeInt(0, 5)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubClientThrottle(throttleMs)

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitUnsupportedInterBrokerVersion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(0, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-ibp-txn")
    val groupId = safeString(data, "fuzz-toc-ibp-g")
    val producerId = data.consumeLong(1L, 1L << 28)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-ibp-topic")
    val committedOffset = data.consumeLong(0L, 100L)
    val leaderNoise = data.consumeInt(0, 4)

    resetTxnOffsetCommitHarness(metadataVersion = MetadataVersion.IBP_0_10_2_IV0)
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.NoCaching)
      catch {
        case e: UnsupportedVersionException => validateUnsupportedVersionInterBrokerGuardMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitV3GroupMetadataPassthrough(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(3, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val transactionalId = safeString(data, "fuzz-toc-v3-txn")
    val groupId = safeString(data, "fuzz-toc-v3-g")
    val producerId = data.consumeLong(1L, 1L << 27)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val memberId = safeString(data, "fuzz-toc-member")
    val generationId = data.consumeInt(0, 50)
    val groupInstanceId = safeString(data, "fuzz-toc-instance")
    val topicName = topicSafe(data, "fuzz-toc-v3-topic")
    val committedOffset = data.consumeLong(0L, 1L << 15)
    val leaderNoise = data.consumeInt(0, 3)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "meta", version, leaderNoise)))),
      memberId, generationId, groupInstanceId)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestTxnOffsetCommitCoordinatorLoadInProgressLegacyRemap(data: FuzzedDataProvider): Unit = {
    val vLegacy = data.consumeShort(0, 1)
    val vModern = data.consumeShort(2, ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
    val useLegacy = data.consumeBoolean()
    val version = if (useLegacy) vLegacy else vModern
    val transactionalId = safeString(data, "fuzz-toc-load-txn")
    val groupId = safeString(data, "fuzz-toc-load-g")
    val producerId = data.consumeLong(1L, 1L << 26)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)
    val topicName = topicSafe(data, "fuzz-toc-load-topic")
    val committedOffset = data.consumeLong(0L, 1L << 14)
    val leaderNoise = data.consumeInt(0, 2)

    resetTxnOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(groupCoordinator.commitTransactionalOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = baseTxnData(transactionalId, groupId, producerId, producerEpoch, version,
      Collections.singletonList(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            txnPartition(0, committedOffset, "", version, leaderNoise)))), "", -1, null)

    val built = new TxnOffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)
      coordinatorFuture.complete(new TxnOffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))))))
    } finally kafkaApis.close()
  }
}
