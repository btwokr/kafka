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
import kafka.server.metadata.KRaftMetadataCache
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.OffsetCommitRequestData
import org.apache.kafka.common.message.OffsetCommitResponseData
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleOffsetCommitRequest`.
 *
 * Covers group READ denial, static membership vs inter-broker version, topic READ
 * filtering, unknown topics and partitions, empty authorized-topic batches,
 * coordinator success and failure completions, synchronous coordinator throws,
 * client throttling, forwarded inner requests, offset-commit API version 0 on
 * ZooKeeper (including metadata-too-large), and version 0 on KRaft
 * (`UnsupportedVersionException` from `requireZkOrThrow` with message check).
 */
class HandleOffsetCommitRequestFuzzTest extends KafkaApisTest {

  private def validateUnsupportedVersionRaftOffsetCommitV0Message(e: UnsupportedVersionException): Unit = {
    val expectedMessage =
      "Unsupported when using a Raft-based metadata quorum: Version 0 offset commit requests"
    if (e.getMessage != expectedMessage) throw e
  }

  private def validateRuntimeExceptionCommitOffsetsSyncMessage(e: RuntimeException): Unit = {
    val expectedMessage = "fuzz-offset-commit-commitOffsets-sync"
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

  private def resetOffsetCommitHarness(metadataVersion: MetadataVersion = MetadataVersion.latestTesting()): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, metadataVersion)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), ArgumentMatchers.anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), ArgumentMatchers.anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
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

  private def offsetCommitPartition(
    partitionIndex: Int,
    committedOffset: Long,
    committedMetadata: String,
    includeLeaderEpoch: Boolean
  ): OffsetCommitRequestData.OffsetCommitRequestPartition = {
    val partition = new OffsetCommitRequestData.OffsetCommitRequestPartition()
      .setPartitionIndex(partitionIndex)
      .setCommittedOffset(committedOffset)
      .setCommittedMetadata(committedMetadata)
    if (includeLeaderEpoch)
      partition.setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
    partition
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitGroupReadDenied(data: FuzzedDataProvider): Unit = {
    val throttleMs = data.consumeInt(0, 120)
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val splitSize = data.consumeInt(1, 48)
    val fuzzTail = data.consumeRemainingAsBytes()
    val rawGroup = new String(fuzzTail.take(splitSize))
    val groupId = if (rawGroup.isEmpty) "fuzz-oc-group-deny" else rawGroup
    val topicName = topicSafe(data, "fuzz-oc-topic-deny")

    resetOffsetCommitHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), ArgumentMatchers.anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyGroupRead()))
    try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitStaticMembershipUnsupported(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(7, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-static-g")
    val instanceId = safeString(data, "fuzz-oc-static-instance")
    val topicName = topicSafe(data, "fuzz-oc-static-topic")

    resetOffsetCommitHarness(metadataVersion = MetadataVersion.IBP_2_2_IV1)
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("member")
      .setGenerationIdOrMemberEpoch(2)
      .setGroupInstanceId(instanceId)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 3L, "meta", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitTopicReadDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-topic-deny-g")
    val deniedTopic = topicSafe(data, "fuzz-oc-secret-topic")
    val allowedTopic = topicSafe(data, "fuzz-oc-open-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(allowedTopic, numPartitions = 1)
    addTopicToMetadataCache(deniedTopic, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(deniedTopic)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6))),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(allowedTopic)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 2L, "", includeLeaderEpoch = version >= 6)))).asJava)

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTopicRead(deniedTopic)))
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponseTopic()
            .setName(allowedTopic)
            .setPartitions(Collections.singletonList(
              new OffsetCommitResponseData.OffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitUnknownTopic(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-unknown-g")
    val missingTopic = topicSafe(data, "fuzz-oc-missing-topic")

    resetOffsetCommitHarness()
    stubNoThrottle()

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(missingTopic)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 9L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitUnknownPartition(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-badpart-g")
    val topicName = topicSafe(data, "fuzz-oc-badpart-topic")
    val badPartition = data.consumeInt(1, 16)

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(List(
            offsetCommitPartition(0, 1L, "ok", includeLeaderEpoch = version >= 6),
            offsetCommitPartition(badPartition, 2L, "bad", includeLeaderEpoch = version >= 6)).asJava)))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new OffsetCommitResponseData.OffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitNoAuthorizedPartitions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-empty-g")
    val onlyUnknownTopic = topicSafe(data, "fuzz-oc-only-unknown")

    resetOffsetCommitHarness()
    stubNoThrottle()

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(onlyUnknownTopic)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 0L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitCoordinatorSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-ok-g")
    val topicName = topicSafe(data, "fuzz-oc-ok-topic")
    val committedOffset = data.consumeLong(0L, 1L << 30)

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, committedOffset, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new OffsetCommitResponseData.OffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitCoordinatorCompleteExceptionally(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-fail-g")
    val topicName = topicSafe(data, "fuzz-oc-fail-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitCommitOffsetsThrowsSync(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val groupId = safeString(data, "fuzz-oc-sync-throw-g")
    val topicName = topicSafe(data, "fuzz-oc-sync-throw-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    when(groupCoordinator.commitOffsets(any(), any(), any()))
      .thenThrow(new RuntimeException("fuzz-offset-commit-commitOffsets-sync"))

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      catch {
        case e: RuntimeException => validateRuntimeExceptionCommitOffsetsSyncMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val groupId = safeString(data, "fuzz-oc-throttle-g")
    val topicName = topicSafe(data, "fuzz-oc-throttle-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubClientThrottle(throttleMs)

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new OffsetCommitResponseData.OffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(1, ApiKeys.OFFSET_COMMIT.latestVersion())
    val throttleMs = data.consumeInt(0, 90)
    val groupId = safeString(data, "fuzz-oc-fwd-g")
    val topicName = topicSafe(data, "fuzz-oc-fwd-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubClientThrottle(throttleMs)

    val coordinatorFuture = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(any(), any(), any())).thenReturn(coordinatorFuture)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setMemberId("m")
      .setGenerationIdOrMemberEpoch(1)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = version >= 6)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      coordinatorFuture.complete(new OffsetCommitResponseData()
        .setTopics(Collections.singletonList(
          new OffsetCommitResponseData.OffsetCommitResponseTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
              new OffsetCommitResponseData.OffsetCommitResponsePartition()
                .setPartitionIndex(0)
                .setErrorCode(Errors.NONE.code))))))
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitVersion0ZkPaths(data: FuzzedDataProvider): Unit = {
    val branch = data.consumeInt(0, 1)
    val metaPad = data.consumeInt(4097, 9000)
    val groupId = safeString(data, "fuzz-oc-zk-g")
    val topicName = topicSafe(data, "fuzz-oc-zk-topic")

    resetOffsetCommitHarness()
    addTopicToMetadataCache(topicName, numPartitions = 1)
    stubNoThrottle()

    val oversizedMetadata = "x" * metaPad
    val partition =
      if (branch == 0) offsetCommitPartition(0, 7L, oversizedMetadata, includeLeaderEpoch = false)
      else offsetCommitPartition(0, 11L, "ok", includeLeaderEpoch = false)

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(partition))))

    val built = new OffsetCommitRequest.Builder(requestData).build(0.toShort)
    val request = buildRequest(built)

    when(zkClient.setOrCreateConsumerOffset(anyString(), any(), anyLong())).thenAnswer(_ => ())

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestOffsetCommitVersion0RaftUnsupported(data: FuzzedDataProvider): Unit = {
    val groupId = safeString(data, "fuzz-oc-raft-v0-g")
    val topicName = topicSafe(data, "fuzz-oc-raft-v0-topic")

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.metadataVersion()).thenReturn(MetadataVersion.latestTesting())
    when(metadataCache.contains(topicName)).thenReturn(true)
    when(metadataCache.getPartitionInfo(anyString(), ArgumentMatchers.eq(0)))
      .thenReturn(Some(new UpdateMetadataPartitionState()))
    stubNoThrottle()

    val requestData = new OffsetCommitRequestData()
      .setGroupId(groupId)
      .setTopics(Collections.singletonList(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName(topicName)
          .setPartitions(Collections.singletonList(
            offsetCommitPartition(0, 1L, "", includeLeaderEpoch = false)))))

    val built = new OffsetCommitRequest.Builder(requestData).build(0.toShort)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleOffsetCommitRequest(request, RequestLocal.NoCaching)
      catch {
        case e: UnsupportedVersionException => validateUnsupportedVersionRaftOffsetCommitV0Message(e)
      }
    } finally kafkaApis.close()
  }
}
