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
import kafka.log.{LogManager, UnifiedLog}
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.DescribeLogDirsRequestData
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsResult
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeLogDirsRequest
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, never, reset, verify, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleDescribeLogDirsRequest`: cluster `DESCRIBE`
 * authorization, the all-partitions path (`topics == null`) using `logManager.allLogs`,
 * targeted topic/partition lists (including multiple topics and empty partition lists),
 * request-quota throttling, and forwarded inner requests.
 */
class HandleDescribeLogDirsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val consumedString = data.consumeString(64)
    if (consumedString == null || consumedString.isEmpty) fallback else consumedString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def resetHarness(): Unit = {
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

  /** Denies `DESCRIBE` on the `CLUSTER` resource only. */
  private def authorizerDenyClusterDescribe(): Authorizer = {
    val auth = mock(classOf[Authorizer])
    when(auth.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult]()
      actions.asScala.foreach { action =>
        val denied = action.operation == AclOperation.DESCRIBE &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER &&
          CLUSTER_NAME == action.resourcePattern.name
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    auth
  }

  private def sampleDescribeLogDirResult(logDirPath: String): DescribeLogDirsResult =
    new DescribeLogDirsResult()
      .setLogDir(logDirPath)
      .setErrorCode(Errors.NONE.code)
      .setTotalBytes(1L << 30)
      .setUsableBytes(1L << 29)

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsClusterDescribeDenied(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val useAllTopicPartitions = data.consumeBoolean()
    val topicName = topicSafe(data, "fuzz-dld-deny-topic")
    val partitionIndex = data.consumeInt(0, 7)

    resetHarness()
    stubNoThrottle()

    val requestData = new DescribeLogDirsRequestData()
    if (useAllTopicPartitions) {
      requestData.setTopics(null)
    } else {
      requestData.topics.add(new DescribableLogDirTopic()
        .setTopic(topicName)
        .setPartitions(Collections.singletonList(Integer.valueOf(partitionIndex))))
    }
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterDescribe()))
    try {
      kafkaApis.handleDescribeLogDirsRequest(request)
      verify(replicaManager, never()).describeLogDirs(any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsAllTopicPartitions(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dld-all-topic")
    val partitionIndex0 = data.consumeInt(0, 11)
    val partitionIndex1 = data.consumeInt(0, 11)
    val mockLogDirPath = safeString(data, "/fuzz/dld-mock-log-dir")

    resetHarness()
    stubNoThrottle()

    val topicPartition0 = new TopicPartition(topicName, partitionIndex0)
    val topicPartition1 = new TopicPartition(topicName, partitionIndex1)
    val unifiedLog0 = mock(classOf[UnifiedLog])
    when(unifiedLog0.topicPartition).thenReturn(topicPartition0)
    val unifiedLog1 = mock(classOf[UnifiedLog])
    when(unifiedLog1.topicPartition).thenReturn(topicPartition1)
    val logManager = mock(classOf[LogManager])
    when(logManager.allLogs).thenReturn(Seq(unifiedLog0, unifiedLog1))
    when(replicaManager.logManager).thenReturn(logManager)

    val expectedPartitions = Set(topicPartition0, topicPartition1)
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(expectedPartitions)).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData().setTopics(null)
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsSpecificSingleTopicMultiplePartitions(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dld-specific-topic")
    val partitionIndex0 = data.consumeInt(0, 9)
    val partitionIndex1 = data.consumeInt(0, 9)
    val mockLogDirPath = safeString(data, "/fuzz/dld-specific-dir")

    resetHarness()
    stubNoThrottle()

    val topicPartition0 = new TopicPartition(topicName, partitionIndex0)
    val topicPartition1 = new TopicPartition(topicName, partitionIndex1)
    val expectedPartitions = Set(topicPartition0, topicPartition1)
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(expectedPartitions)).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData()
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicName)
      .setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex0), Integer.valueOf(partitionIndex1))))
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsSpecificMultiTopic(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val topicNameA = topicSafe(data, "fuzz-dld-topic-a")
    val topicNameB = topicSafe(data, "fuzz-dld-topic-b")
    val partitionTopicA = data.consumeInt(0, 5)
    val partitionTopicB = data.consumeInt(0, 5)
    val mockLogDirPath = safeString(data, "/fuzz/dld-multi-dir")

    resetHarness()
    stubNoThrottle()

    val topicPartitionA = new TopicPartition(topicNameA, partitionTopicA)
    val topicPartitionB = new TopicPartition(topicNameB, partitionTopicB)
    val expectedPartitions = Set(topicPartitionA, topicPartitionB)
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(expectedPartitions)).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData()
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicNameA)
      .setPartitions(Collections.singletonList(Integer.valueOf(partitionTopicA))))
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicNameB)
      .setPartitions(Collections.singletonList(Integer.valueOf(partitionTopicB))))
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsSpecificTopicEmptyPartitionList(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dld-empty-parts")
    val mockLogDirPath = safeString(data, "/fuzz/dld-empty-parts-dir")

    resetHarness()
    stubNoThrottle()

    val expectedPartitions = Set.empty[TopicPartition]
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(expectedPartitions)).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData()
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicName)
      .setPartitions(Collections.emptyList()))
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsNoAuthorizerAllPartitions(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val topicName = topicSafe(data, "fuzz-dld-noauth-topic")
    val partitionIndex = data.consumeInt(0, 4)
    val mockLogDirPath = safeString(data, "/fuzz/dld-noauth-dir")

    resetHarness()
    stubNoThrottle()

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    val unifiedLog = mock(classOf[UnifiedLog])
    when(unifiedLog.topicPartition).thenReturn(topicPartition)
    val logManager = mock(classOf[LogManager])
    when(logManager.allLogs).thenReturn(Seq(unifiedLog))
    when(replicaManager.logManager).thenReturn(logManager)

    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(Set(topicPartition))).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData().setTopics(null)
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val throttleTimeMs = data.consumeInt(1, 500)
    val topicName = topicSafe(data, "fuzz-dld-throttle-topic")
    val partitionIndex = data.consumeInt(0, 4)
    val mockLogDirPath = safeString(data, "/fuzz/dld-throttle-dir")

    resetHarness()
    stubClientThrottle(throttleTimeMs)

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(Set(topicPartition))).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData()
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicName)
      .setPartitions(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val throttleTimeMs = data.consumeInt(0, 200)
    val topicName = topicSafe(data, "fuzz-dld-fwd-topic")
    val partitionIndex = data.consumeInt(0, 4)
    val mockLogDirPath = safeString(data, "/fuzz/dld-fwd-dir")

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)

    val topicPartition = new TopicPartition(topicName, partitionIndex)
    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(Set(topicPartition))).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData()
    requestData.topics.add(new DescribableLogDirTopic()
      .setTopic(topicName)
      .setPartitions(Collections.singletonList(Integer.valueOf(partitionIndex))))
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeLogDirsAllPartitionsNoLogs(data: FuzzedDataProvider): Unit = {
    val requestVersion = data.consumeShort(
      ApiKeys.DESCRIBE_LOG_DIRS.oldestVersion(),
      ApiKeys.DESCRIBE_LOG_DIRS.latestVersion())
    val mockLogDirPath = safeString(data, "/fuzz/dld-no-logs-dir")

    resetHarness()
    stubNoThrottle()

    val logManager = mock(classOf[LogManager])
    when(logManager.allLogs).thenReturn(Seq.empty)
    when(replicaManager.logManager).thenReturn(logManager)

    val describeResults = List(sampleDescribeLogDirResult(mockLogDirPath))
    when(replicaManager.describeLogDirs(Set.empty)).thenReturn(describeResults)

    val requestData = new DescribeLogDirsRequestData().setTopics(null)
    val built = new DescribeLogDirsRequest.Builder(requestData).build(requestVersion)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleDescribeLogDirsRequest(request)
    finally kafkaApis.close()
  }
}
