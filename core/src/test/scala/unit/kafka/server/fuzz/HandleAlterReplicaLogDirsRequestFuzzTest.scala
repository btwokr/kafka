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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.{
  AlterReplicaLogDir,
  AlterReplicaLogDirTopic
}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, never, reset, verify, when}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleAlterReplicaLogDirsRequest`: cluster
 * `ALTER` authorization, `replicaManager.alterReplicaLogDirs` success path with
 * topic grouping in the response, empty alteration lists, multiple directories
 * and topics, request-quota throttling, and forwarded inner requests.
 */
class HandleAlterReplicaLogDirsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val consumedString = data.consumeString(64)
    if (consumedString == null || consumedString.isEmpty) fallback else consumedString
  }

  private def topicSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    raw.replaceAll("[^a-zA-Z0-9._-]", "_").take(249)
  }

  private def pathSafe(data: FuzzedDataProvider, fallback: String): String = {
    val raw = safeString(data, fallback)
    if (raw.startsWith("/")) raw.take(512) else "/" + raw.take(511)
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

  private def stubClientThrottle(throttleMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
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
        val denied = action.operation == AclOperation.ALTER &&
          action.resourcePattern.resourceType == ResourceType.CLUSTER &&
          CLUSTER_NAME == action.resourcePattern.name
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    auth
  }

  private val fuzzErrorPool: Array[Errors] = Array(
    Errors.NONE,
    Errors.UNKNOWN_TOPIC_OR_PARTITION,
    Errors.NOT_LEADER_OR_FOLLOWER,
    Errors.LOG_DIR_NOT_FOUND,
    Errors.KAFKA_STORAGE_ERROR,
    Errors.INVALID_TOPIC_EXCEPTION
  )

  private def pickError(data: FuzzedDataProvider): Errors =
    fuzzErrorPool(data.consumeInt(0, fuzzErrorPool.length - 1))

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsClusterAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-deny")
    val topicName = topicSafe(data, "fuzz-arl-deny-topic")
    val partition0 = data.consumeInt(0, 7)
    val partition1 = data.consumeInt(0, 7)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(
      Integer.valueOf(partition0), Integer.valueOf(partition1))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterAlter()))
    try {
      kafkaApis.handleAlterReplicaLogDirsRequest(request)
      verify(replicaManager, never()).alterReplicaLogDirs(any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsEmptyDirsAuthorized(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())

    resetHarness()
    stubNoThrottle()
    when(replicaManager.alterReplicaLogDirs(Map.empty[TopicPartition, String]))
      .thenReturn(Map.empty[TopicPartition, Errors])

    val reqData = new AlterReplicaLogDirsRequestData()
    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsSingleTopicMultiplePartitions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-one")
    val topicName = topicSafe(data, "fuzz-arl-one-topic")
    val partition0 = data.consumeInt(0, 11)
    val partition1 = data.consumeInt(0, 11)
    val partition2 = data.consumeInt(0, 11)
    val errorPartition0 = pickError(data)
    val errorPartition1 = pickError(data)
    val errorPartition2 = pickError(data)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(
      Integer.valueOf(partition0), Integer.valueOf(partition1), Integer.valueOf(partition2))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartition0 = new TopicPartition(topicName, partition0)
    val topicPartition1 = new TopicPartition(topicName, partition1)
    val topicPartition2 = new TopicPartition(topicName, partition2)
    val replicaManagerResults = Map(
      topicPartition0 -> errorPartition0,
      topicPartition1 -> errorPartition1,
      topicPartition2 -> errorPartition2)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(replicaManagerResults)

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsMultipleTopicsGrouped(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-multi")
    val topicNameA = topicSafe(data, "fuzz-arl-topic-a")
    val topicNameB = topicSafe(data, "fuzz-arl-topic-b")
    val partitionTopicA = data.consumeInt(0, 5)
    val partitionTopicB = data.consumeInt(0, 5)
    val errorTopicA = pickError(data)
    val errorTopicB = pickError(data)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicNameA).setPartitions(util.Arrays.asList(Integer.valueOf(partitionTopicA))))
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicNameB).setPartitions(util.Arrays.asList(Integer.valueOf(partitionTopicB))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartitionA = new TopicPartition(topicNameA, partitionTopicA)
    val topicPartitionB = new TopicPartition(topicNameB, partitionTopicB)
    val replicaManagerResults = Map(topicPartitionA -> errorTopicA, topicPartitionB -> errorTopicB)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(replicaManagerResults)

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsTwoDirectories(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPathFirst = pathSafe(data, "/fuzz/arl-d1")
    val logDirectoryPathSecond = pathSafe(data, "/fuzz/arl-d2")
    val topicName = topicSafe(data, "fuzz-arl-two-dir-topic")
    val partitionFirstDirectory = data.consumeInt(0, 4)
    val partitionSecondDirectory = data.consumeInt(0, 4)
    val errorFirstDirectory = pickError(data)
    val errorSecondDirectory = pickError(data)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDirFirst = new AlterReplicaLogDir().setPath(logDirectoryPathFirst)
    alterReplicaLogDirFirst.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionFirstDirectory))))
    val alterReplicaLogDirSecond = new AlterReplicaLogDir().setPath(logDirectoryPathSecond)
    alterReplicaLogDirSecond.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionSecondDirectory))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDirFirst)
    reqData.dirs().add(alterReplicaLogDirSecond)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartitionFirstDirectory = new TopicPartition(topicName, partitionFirstDirectory)
    val topicPartitionSecondDirectory = new TopicPartition(topicName, partitionSecondDirectory)
    val replicaManagerResults = Map(
      topicPartitionFirstDirectory -> errorFirstDirectory,
      topicPartitionSecondDirectory -> errorSecondDirectory)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(replicaManagerResults)

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsNoAuthorizer(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-noauth")
    val topicName = topicSafe(data, "fuzz-arl-noauth")
    val partitionIndex = data.consumeInt(0, 3)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(topicPartition -> Errors.NONE))

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val throttleMs = data.consumeInt(1, 500)
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-throttle")
    val topicName = topicSafe(data, "fuzz-arl-throttle")
    val partitionIndex = data.consumeInt(0, 3)

    resetHarness()
    stubClientThrottle(throttleMs)

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(topicPartition -> Errors.NONE))

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val throttleMs = data.consumeInt(0, 200)
    val logDirectoryPath = pathSafe(data, "/fuzz/arl-fwd")
    val topicName = topicSafe(data, "fuzz-arl-fwd")
    val partitionIndex = data.consumeInt(0, 3)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val alterReplicaLogDir = new AlterReplicaLogDir().setPath(logDirectoryPath)
    alterReplicaLogDir.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(topicPartition -> Errors.NONE))

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAlterReplicaLogDirsDuplicateTopicPartitionLastDirWins(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.ALTER_REPLICA_LOG_DIRS.oldestVersion(),
      ApiKeys.ALTER_REPLICA_LOG_DIRS.latestVersion())
    val logDirectoryPathFirst = pathSafe(data, "/fuzz/arl-dup-a")
    val logDirectoryPathSecond = pathSafe(data, "/fuzz/arl-dup-b")
    val topicName = topicSafe(data, "fuzz-arl-dup-topic")
    val partitionIndex = data.consumeInt(0, 5)
    val expectedError = pickError(data)

    resetHarness()
    stubNoThrottle()

    val alterReplicaLogDirFirst = new AlterReplicaLogDir().setPath(logDirectoryPathFirst)
    alterReplicaLogDirFirst.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex))))
    val alterReplicaLogDirSecond = new AlterReplicaLogDir().setPath(logDirectoryPathSecond)
    alterReplicaLogDirSecond.topics().add(new AlterReplicaLogDirTopic().setName(topicName).setPartitions(util.Arrays.asList(Integer.valueOf(partitionIndex))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(alterReplicaLogDirFirst)
    reqData.dirs().add(alterReplicaLogDirSecond)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val topicPartition = new TopicPartition(topicName, partitionIndex)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(topicPartition -> expectedError))

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }
}
