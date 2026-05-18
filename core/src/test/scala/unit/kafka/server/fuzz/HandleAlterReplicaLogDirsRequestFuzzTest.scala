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
    val s = data.consumeString(64)
    if (s == null || s.isEmpty) fallback else s
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
      actions.asScala.foreach { a =>
        val denied = a.operation == AclOperation.ALTER &&
          a.resourcePattern.resourceType == ResourceType.CLUSTER &&
          CLUSTER_NAME == a.resourcePattern.name
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
    val path = pathSafe(data, "/fuzz/arl-deny")
    val topic = topicSafe(data, "fuzz-arl-deny-topic")
    val p0 = data.consumeInt(0, 7)
    val p1 = data.consumeInt(0, 7)

    resetHarness()
    stubNoThrottle()

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(
      Integer.valueOf(p0), Integer.valueOf(p1))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
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
    val path = pathSafe(data, "/fuzz/arl-one")
    val topic = topicSafe(data, "fuzz-arl-one-topic")
    val p0 = data.consumeInt(0, 11)
    val p1 = data.consumeInt(0, 11)
    val p2 = data.consumeInt(0, 11)
    val e0 = pickError(data)
    val e1 = pickError(data)
    val e2 = pickError(data)

    resetHarness()
    stubNoThrottle()

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(
      Integer.valueOf(p0), Integer.valueOf(p1), Integer.valueOf(p2))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp0 = new TopicPartition(topic, p0)
    val tp1 = new TopicPartition(topic, p1)
    val tp2 = new TopicPartition(topic, p2)
    val expected = Map(tp0 -> e0, tp1 -> e1, tp2 -> e2)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(expected)

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
    val path = pathSafe(data, "/fuzz/arl-multi")
    val topicA = topicSafe(data, "fuzz-arl-topic-a")
    val topicB = topicSafe(data, "fuzz-arl-topic-b")
    val pa = data.consumeInt(0, 5)
    val pb = data.consumeInt(0, 5)
    val ea = pickError(data)
    val eb = pickError(data)

    resetHarness()
    stubNoThrottle()

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topicA).setPartitions(util.Arrays.asList(Integer.valueOf(pa))))
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topicB).setPartitions(util.Arrays.asList(Integer.valueOf(pb))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tpa = new TopicPartition(topicA, pa)
    val tpb = new TopicPartition(topicB, pb)
    val expected = Map(tpa -> ea, tpb -> eb)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(expected)

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
    val path1 = pathSafe(data, "/fuzz/arl-d1")
    val path2 = pathSafe(data, "/fuzz/arl-d2")
    val topic = topicSafe(data, "fuzz-arl-two-dir-topic")
    val p1 = data.consumeInt(0, 4)
    val p2 = data.consumeInt(0, 4)
    val e1 = pickError(data)
    val e2 = pickError(data)

    resetHarness()
    stubNoThrottle()

    val dir1 = new AlterReplicaLogDir().setPath(path1)
    dir1.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(p1))))
    val dir2 = new AlterReplicaLogDir().setPath(path2)
    dir2.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(p2))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir1)
    reqData.dirs().add(dir2)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp1 = new TopicPartition(topic, p1)
    val tp2 = new TopicPartition(topic, p2)
    val expected = Map(tp1 -> e1, tp2 -> e2)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap)).thenReturn(expected)

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
    val path = pathSafe(data, "/fuzz/arl-noauth")
    val topic = topicSafe(data, "fuzz-arl-noauth")
    val p0 = data.consumeInt(0, 3)

    resetHarness()
    stubNoThrottle()

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(p0))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp = new TopicPartition(topic, p0)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(tp -> Errors.NONE))

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
    val path = pathSafe(data, "/fuzz/arl-throttle")
    val topic = topicSafe(data, "fuzz-arl-throttle")
    val p0 = data.consumeInt(0, 3)

    resetHarness()
    stubClientThrottle(throttleMs)

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(p0))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp = new TopicPartition(topic, p0)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(tp -> Errors.NONE))

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
    val path = pathSafe(data, "/fuzz/arl-fwd")
    val topic = topicSafe(data, "fuzz-arl-fwd")
    val p0 = data.consumeInt(0, 3)

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val dir = new AlterReplicaLogDir().setPath(path)
    dir.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(p0))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp = new TopicPartition(topic, p0)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(tp -> Errors.NONE))

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
    val pathFirst = pathSafe(data, "/fuzz/arl-dup-a")
    val pathSecond = pathSafe(data, "/fuzz/arl-dup-b")
    val topic = topicSafe(data, "fuzz-arl-dup-topic")
    val part = data.consumeInt(0, 5)
    val err = pickError(data)

    resetHarness()
    stubNoThrottle()

    val dir1 = new AlterReplicaLogDir().setPath(pathFirst)
    dir1.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(part))))
    val dir2 = new AlterReplicaLogDir().setPath(pathSecond)
    dir2.topics().add(new AlterReplicaLogDirTopic().setName(topic).setPartitions(util.Arrays.asList(Integer.valueOf(part))))
    val reqData = new AlterReplicaLogDirsRequestData()
    reqData.dirs().add(dir1)
    reqData.dirs().add(dir2)
    val partitionDirs = new AlterReplicaLogDirsRequest(reqData, version).partitionDirs()
    val tp = new TopicPartition(topic, part)
    when(replicaManager.alterReplicaLogDirs(partitionDirs.asScala.toMap))
      .thenReturn(Map(tp -> err))

    val built = new AlterReplicaLogDirsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerAllowAll()))
    try kafkaApis.handleAlterReplicaLogDirsRequest(request)
    finally kafkaApis.close()
  }
}
