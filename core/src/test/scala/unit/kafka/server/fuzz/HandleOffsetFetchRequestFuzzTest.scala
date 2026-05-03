package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.OffsetFetchRequestData
import org.apache.kafka.common.message.OffsetFetchRequestData.{OffsetFetchRequestGroup, OffsetFetchRequestTopics}
import org.apache.kafka.common.message.OffsetFetchResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{OffsetFetchRequest, RequestContext}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleOffsetFetchRequest`.
 *
 * Version 0 is routed to `handleOffsetFetchRequestFromZookeeper` (ZK-backed
 * offset reads with group and topic ACL checks). Versions 1+ use
 * `handleOffsetFetchRequestFromCoordinator` (group coordinator futures,
 * `sendMaybeThrottle` on completion, and topic authorization filtering on
 * coordinator responses).
 */
class HandleOffsetFetchRequestFuzzTest extends KafkaApisTest {

  private def helperSplitByteArray(byteArray: Array[Byte], splitSize: Int): (String, Array[Byte]) = {
    require(splitSize >= 0, "Split size must be non-negative")
    val (stringBytes, remainingBytes) = byteArray.splitAt(splitSize)
    (new String(stringBytes), remainingBytes)
  }

  /**
   * Offset fetch v0: ZK path (`handleOffsetFetchRequestFromZookeeper`).
   * Rotates through group deny, partial topic deny, missing topic in cache,
   * ZK hit / miss, ZK exception, and throttling on `sendResponseMaybeThrottle`.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestOffsetFetchZk(data: FuzzedDataProvider): Unit = {
    val mode = data.consumeInt(0, 5)
    val throttleMs = data.consumeInt(0, 100)
    val splitSize = data.consumeInt(1, 64)
    val (rawGroup, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val groupId = if (rawGroup.isEmpty) "fuzz-zk-group" else rawGroup

    val topicA = s"fuzz-of-a-$groupId.hashCode".replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
    val topicB = s"fuzz-of-b-$groupId.hashCode".replaceAll("[^a-zA-Z0-9._-]", "_").take(200)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val tps = List(new TopicPartition(topicA, 0), new TopicPartition(topicB, 1))
    val req = new OffsetFetchRequest.Builder(groupId, false, tps.asJava, false).build(0.toShort)
    val request = buildRequest(req)

    mode match {
      case 4 =>
        val authorizer = mock(classOf[Authorizer])
        when(authorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { inv =>
          val actions = inv.getArgument(1, classOf[util.List[Action]])
          val out = new util.ArrayList[AuthorizationResult](actions.size)
          actions.forEach { a =>
            val denied = a.resourcePattern.resourceType == ResourceType.GROUP &&
              a.operation == AclOperation.DESCRIBE &&
              a.resourcePattern.name == groupId
            out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
          }
          out
        }
        val k = createKafkaApis(authorizer = Some(authorizer))
        try k.handleOffsetFetchRequest(request) finally k.close()

      case 5 =>
        addTopicToMetadataCache(topicA, numPartitions = 2, numBrokers = 2)
        addTopicToMetadataCache(topicB, numPartitions = 2, numBrokers = 2)
        val authorizer = mock(classOf[Authorizer])
        when(authorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { inv =>
          val actions = inv.getArgument(1, classOf[util.List[Action]])
          val out = new util.ArrayList[AuthorizationResult](actions.size)
          actions.forEach { a =>
            val denied = a.resourcePattern.resourceType == ResourceType.TOPIC &&
              a.operation == AclOperation.DESCRIBE &&
              a.resourcePattern.name == topicB
            out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
          }
          out
        }
        when(zkClient.getConsumerOffset(groupId, tps.head)).thenReturn(Some(42L))
        when(zkClient.getConsumerOffset(groupId, tps(1))).thenReturn(None)
        val k = createKafkaApis(authorizer = Some(authorizer))
        try k.handleOffsetFetchRequest(request) finally k.close()

      case 0 =>
        when(zkClient.getConsumerOffset(any(), any())).thenReturn(Some(7L))
        val k = createKafkaApis()
        try k.handleOffsetFetchRequest(request) finally k.close()

      case 1 =>
        addTopicToMetadataCache(topicA, numPartitions = 2, numBrokers = 2)
        addTopicToMetadataCache(topicB, numPartitions = 2, numBrokers = 2)
        when(zkClient.getConsumerOffset(any(), any())).thenReturn(None)
        val k = createKafkaApis()
        try k.handleOffsetFetchRequest(request) finally k.close()

      case 2 =>
        addTopicToMetadataCache(topicA, numPartitions = 2, numBrokers = 2)
        addTopicToMetadataCache(topicB, numPartitions = 2, numBrokers = 2)
        when(zkClient.getConsumerOffset(any(), any())).thenThrow(new RuntimeException("fuzz-zk"))
        val k = createKafkaApis()
        try k.handleOffsetFetchRequest(request) finally k.close()

      case 3 =>
        addTopicToMetadataCache(topicA, numPartitions = 2, numBrokers = 2)
        addTopicToMetadataCache(topicB, numPartitions = 2, numBrokers = 2)
        when(zkClient.getConsumerOffset(groupId, tps.head)).thenReturn(Some(99L))
        when(zkClient.getConsumerOffset(groupId, tps(1))).thenReturn(Some(100L))
        val k = createKafkaApis()
        try k.handleOffsetFetchRequest(request) finally k.close()
    }
  }

  /**
   * Coordinator path for v8+ with multiple groups (batched offset fetch).
   * Mixes `fetchOffsets`, `fetchAllOffsets`, success, error code, and
   * exceptional completion so `fetchAllOffsetsForGroup` / `fetchOffsetsForGroup`
   * handle branches run.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestOffsetFetchCoordinatorMultiGroup(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(8, ApiKeys.OFFSET_FETCH.latestVersion).toShort
    val numGroups = data.consumeInt(1, 4)
    val splitSize = data.consumeInt(1, 32)
    val (prefix, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val p = if (prefix.isEmpty) "g" else prefix.replaceAll("[^a-zA-Z0-9._-]", "_").take(32)

    val requireStable = data.consumeBoolean()

    case class Spec(gid: String, branch: Int, partitionList: util.List[TopicPartition])

    val specs = (0 until numGroups).map { i =>
      val gid = s"$p-$i"
      val branch = data.consumeInt(0, 3)
      val partitionList: util.List[TopicPartition] = branch match {
        case 0 =>
          util.Arrays.asList(
            new TopicPartition(s"$p-t-$i", 0),
            new TopicPartition(s"$p-t-$i", 1))
        case 1 => null
        case 2 =>
          util.Arrays.asList(
            new TopicPartition(s"$p-secret-$i", 0),
            new TopicPartition(s"$p-open-$i", 1))
        case _ =>
          util.Collections.singletonList(new TopicPartition(s"$p-one-$i", 0))
      }
      Spec(gid, branch, partitionList)
    }

    val jmap = new util.HashMap[String, util.List[TopicPartition]]()
    specs.foreach(s => jmap.put(s.gid, s.partitionList))

    val req = new OffsetFetchRequest.Builder(jmap, requireStable, false).build(version)
    val request = buildRequest(req)

    def expectedRequestGroup(s: Spec): OffsetFetchRequestGroup = {
      val g = new OffsetFetchRequestGroup().setGroupId(s.gid)
      if (s.partitionList == null) g.setTopics(null)
      else {
        val byTopic = s.partitionList.asScala.groupBy(_.topic)
        val topics = byTopic.map { case (name, tps) =>
          new OffsetFetchRequestTopics()
            .setName(name)
            .setPartitionIndexes(tps.map(_.partition: Integer).asJava)
        }.toList.asJava
        g.setTopics(topics)
      }
      g
    }

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](), anyLong)).thenReturn(0)

    val fetchOffsetsFutures = new util.HashMap[String, CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]]()
    val fetchAllFutures = new util.HashMap[String, CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]]()

    specs.foreach { s =>
      if (s.partitionList == null) {
        val fut = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
        fetchAllFutures.put(s.gid, fut)
        s.branch match {
          case 1 =>
            if (data.consumeBoolean())
              fut.completeExceptionally(Errors.INVALID_GROUP_ID.exception)
            else
              fut.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(s.gid)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code))
          case _ =>
            fut.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(s.gid)
              .setTopics(List(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(s"$p-all-${s.gid}")
                  .setPartitions(List(
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(0)
                      .setCommittedOffset(11L)
                      .setCommittedLeaderEpoch(1)
                  ).asJava)
              ).asJava))
        }
      } else {
        val fut = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
        fetchOffsetsFutures.put(s.gid, fut)
        val g = expectedRequestGroup(s)
        s.branch match {
          case 2 =>
            if (data.consumeBoolean())
              fut.completeExceptionally(Errors.NOT_COORDINATOR.exception)
            else {
              val topics = g.topics.asScala.map { t =>
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                  .setName(t.name)
                  .setPartitions(t.partitionIndexes.asScala.map { pi =>
                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                      .setPartitionIndex(pi)
                      .setCommittedOffset(22L + pi)
                      .setCommittedLeaderEpoch(2)
                  }.asJava)
              }.asJava
              fut.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(s.gid)
                .setTopics(topics))
            }
          case _ =>
            val topics = g.topics.asScala.map { t =>
              new OffsetFetchResponseData.OffsetFetchResponseTopics()
                .setName(t.name)
                .setPartitions(t.partitionIndexes.asScala.map { pi =>
                  new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                    .setPartitionIndex(pi)
                    .setCommittedOffset(33L + pi)
                    .setCommittedLeaderEpoch(3)
                }.asJava)
            }.asJava
            fut.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
              .setGroupId(s.gid)
              .setTopics(topics))
        }
      }
    }

    when(groupCoordinator.fetchOffsets(any(), any(), anyBoolean())).thenAnswer { inv =>
      val g = inv.getArgument(1, classOf[OffsetFetchRequestGroup])
      fetchOffsetsFutures.get(g.groupId)
    }
    when(groupCoordinator.fetchAllOffsets(any(), any(), anyBoolean())).thenAnswer { inv =>
      val g = inv.getArgument(1, classOf[OffsetFetchRequestGroup])
      fetchAllFutures.get(g.groupId)
    }

    val topicAuthorizer = mock(classOf[Authorizer])
    when(topicAuthorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { inv =>
      val actions = inv.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { a =>
        val name = a.resourcePattern.name
        val denied = a.resourcePattern.resourceType == ResourceType.TOPIC &&
          a.operation == AclOperation.DESCRIBE &&
          name != null && name.contains("secret")
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    }

    val k = createKafkaApis(authorizer = Some(topicAuthorizer))
    try k.handleOffsetFetchRequest(request) finally k.close()
  }

  /**
   * Coordinator path for single-group protocol versions 1&ndash;7 (no batching).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestOffsetFetchCoordinatorV1To7(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(1, 7).toShort
    val requireStable = data.consumeBoolean()
    val allPartitions = version >= 2 && data.consumeBoolean()
    val splitSize = data.consumeInt(1, 32)
    val (rawGroup, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val groupId = if (rawGroup.isEmpty) "fuzz-c-group" else rawGroup.take(200)

    val partitions: util.List[TopicPartition] =
      if (allPartitions) null
      else List(new TopicPartition("fuzz-coord-topic", 0), new TopicPartition("fuzz-coord-topic", 1)).asJava

    val req = new OffsetFetchRequest.Builder(groupId, requireStable, partitions, false).build(version)
    val request = buildRequest(req)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](), anyLong)).thenReturn(0)

    val expectedGroup = new OffsetFetchRequestData.OffsetFetchRequestGroup()
      .setGroupId(groupId)
    if (allPartitions)
      expectedGroup.setTopics(null)
    else
      expectedGroup.setTopics(List(
        new OffsetFetchRequestTopics()
          .setName("fuzz-coord-topic")
          .setPartitionIndexes(List[Integer](0, 1).asJava)
      ).asJava)

    val fut = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    if (allPartitions)
      when(groupCoordinator.fetchAllOffsets(request.context, expectedGroup, requireStable)).thenReturn(fut)
    else
      when(groupCoordinator.fetchOffsets(request.context, expectedGroup, requireStable)).thenReturn(fut)

    if (data.consumeBoolean())
      fut.completeExceptionally(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception)
    else
      fut.complete(new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId(groupId)
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("fuzz-coord-topic")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(1L)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(2L)
                .setCommittedLeaderEpoch(2)
            ).asJava)
        ).asJava))

    val k = createKafkaApis()
    try k.handleOffsetFetchRequest(request) finally k.close()
  }

  /**
   * `sendMaybeThrottle` on the coordinator completion path with non-zero
   * request quota and a forwarded request (skips local `throttle()`).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestOffsetFetchCoordinatorThrottleAndForwarded(data: FuzzedDataProvider): Unit = {
    val useForwarded = data.consumeBoolean()
    val throttleMs = data.consumeInt(0, 100)
    val version = data.consumeInt(3, ApiKeys.OFFSET_FETCH.latestVersion).toShort
    val splitSize = data.consumeInt(1, 24)
    val (gidRaw, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val groupId = if (gidRaw.isEmpty) "fuzz-throttle-g" else gidRaw.take(120)

    val partitions = util.Collections.singletonList(new TopicPartition("fuzz-throttle-tp", 0))
    val req = new OffsetFetchRequest.Builder(groupId, false, partitions, false).build(version)
    val request =
      if (useForwarded) buildForwardedRequest(req)
      else buildRequest(req)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val expectedGroup = new OffsetFetchRequestGroup()
      .setGroupId(groupId)
      .setTopics(List(
        new OffsetFetchRequestTopics()
          .setName("fuzz-throttle-tp")
          .setPartitionIndexes(List[Integer](0).asJava)
      ).asJava)

    val fut = CompletableFuture.completedFuture(
      new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId(groupId)
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("fuzz-throttle-tp")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(5L)
                .setCommittedLeaderEpoch(1)
            ).asJava)
        ).asJava)
    )
    when(groupCoordinator.fetchOffsets(request.context, expectedGroup, false)).thenReturn(fut)

    val k = createKafkaApis()
    try k.handleOffsetFetchRequest(request) finally k.close()
  }
}
