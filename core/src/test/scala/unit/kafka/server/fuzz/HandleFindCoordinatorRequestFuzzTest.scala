package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import kafka.server.metadata.ZkMetadataCache
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.FindCoordinatorRequest
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import scala.collection.Seq
import scala.jdk.CollectionConverters._
/**
 * Jazzer fuzz tests targeting `KafkaApis.handleFindCoordinatorRequest`.
 *
 * The target is deliberately split by request-version family because
 * `handleFindCoordinatorRequest` dispatches to separate implementations for
 * v0-v3 and v4+ before entering the shared `getCoordinator` helper.
 */
class HandleFindCoordinatorRequestFuzzTest extends KafkaApisTest {
  private def safeKey(data: FuzzedDataProvider, prefix: String): String = {
    val key = data.consumeString(64)
    if (key.isEmpty) prefix else key
  }
  private def buildFindCoordinatorRequest(version: Short,
                                          keyType: Byte,
                                          keys: Seq[String]): RequestChannel.Request = {
    val requestData = new FindCoordinatorRequestData().setKeyType(keyType)
    if (version >= FindCoordinatorRequest.MIN_BATCHED_VERSION)
      requestData.setCoordinatorKeys(keys.asJava)
    else
      requestData.setKey(keys.headOption.getOrElse("fuzz-coordinator"))
    buildRequest(new FindCoordinatorRequest.Builder(requestData).build(version))
  }
  private def allowOrDenyAuthorizer(result: AuthorizationResult): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]]()))
      .thenAnswer(invocation => {
        val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
        Collections.nCopies(actions.size, result)
      })
    authorizer
  }
  private def stubNoThrottle(): Unit = {
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }
  private def metadataTopic(name: String,
                            error: Errors,
                            partition: Int,
                            leaderId: Int): MetadataResponseTopic = {
    val partitions =
      if (error == Errors.NONE) {
        Collections.singletonList(new MetadataResponsePartition()
          .setPartitionIndex(partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderId(leaderId)
          .setLeaderEpoch(1)
          .setReplicaNodes(Collections.singletonList(Integer.valueOf(0)))
          .setIsrNodes(Collections.singletonList(Integer.valueOf(0)))
          .setOfflineReplicas(Collections.emptyList[Integer]()))
      } else {
        Collections.emptyList[MetadataResponsePartition]()
      }
    new MetadataResponseTopic()
      .setName(name)
      .setIsInternal(true)
      .setErrorCode(error.code)
      .setPartitions(partitions)
  }
  private def stubMetadata(topicName: String,
                           partition: Int,
                           responseError: Errors,
                           leaderId: Int,
                           endpoint: Option[Node]): Unit = {
    val cache = mock(classOf[ZkMetadataCache])
    metadataCache = cache
    when(cache.getTopicMetadata(any[Set[String]], any[ListenerName], any[Boolean], any[Boolean]))
      .thenReturn(Seq(metadataTopic(topicName, responseError, partition, leaderId)))
    when(cache.getAliveBrokerNode(any[Int], any[ListenerName])).thenReturn(endpoint)
  }
  /**
   * Drives the v0-v3 handler and both response paths:
   *   - successful coordinator lookup uses `sendResponseMaybeThrottle`
   *   - any lookup error uses `sendErrorResponseMaybeThrottle`
   *
   * The modes cover group, transaction, share-version rejection, missing
   * internal-topic metadata, metadata-error, missing-leader, missing-endpoint,
   * and live-endpoint coordinator lookups.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestFindCoordinatorBeforeV4(data: FuzzedDataProvider): Unit = {
    val mode = data.consumeInt(0, 7)
    val version1 = data.consumeShort(1, 3)
    val version2 = data.consumeShort(0, 3)
    val coordType = data.consumeBoolean()
    val version: Short = mode match {
      case 1 | 2 => version1
      case _ => version2
    }
    val key = safeKey(data, "fuzz-old-coordinator")
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, autoTopicCreationManager)
    stubNoThrottle()
    when(groupCoordinator.partitionFor(anyString())).thenReturn(0)
    when(txnCoordinator.partitionFor(anyString())).thenReturn(0)
    val (keyType, authorizer) = mode match {
      case 0 =>
        CoordinatorType.GROUP.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED))
      case 1 =>
        CoordinatorType.TRANSACTION.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED))
      case 2 =>
        CoordinatorType.SHARE.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED))
      case _ =>
        val coordinatorType =
          if (coordType) CoordinatorType.GROUP else CoordinatorType.TRANSACTION
        coordinatorType.id -> None
    }
    mode match {
      case 3 =>
      // No metadata for the internal topic: auto-topic creation branch.
      case 4 =>
        stubMetadata(Topic.GROUP_METADATA_TOPIC_NAME, 0, Errors.LEADER_NOT_AVAILABLE, 0, None)
      case 5 =>
        stubMetadata(Topic.GROUP_METADATA_TOPIC_NAME, 0, Errors.NONE,
          org.apache.kafka.common.requests.MetadataResponse.NO_LEADER_ID, None)
      case 6 =>
        stubMetadata(Topic.GROUP_METADATA_TOPIC_NAME, 0, Errors.NONE, 0, None)
      case 7 =>
        addTopicToMetadataCache(Topic.GROUP_METADATA_TOPIC_NAME, numPartitions = 1, numBrokers = 1)
      case _ =>
    }
    val request = buildFindCoordinatorRequest(version, keyType, Seq(key))
    val kafkaApis = createKafkaApis(authorizer = authorizer)
    try kafkaApis.handleFindCoordinatorRequest(request)
    finally kafkaApis.close()
  }
  /**
   * Drives the v4+ batched handler. Every coordinator key is folded into the
   * response, so this varies batch size and lookup mode to cover all reachable
   * `getCoordinator` exits while staying in the v4+ response path.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestFindCoordinatorV4AndAbove(data: FuzzedDataProvider): Unit = {
    val mode = data.consumeInt(0, 8)
    val version: Short =
      if (mode == 3) data.consumeShort(4, 5)
      else data.consumeShort(4, ApiKeys.FIND_COORDINATOR.latestVersion)
    val numKeys = data.consumeInt(1, 4)
    val coordType = data.consumeBoolean()
    val keys = (0 until numKeys).map(i => safeKey(data, s"fuzz-new-coordinator-$i"))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, autoTopicCreationManager)
    stubNoThrottle()
    when(groupCoordinator.partitionFor(anyString())).thenReturn(0)
    when(txnCoordinator.partitionFor(anyString())).thenReturn(0)

    val (keyType, authorizer) = mode match {
      case 0 =>
        CoordinatorType.GROUP.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED))
      case 1 =>
        CoordinatorType.TRANSACTION.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED))
      case 2 =>
        CoordinatorType.SHARE.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED))
      case 3 =>
        CoordinatorType.SHARE.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED))
      case 4 =>
        CoordinatorType.SHARE.id -> Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED))
      case _ =>
        val coordinatorType =
          if (coordType) CoordinatorType.GROUP else CoordinatorType.TRANSACTION
        coordinatorType.id -> None
    }
    mode match {
      case 5 =>
      // No metadata for the internal topic: auto-topic creation branch.
      case 6 =>
        stubMetadata(Topic.TRANSACTION_STATE_TOPIC_NAME, 0, Errors.LEADER_NOT_AVAILABLE, 0, None)
      case 7 =>
        stubMetadata(Topic.TRANSACTION_STATE_TOPIC_NAME, 0, Errors.NONE, 0, None)
      case 8 =>
        addTopicToMetadataCache(Topic.TRANSACTION_STATE_TOPIC_NAME, numPartitions = 1, numBrokers = 1)
      case _ =>
    }
    val request = buildFindCoordinatorRequest(version, keyType, keys)
    val kafkaApis = createKafkaApis(authorizer = authorizer)
    try kafkaApis.handleFindCoordinatorRequest(request)
    finally kafkaApis.close()
  }
}