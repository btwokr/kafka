package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{HeartbeatRequest, RequestContext}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_2_IV1, IBP_2_3_IV0}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleHeartbeatRequest`.
 *
 * The method has three arms:
 *   - static membership on an inter-broker protocol older than 2.3
 *     (`UNSUPPORTED_VERSION` via `getErrorResponse`),
 *   - `GROUP_AUTHORIZATION_FAILED` when READ on the group is denied,
 *   - otherwise `groupCoordinator.heartbeat` with success or exception
 *     paths in the completion handler.
 */
class HandleHeartbeatRequestFuzzTest extends KafkaApisTest {

  private def resetZkMetadataToLatestTesting(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
  }

  /** `HeartbeatRequest.Builder` rejects `groupInstanceId` on protocol versions below 3. */
  private def heartbeatVersionAllowingInstanceId(version: Short, groupInstanceId: String): Short =
    if (groupInstanceId != null && version < 3)
      3
    else
      version

  private def stubNoThrottling(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def splitBytes(data: FuzzedDataProvider, splitSize: Int): (String, Array[Byte]) = {
    val bytes = data.consumeRemainingAsBytes()
    val (prefix, rest) = bytes.splitAt(splitSize)
    (new String(prefix), rest)
  }

  /**
   * Varies API version, group / member / instance id strings, generation id,
   * and whether the coordinator future completes normally or exceptionally.
   * Uses current metadata version so static membership is allowed when
   * `groupInstanceId` is non-null.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestHeartbeatCoordinatorPath(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val splitSize = data.consumeInt(0, 128)
    val (groupIdPrefix, tail) = splitBytes(data, splitSize)
    val (memberPrefix, tail2) = {
      val s = data.consumeInt(0, math.min(64, tail.length))
      val (a, b) = tail.splitAt(s)
      (new String(a), b)
    }
    val instanceLen = data.consumeInt(0, math.min(64, tail2.length))
    val instanceId =
      if (instanceLen == 0) null
      else new String(tail2.take(instanceLen))
    val generationId = data.consumeInt()
    val completeWithError = data.consumeBoolean()

    val groupId = if (groupIdPrefix.isEmpty) "fuzz-group" else groupIdPrefix
    val memberId = if (memberPrefix.isEmpty) "fuzz-member" else memberPrefix

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGroupInstanceId(instanceId)
      .setGenerationId(generationId)

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(
      heartbeatVersionAllowingInstanceId(version, instanceId))
    val request = buildRequest(heartbeatReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val future = new CompletableFuture[org.apache.kafka.common.message.HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleHeartbeatRequest(request)
      if (completeWithError)
        future.completeExceptionally(new IllegalStateException("fuzz-coordinator-error"))
      else
        future.complete(new org.apache.kafka.common.message.HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * `groupInstanceId` set on IBP &lt; 2.3 triggers the early
   * `UNSUPPORTED_VERSION` response (no coordinator call).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestHeartbeatStaticMembershipOldIbp(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val splitSize = data.consumeInt(0, 64)
    val (gid, rest) = splitBytes(data, splitSize)
    val mid = if (rest.isEmpty) "m" else new String(rest)
    val instanceId = if (gid.isEmpty) "fuzz-static-id" else s"$gid-instance"

    val requestData = new HeartbeatRequestData()
      .setGroupId(if (gid.isEmpty) "g" else gid)
      .setMemberId(mid)
      .setGroupInstanceId(instanceId)
      .setGenerationId(data.consumeInt())

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(
      heartbeatVersionAllowingInstanceId(version, instanceId))
    val request = buildRequest(heartbeatReq)

    metadataCache = MetadataCache.zkMetadataCache(brokerId, IBP_2_2_IV1)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val kafkaApis = createKafkaApis(IBP_2_2_IV1)
    try kafkaApis.handleHeartbeatRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Authorized path on IBP &ge; 2.3 with non-null `groupInstanceId` exercises
   * the branch where static membership is supported (coordinator invoked).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestHeartbeatStaticMembershipSupportedIbp(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val splitSize = data.consumeInt(0, 64)
    val (gid, rest) = splitBytes(data, splitSize)
    val instanceId = if (rest.isEmpty) "static" else new String(rest)

    val requestData = new HeartbeatRequestData()
      .setGroupId(if (gid.isEmpty) "fuzz-g" else gid)
      .setMemberId("member")
      .setGroupInstanceId(instanceId)
      .setGenerationId(data.consumeInt(0, Int.MaxValue))

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(
      heartbeatVersionAllowingInstanceId(version, instanceId))
    val request = buildRequest(heartbeatReq)

    metadataCache = MetadataCache.zkMetadataCache(brokerId, IBP_2_3_IV0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val future = new CompletableFuture[org.apache.kafka.common.message.HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis(IBP_2_3_IV0)
    try {
      kafkaApis.handleHeartbeatRequest(request)
      future.complete(new org.apache.kafka.common.message.HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Denies READ on the group; expects `GROUP_AUTHORIZATION_FAILED` without
   * calling the coordinator.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestHeartbeatAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val splitSize = data.consumeInt(0, 64)
    val (gid, rest) = splitBytes(data, splitSize)
    val memberId = if (rest.isEmpty) "m" else new String(rest)

    val requestData = new HeartbeatRequestData()
      .setGroupId(if (gid.isEmpty) "fuzz-auth-group" else gid)
      .setMemberId(memberId)
      .setGroupInstanceId(null)
      .setGenerationId(data.consumeInt())

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(version)
    val request = buildRequest(heartbeatReq)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]])).thenAnswer { _ =>
      Seq(AuthorizationResult.DENIED).asJava
    }

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    try kafkaApis.handleHeartbeatRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Request-quota throttling on the success path (`sendMaybeThrottle` with
   * non-zero throttle time).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestHeartbeatThrottled(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val splitSize = data.consumeInt(0, 64)
    val (gid, _) = splitBytes(data, splitSize)

    val requestData = new HeartbeatRequestData()
      .setGroupId(if (gid.isEmpty) "fuzz-throttle-group" else gid)
      .setMemberId("m")
      .setGenerationId(0)

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(version)
    val request = buildRequest(heartbeatReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val future = new CompletableFuture[org.apache.kafka.common.message.HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleHeartbeatRequest(request)
      future.complete(new org.apache.kafka.common.message.HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }
}
