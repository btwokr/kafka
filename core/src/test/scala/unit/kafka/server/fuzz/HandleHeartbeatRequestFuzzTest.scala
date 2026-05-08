package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.message.HeartbeatRequestData
import org.apache.kafka.common.message.HeartbeatResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{HeartbeatRequest, RequestContext}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_2_IV1, IBP_2_3_IV0}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture

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

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(128)
    if (value == null || value.isEmpty) fallback else value
  }

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

  /**
   * Varies API version, group / member / instance id strings, generation id,
   * and whether the coordinator future completes normally or exceptionally.
   * Uses current metadata version so static membership is allowed when
   * `groupInstanceId` is non-null.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestHeartbeatCoordinatorPath(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-group")
    val memberId = safeString(data, "fuzz-member")
    val useInstanceId = data.consumeBoolean()
    val groupInstanceId = if (useInstanceId) safeString(data, "fuzz-instance") else null
    val generationId = data.consumeInt()
    val completeWithError = data.consumeBoolean()

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGroupInstanceId(groupInstanceId)
      .setGenerationId(generationId)

    val builtVersion = heartbeatVersionAllowingInstanceId(version, groupInstanceId)
    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(heartbeatReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleHeartbeatRequest(request)
      if (completeWithError)
        future.completeExceptionally(new IllegalStateException("fuzz-coordinator-error"))
      else
        future.complete(new HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * `groupInstanceId` set on IBP &lt; 2.3 triggers the early
   * `UNSUPPORTED_VERSION` response (no coordinator call).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestHeartbeatStaticMembershipOldIbp(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-g")
    val memberId = safeString(data, "fuzz-m")
    val groupInstanceId = safeString(data, "fuzz-static-id")
    val generationId = data.consumeInt()

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGroupInstanceId(groupInstanceId)
      .setGenerationId(generationId)

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(
      heartbeatVersionAllowingInstanceId(version, groupInstanceId))
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
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestHeartbeatStaticMembershipSupportedIbp(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-g")
    val groupInstanceId = safeString(data, "fuzz-static-inst")
    val generationId = data.consumeInt(0, Int.MaxValue)

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId("member")
      .setGroupInstanceId(groupInstanceId)
      .setGenerationId(generationId)

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(
      heartbeatVersionAllowingInstanceId(version, groupInstanceId))
    val request = buildRequest(heartbeatReq)

    metadataCache = MetadataCache.zkMetadataCache(brokerId, IBP_2_3_IV0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottling()

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis(IBP_2_3_IV0)
    try {
      kafkaApis.handleHeartbeatRequest(request)
      future.complete(new HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Denies READ on the group; expects `GROUP_AUTHORIZATION_FAILED` without
   * calling the coordinator.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestHeartbeatAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-auth-group")
    val memberId = safeString(data, "fuzz-auth-member")
    val generationId = data.consumeInt()

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGroupInstanceId(null)
      .setGenerationId(generationId)

    val heartbeatReq = new HeartbeatRequest.Builder(requestData).build(version)
    val request = buildRequest(heartbeatReq)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.DENIED))

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
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestHeartbeatThrottled(data: FuzzedDataProvider): Unit = {
    resetZkMetadataToLatestTesting()
    val version = data.consumeInt(
      ApiKeys.HEARTBEAT.oldestVersion().toInt,
      ApiKeys.HEARTBEAT.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val groupId = safeString(data, "fuzz-throttle-group")

    val requestData = new HeartbeatRequestData()
      .setGroupId(groupId)
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

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(any[RequestContext], any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleHeartbeatRequest(request)
      future.complete(new HeartbeatResponseData())
    } finally {
      kafkaApis.close()
    }
  }
}
