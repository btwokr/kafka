package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, RequestLocal}
import org.apache.kafka.common.message.JoinGroupRequestData.{JoinGroupRequestProtocol, JoinGroupRequestProtocolCollection}
import org.apache.kafka.common.message.{JoinGroupRequestData, JoinGroupResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleJoinGroupRequest`.
 *
 * The target has three main exits before response serialization:
 *   - static membership rejected when broker metadata version is too old
 *   - group READ authorization rejected
 *   - authorized request delegated to GroupCoordinator, whose future may
 *     complete with either a response or an exception.
 */
class HandleJoinGroupRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
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

  private def buildJoinGroupRequest(
    version: Short,
    groupInstanceId: Option[String],
    protocolName: String,
    protocolBytes: Array[Byte],
    groupId: String,
    memberId: String,
    protocolType: String,
    sessionTimeoutMs: Int,
    rebalanceTimeoutMs: Int,
    reason: String
  ): JoinGroupRequest = {
    val protocols = new JoinGroupRequestProtocolCollection()
    protocols.add(new JoinGroupRequestProtocol()
      .setName(protocolName)
      .setMetadata(protocolBytes))
    val requestData = new JoinGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setProtocolType(protocolType)
      .setSessionTimeoutMs(sessionTimeoutMs)
      .setRebalanceTimeoutMs(rebalanceTimeoutMs)
      .setProtocols(protocols)
    groupInstanceId.foreach(requestData.setGroupInstanceId)
    if (version >= 8)
      requestData.setReason(reason)
    new JoinGroupRequest.Builder(requestData).build(version)
  }

  private def joinSuccessResponse(
    memberId: String,
    generationId: Int,
    leader: String,
    protocolType: String,
    version: Short,
    includeProtocolName: Boolean,
    protocolName: String
  ): JoinGroupResponseData = {
    new JoinGroupResponseData()
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setLeader(leader)
      .setProtocolType(if (version >= 7) protocolType else null)
      .setProtocolName(if (includeProtocolName) protocolName else null)
  }

  /**
   * Drives early error paths:
   *   - static membership + old IBP => UNSUPPORTED_VERSION
   *   - authorization denial => GROUP_AUTHORIZATION_FAILED
   *
   * Both routes use `sendMaybeThrottle` without invoking GroupCoordinator.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestJoinGroupEarlyErrors(data: FuzzedDataProvider): Unit = {
    val staticMembershipUnsupported = data.consumeBoolean()
    val version: Short =
      if (staticMembershipUnsupported) data.consumeShort(5, ApiKeys.JOIN_GROUP.latestVersion)
      else data.consumeShort(ApiKeys.JOIN_GROUP.oldestVersion, ApiKeys.JOIN_GROUP.latestVersion)
    val groupInstanceStr = safeString(data, "group-instance")
    val groupInstanceId =
      if (staticMembershipUnsupported) Some(groupInstanceStr) else None
    val protocolName = safeString(data, "range")
    val protocolBytesLen = data.consumeInt(0, 128)
    val protocolBytes = data.consumeBytes(protocolBytesLen)
    val groupId = safeString(data, "fuzz-group")
    val memberId = safeString(data, JoinGroupRequest.UNKNOWN_MEMBER_ID)
    val protocolType = safeString(data, "consumer")
    val sessionTimeoutMs = data.consumeInt(1, 60_000)
    val rebalanceTimeoutMs = data.consumeInt(1, 120_000)
    val reason = safeString(data, "fuzz join")

    val request = buildRequest(buildJoinGroupRequest(
      version, groupInstanceId, protocolName, protocolBytes, groupId, memberId,
      protocolType, sessionTimeoutMs, rebalanceTimeoutMs, reason))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()
    val kafkaApis =
      if (staticMembershipUnsupported)
        createKafkaApis(interBrokerProtocolVersion = MetadataVersion.IBP_2_2_IV1)
      else
        createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED)))
    try kafkaApis.handleJoinGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }

  /**
   * Drives the authorized coordinator path and varies future completion:
   *   - successful JoinGroupResponseData
   *   - coordinator future completes exceptionally
   *
   * The request version range also covers the v0 rebalance-timeout rewrite in
   * JoinGroupRequest plus v7+ protocol-type response serialization.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestJoinGroupCoordinatorFuture(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.JOIN_GROUP.oldestVersion,
      ApiKeys.JOIN_GROUP.latestVersion)
    val useGroupInstance = version >= 5 && data.consumeBoolean()
    val groupInstanceStr = safeString(data, "group-instance")
    val groupInstanceId = if (useGroupInstance) Some(groupInstanceStr) else None
    val protocolName = safeString(data, "range")
    val protocolBytesLen = data.consumeInt(0, 128)
    val protocolBytes = data.consumeBytes(protocolBytesLen)
    val groupId = safeString(data, "fuzz-group")
    val memberId = safeString(data, JoinGroupRequest.UNKNOWN_MEMBER_ID)
    val protocolType = safeString(data, "consumer")
    val sessionTimeoutMs = data.consumeInt(1, 60_000)
    val rebalanceTimeoutMs = data.consumeInt(1, 120_000)
    val reason = safeString(data, "fuzz join")
    val completeWithException = data.consumeBoolean()
    val useRequestTimeoutError = data.consumeBoolean()
    val respMemberId = safeString(data, "member")
    val respGenerationId = data.consumeInt(0, 10)
    val respLeader = safeString(data, "leader")
    val respProtocolType = safeString(data, "consumer")
    val includeProtocolName = data.consumeBoolean()
    val respProtocolName = safeString(data, "range")

    val request = buildRequest(buildJoinGroupRequest(
      version, groupInstanceId, protocolName, protocolBytes, groupId, memberId,
      protocolType, sessionTimeoutMs, rebalanceTimeoutMs, reason))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()
    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      any(),
      any[JoinGroupRequestData],
      any()
    )).thenReturn(future)
    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try {
      val handled = kafkaApis.handleJoinGroupRequest(request, RequestLocal.NoCaching)
      if (completeWithException) {
        val error =
          if (useRequestTimeoutError) Errors.REQUEST_TIMED_OUT
          else Errors.COORDINATOR_NOT_AVAILABLE
        future.completeExceptionally(error.exception)
      } else {
        future.complete(joinSuccessResponse(
          respMemberId, respGenerationId, respLeader, respProtocolType, version,
          includeProtocolName, respProtocolName))
      }
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Same authorized coordinator path as `fuzzTestJoinGroupCoordinatorFuture`,
   * but with a positive request throttle to exercise the throttled
   * `RequestHandlerHelper.sendMaybeThrottle` branch for JoinGroup responses.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestJoinGroupThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.JOIN_GROUP.oldestVersion,
      ApiKeys.JOIN_GROUP.latestVersion)
    val protocolName = safeString(data, "range")
    val protocolBytesLen = data.consumeInt(0, 128)
    val protocolBytes = data.consumeBytes(protocolBytesLen)
    val groupId = safeString(data, "fuzz-group")
    val memberId = safeString(data, JoinGroupRequest.UNKNOWN_MEMBER_ID)
    val protocolType = safeString(data, "consumer")
    val sessionTimeoutMs = data.consumeInt(1, 60_000)
    val rebalanceTimeoutMs = data.consumeInt(1, 120_000)
    val reason = safeString(data, "fuzz join")
    val throttleMs = data.consumeInt(1, 100)
    val respMemberId = safeString(data, "member")
    val respGenerationId = data.consumeInt(0, 10)
    val respLeader = safeString(data, "leader")
    val respProtocolType = safeString(data, "consumer")
    val includeProtocolName = data.consumeBoolean()
    val respProtocolName = safeString(data, "range")

    val request = buildRequest(buildJoinGroupRequest(
      version, None, protocolName, protocolBytes, groupId, memberId,
      protocolType, sessionTimeoutMs, rebalanceTimeoutMs, reason))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    when(groupCoordinator.joinGroup(
      any(),
      any[JoinGroupRequestData],
      any()
    )).thenReturn(CompletableFuture.completedFuture(joinSuccessResponse(
      respMemberId, respGenerationId, respLeader, respProtocolType, version,
      includeProtocolName, respProtocolName)))
    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try kafkaApis.handleJoinGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }
}
