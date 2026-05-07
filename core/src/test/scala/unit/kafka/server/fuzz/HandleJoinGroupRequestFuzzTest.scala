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
    if (value.isEmpty) fallback else value
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
  private def buildJoinGroupRequest(data: FuzzedDataProvider,
                                    version: Short,
                                    groupInstanceId: Option[String]): JoinGroupRequest = {
    val protocolName = safeString(data, "range")
    val protocolBytes = data.consumeBytes(data.consumeInt(0, 128))
    val protocols = new JoinGroupRequestProtocolCollection()
    protocols.add(new JoinGroupRequestProtocol()
      .setName(protocolName)
      .setMetadata(protocolBytes))
    val requestData = new JoinGroupRequestData()
      .setGroupId(safeString(data, "fuzz-group"))
      .setMemberId(safeString(data, JoinGroupRequest.UNKNOWN_MEMBER_ID))
      .setProtocolType(safeString(data, "consumer"))
      .setSessionTimeoutMs(data.consumeInt(1, 60_000))
      .setRebalanceTimeoutMs(data.consumeInt(1, 120_000))
      .setProtocols(protocols)
    groupInstanceId.foreach(requestData.setGroupInstanceId)
    if (version >= 8)
      requestData.setReason(safeString(data, "fuzz join"))
    new JoinGroupRequest.Builder(requestData).build(version)
  }
  private def successResponse(data: FuzzedDataProvider, version: Short): JoinGroupResponseData = {
    new JoinGroupResponseData()
      .setMemberId(safeString(data, "member"))
      .setGenerationId(data.consumeInt(0, 10))
      .setLeader(safeString(data, "leader"))
      .setProtocolType(if (version >= 7) safeString(data, "consumer") else null)
      .setProtocolName(if (data.consumeBoolean()) safeString(data, "range") else null)
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
      if (staticMembershipUnsupported) data.consumeInt(5, ApiKeys.JOIN_GROUP.latestVersion).toShort
      else data.consumeInt(ApiKeys.JOIN_GROUP.oldestVersion.toInt, ApiKeys.JOIN_GROUP.latestVersion.toInt).toShort
    val groupInstanceId =
      if (staticMembershipUnsupported) Some(safeString(data, "group-instance"))
      else None
    val request = buildRequest(buildJoinGroupRequest(data, version, groupInstanceId))
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
    val version = data.consumeInt(
      ApiKeys.JOIN_GROUP.oldestVersion.toInt,
      ApiKeys.JOIN_GROUP.latestVersion.toInt).toShort
    val request = buildRequest(buildJoinGroupRequest(data, version,
      if (version >= 5 && data.consumeBoolean()) Some(safeString(data, "group-instance")) else None))
    val completeWithException = data.consumeBoolean()
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
        val error = if (data.consumeBoolean()) Errors.REQUEST_TIMED_OUT else Errors.COORDINATOR_NOT_AVAILABLE
        future.completeExceptionally(error.exception)
      } else {
        future.complete(successResponse(data, version))
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
    val version = data.consumeInt(
      ApiKeys.JOIN_GROUP.oldestVersion.toInt,
      ApiKeys.JOIN_GROUP.latestVersion.toInt).toShort
    val request = buildRequest(buildJoinGroupRequest(data, version, None))
    val throttleMs = data.consumeInt(1, 100)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    when(groupCoordinator.joinGroup(
      any(),
      any[JoinGroupRequestData],
      any()
    )).thenReturn(CompletableFuture.completedFuture(successResponse(data, version)))
    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try kafkaApis.handleJoinGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }
}