package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import org.apache.kafka.common.message.{LeaveGroupRequestData, LeaveGroupResponseData}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.{Arrays, Collections}
import java.util.concurrent.CompletableFuture

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleLeaveGroupRequest`.
 *
 * The handler authorizes READ on the group, then delegates to
 * `groupCoordinator.leaveGroup` with `LeaveGroupRequest.normalizedData()`
 * (single-member wire format for versions below 3, batch format from v3).
 */
class HandleLeaveGroupRequestFuzzTest extends KafkaApisTest {

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

  private def nullableInstance(s: String): String =
    if (s == null || s.isEmpty) null else s

  private def buildMemberIdentities(
    version: Short,
    memberId1: String,
    instance1: String,
    twoMembers: Boolean,
    memberId2: String,
    instance2: String
  ): util.List[MemberIdentity] = {
    val id1 = new MemberIdentity()
      .setMemberId(memberId1)
      .setGroupInstanceId(nullableInstance(instance1))
    if (version >= 3 && twoMembers)
      Arrays.asList(id1, new MemberIdentity()
        .setMemberId(memberId2)
        .setGroupInstanceId(nullableInstance(instance2)))
    else
      Collections.singletonList(id1)
  }

  private def successLeaveResponse(
    version: Short,
    memberId1: String,
    instance1: String,
    twoMembers: Boolean,
    memberId2: String,
    instance2: String
  ): LeaveGroupResponseData = {
    val d = new LeaveGroupResponseData().setErrorCode(Errors.NONE.code)
    if (version >= 3) {
      val out = new util.ArrayList[MemberResponse]()
      out.add(new MemberResponse()
        .setMemberId(memberId1)
        .setGroupInstanceId(nullableInstance(instance1)))
      if (twoMembers)
        out.add(new MemberResponse()
          .setMemberId(memberId2)
          .setGroupInstanceId(nullableInstance(instance2)))
      d.setMembers(out)
    } else {
      // `new LeaveGroupResponse(data, version)` for v < 3 requires exactly one
      // member row when the top-level error is NONE (see LeaveGroupResponse.java).
      d.setMembers(Collections.singletonList(
        new MemberResponse()
          .setMemberId(memberId1)
          .setErrorCode(Errors.NONE.code)
      ))
    }
    d
  }

  /**
   * Denies READ on the group; expects `GROUP_AUTHORIZATION_FAILED` without
   * calling the coordinator.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaveGroupAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LEAVE_GROUP.oldestVersion().toInt,
      ApiKeys.LEAVE_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-leave-auth-g")
    val memberId1 = safeString(data, "fuzz-leave-auth-m1")
    val memberId2 = safeString(data, "fuzz-leave-auth-m2")
    val useInst1 = data.consumeBoolean()
    val instance1 = if (useInst1) safeString(data, "fuzz-leave-auth-i1") else ""
    val useInst2 = data.consumeBoolean()
    val instance2 = if (useInst2) safeString(data, "fuzz-leave-auth-i2") else ""
    val twoMembers = version >= 3 && data.consumeBoolean()

    val members = buildMemberIdentities(version, memberId1, instance1, twoMembers, memberId2, instance2)
    val built = new LeaveGroupRequest.Builder(groupId, members).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED)))
    try kafkaApis.handleLeaveGroupRequest(request).join()
    finally kafkaApis.close()
  }

  /**
   * Authorized path: `leaveGroup` future completes with a response or with a
   * Kafka error; covers legacy (v0–2) normalized payload vs batch v3+.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaveGroupCoordinatorFuture(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LEAVE_GROUP.oldestVersion().toInt,
      ApiKeys.LEAVE_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-leave-g")
    val memberId1 = safeString(data, "fuzz-leave-m1")
    val memberId2 = safeString(data, "fuzz-leave-m2")
    val useInst1 = data.consumeBoolean()
    val instance1 = if (useInst1) safeString(data, "fuzz-leave-i1") else ""
    val useInst2 = data.consumeBoolean()
    val instance2 = if (useInst2) safeString(data, "fuzz-leave-i2") else ""
    val twoMembers = version >= 3 && data.consumeBoolean()
    val completeWithException = data.consumeBoolean()
    val useRequestTimeoutError = data.consumeBoolean()

    val members = buildMemberIdentities(version, memberId1, instance1, twoMembers, memberId2, instance2)
    val built = new LeaveGroupRequest.Builder(groupId, members).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(any(), any[LeaveGroupRequestData])).thenReturn(future)

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try {
      val handled = kafkaApis.handleLeaveGroupRequest(request)
      if (completeWithException) {
        val err =
          if (useRequestTimeoutError) Errors.REQUEST_TIMED_OUT
          else Errors.COORDINATOR_NOT_AVAILABLE
        future.completeExceptionally(err.exception)
      } else {
        future.complete(successLeaveResponse(version, memberId1, instance1, twoMembers, memberId2, instance2))
      }
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Non-zero request-quota throttle on the coordinator completion path
   * (`sendMaybeThrottle` after `leaveGroup` completes).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaveGroupThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LEAVE_GROUP.oldestVersion().toInt,
      ApiKeys.LEAVE_GROUP.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val groupId = safeString(data, "fuzz-leave-throttle-g")
    val memberId1 = safeString(data, "fuzz-leave-throttle-m1")
    val memberId2 = safeString(data, "fuzz-leave-throttle-m2")
    val useInst1 = data.consumeBoolean()
    val instance1 = if (useInst1) safeString(data, "fuzz-leave-throttle-i1") else ""
    val useInst2 = data.consumeBoolean()
    val instance2 = if (useInst2) safeString(data, "fuzz-leave-throttle-i2") else ""
    val twoMembers = version >= 3 && data.consumeBoolean()

    val members = buildMemberIdentities(version, memberId1, instance1, twoMembers, memberId2, instance2)
    val built = new LeaveGroupRequest.Builder(groupId, members).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val responseData = successLeaveResponse(version, memberId1, instance1, twoMembers, memberId2, instance2)
    when(groupCoordinator.leaveGroup(any(), any[LeaveGroupRequestData]))
      .thenReturn(CompletableFuture.completedFuture(responseData))

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try kafkaApis.handleLeaveGroupRequest(request).join()
    finally kafkaApis.close()
  }
}
