package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.message.{SyncGroupRequestData, SyncGroupResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_2_IV1, IBP_2_3_IV0}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleSyncGroupRequest`.
 *
 * The method mirrors JoinGroup / Heartbeat static-membership guards, then
 * enforces mandatory protocol type/name for SyncGroup v5+, group
 * authorization, and finally delegates to `groupCoordinator.syncGroup` with
 * success, exception, or throttled response paths.
 */
class HandleSyncGroupRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  /** `SyncGroupRequest.Builder` rejects `groupInstanceId` on wire versions below 3. */
  private def syncVersionAllowingGroupInstance(version: Short, groupInstanceId: String): Short =
    if (groupInstanceId != null && version < 3)
      3
    else
      version

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

  /**
   * `groupInstanceId` on inter-broker protocol older than 2.3 yields
   * `UNSUPPORTED_VERSION` before the coordinator runs.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupStaticMembershipOldIbp(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.SYNC_GROUP.oldestVersion().toInt,
      ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-g")
    val memberId = safeString(data, "fuzz-m")
    val generationId = data.consumeInt()
    val groupInstanceId = safeString(data, "fuzz-static-instance")
    val protocolType = safeString(data, "consumer")
    val protocolName = safeString(data, "range")
    val assignMemberId = safeString(data, "assign-m")
    val assignLen = data.consumeInt(0, 64)
    val assignBytes = data.consumeBytes(assignLen)
    val assignment = new SyncGroupRequestData.SyncGroupRequestAssignment()
      .setMemberId(assignMemberId)
      .setAssignment(assignBytes)
    val assignments = Collections.singletonList(assignment)

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setGroupInstanceId(groupInstanceId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments)

    val builtVersion = syncVersionAllowingGroupInstance(version, groupInstanceId)
    val syncReq = new SyncGroupRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(syncReq)

    metadataCache = MetadataCache.zkMetadataCache(brokerId, IBP_2_2_IV1)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(IBP_2_2_IV1)
    try kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }

  /**
   * From SyncGroup v5 onward, missing protocol type or name yields
   * `INCONSISTENT_GROUP_PROTOCOL` without calling the coordinator.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupInconsistentProtocol(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(5, ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val omitProtocolType = data.consumeBoolean()
    val groupId = safeString(data, "fuzz-g")
    val memberId = safeString(data, "fuzz-m")
    val generationId = data.consumeInt()
    val presentProtocol = safeString(data, "consumer")

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setAssignments(Collections.emptyList())
    if (omitProtocolType) {
      requestData.setProtocolType(null)
      requestData.setProtocolName(presentProtocol)
    } else {
      requestData.setProtocolType(presentProtocol)
      requestData.setProtocolName(null)
    }

    val syncReq = new SyncGroupRequest.Builder(requestData).build(version)
    val request = buildRequest(syncReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }

  /**
   * Group READ denied yields `GROUP_AUTHORIZATION_FAILED` without coordinator
   * interaction. Request fields satisfy v5 mandatory protocol checks when
   * the wire version is 5 or higher.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.SYNC_GROUP.oldestVersion().toInt,
      ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-auth-g")
    val memberId = safeString(data, "fuzz-auth-m")
    val generationId = data.consumeInt()
    val useInstance = version >= 3 && data.consumeBoolean()
    val groupInstanceId = if (useInstance) safeString(data, "fuzz-instance") else null
    val protocolType =
      if (version >= 5) safeString(data, "consumer") else null
    val protocolName =
      if (version >= 5) safeString(data, "range") else null
    val assignMemberId = safeString(data, "m")
    val assignLen = data.consumeInt(0, 32)
    val assignBytes = data.consumeBytes(assignLen)
    val assignment = new SyncGroupRequestData.SyncGroupRequestAssignment()
      .setMemberId(assignMemberId)
      .setAssignment(assignBytes)
    val assignments = Collections.singletonList(assignment)

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments)
    if (groupInstanceId != null)
      requestData.setGroupInstanceId(groupInstanceId)

    val builtVersion = syncVersionAllowingGroupInstance(version, groupInstanceId)
    val syncReq = new SyncGroupRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(syncReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.DENIED)))
    try kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }

  /**
   * Authorized path: coordinator future completes with a response or with a
   * Kafka error exception; exercises both branches of the completion handler.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupCoordinatorFuture(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.SYNC_GROUP.oldestVersion().toInt,
      ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-coord-g")
    val memberId = safeString(data, "fuzz-coord-m")
    val generationId = data.consumeInt()
    val useInstance = version >= 3 && data.consumeBoolean()
    val groupInstanceId = if (useInstance) safeString(data, "fuzz-inst") else null
    val protocolType =
      if (version >= 5) safeString(data, "consumer") else null
    val protocolName =
      if (version >= 5) safeString(data, "range") else null
    val assignMemberId = safeString(data, "am")
    val assignLen = data.consumeInt(0, 64)
    val assignBytes = data.consumeBytes(assignLen)
    val assignment = new SyncGroupRequestData.SyncGroupRequestAssignment()
      .setMemberId(assignMemberId)
      .setAssignment(assignBytes)
    val assignments = Collections.singletonList(assignment)
    val completeWithException = data.consumeBoolean()
    val useRequestTimeoutError = data.consumeBoolean()
    val successProtoType = safeString(data, "consumer")
    val successProtoName = safeString(data, "range")
    val respAssignLen = data.consumeInt(0, 64)
    val respAssignBytes = data.consumeBytes(respAssignLen)

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments)
    if (groupInstanceId != null)
      requestData.setGroupInstanceId(groupInstanceId)

    val builtVersion = syncVersionAllowingGroupInstance(version, groupInstanceId)
    val syncReq = new SyncGroupRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(syncReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(any(), any[SyncGroupRequestData], any())).thenReturn(future)

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try {
      val handled = kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching)
      if (completeWithException) {
        val err =
          if (useRequestTimeoutError) Errors.REQUEST_TIMED_OUT
          else Errors.COORDINATOR_NOT_AVAILABLE
        future.completeExceptionally(err.exception)
      } else {
        future.complete(new SyncGroupResponseData()
          .setProtocolType(if (version >= 5) successProtoType else null)
          .setProtocolName(if (version >= 5) successProtoName else null)
          .setAssignment(respAssignBytes))
      }
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Static membership allowed on IBP &ge; 2.3 with non-null `groupInstanceId`
   * so the coordinator path runs (same handler shape as dynamic members).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupStaticMembershipSupportedIbp(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.SYNC_GROUP.oldestVersion().toInt,
      ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val groupId = safeString(data, "fuzz-static-ok-g")
    val memberId = safeString(data, "fuzz-static-ok-m")
    val generationId = data.consumeInt(0, Int.MaxValue)
    val groupInstanceId = safeString(data, "fuzz-static-ok-inst")
    val protocolType =
      if (version >= 5) safeString(data, "consumer") else null
    val protocolName =
      if (version >= 5) safeString(data, "range") else null
    val assignMemberId = safeString(data, "am")
    val assignLen = data.consumeInt(0, 48)
    val assignBytes = data.consumeBytes(assignLen)
    val responseProtoType = safeString(data, "consumer")
    val responseProtoName = safeString(data, "range")
    val assignment = new SyncGroupRequestData.SyncGroupRequestAssignment()
      .setMemberId(assignMemberId)
      .setAssignment(assignBytes)
    val assignments = Collections.singletonList(assignment)

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setGroupInstanceId(groupInstanceId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments)

    val builtVersion = syncVersionAllowingGroupInstance(version, groupInstanceId)
    val syncReq = new SyncGroupRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(syncReq)

    metadataCache = MetadataCache.zkMetadataCache(brokerId, IBP_2_3_IV0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(any(), any[SyncGroupRequestData], any())).thenReturn(future)

    val kafkaApis = createKafkaApis(IBP_2_3_IV0, authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try {
      val handled = kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching)
      future.complete(new SyncGroupResponseData()
        .setProtocolType(if (version >= 5) responseProtoType else null)
        .setProtocolName(if (version >= 5) responseProtoName else null)
        .setAssignment(Array.emptyByteArray))
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Positive request-quota throttle on the coordinator completion path
   * (`sendMaybeThrottle` with non-zero throttle time).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestSyncGroupThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.SYNC_GROUP.oldestVersion().toInt,
      ApiKeys.SYNC_GROUP.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val groupId = safeString(data, "fuzz-throttle-g")
    val memberId = safeString(data, "fuzz-throttle-m")
    val generationId = data.consumeInt()
    val useInstance = version >= 3 && data.consumeBoolean()
    val groupInstanceId = if (useInstance) safeString(data, "fuzz-throttle-inst") else null
    val protocolType =
      if (version >= 5) safeString(data, "consumer") else null
    val protocolName =
      if (version >= 5) safeString(data, "range") else null
    val assignMemberId = safeString(data, "tm")
    val assignLen = data.consumeInt(0, 32)
    val assignBytes = data.consumeBytes(assignLen)
    val assignment = new SyncGroupRequestData.SyncGroupRequestAssignment()
      .setMemberId(assignMemberId)
      .setAssignment(assignBytes)
    val assignments = Collections.singletonList(assignment)
    val responseProtoType = safeString(data, "consumer")
    val responseProtoName = safeString(data, "range")
    val respAssignLen = data.consumeInt(0, 32)
    val respAssignBytes = data.consumeBytes(respAssignLen)

    val requestData = new SyncGroupRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId)
      .setGenerationId(generationId)
      .setProtocolType(protocolType)
      .setProtocolName(protocolName)
      .setAssignments(assignments)
    if (groupInstanceId != null)
      requestData.setGroupInstanceId(groupInstanceId)

    val builtVersion = syncVersionAllowingGroupInstance(version, groupInstanceId)
    val syncReq = new SyncGroupRequest.Builder(requestData).build(builtVersion)
    val request = buildRequest(syncReq)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val responseData = new SyncGroupResponseData()
      .setProtocolType(if (version >= 5) responseProtoType else null)
      .setProtocolName(if (version >= 5) responseProtoName else null)
      .setAssignment(respAssignBytes)

    when(groupCoordinator.syncGroup(any(), any[SyncGroupRequestData], any()))
      .thenReturn(CompletableFuture.completedFuture(responseData))

    val kafkaApis = createKafkaApis(authorizer = Some(allowOrDenyAuthorizer(AuthorizationResult.ALLOWED)))
    try kafkaApis.handleSyncGroupRequest(request, RequestLocal.NoCaching).join()
    finally kafkaApis.close()
  }
}

