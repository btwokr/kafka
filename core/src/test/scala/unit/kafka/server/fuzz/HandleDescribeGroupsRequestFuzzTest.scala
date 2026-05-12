package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import org.apache.kafka.common.message.DescribeGroupsRequestData
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DescribeGroupsRequest, RequestContext}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDescribeGroupsRequest`.
 *
 * The handler partitions group ids by DESCRIBE authorization, attaches
 * `GROUP_AUTHORIZATION_FAILED` entries for denied ids, then calls
 * `groupCoordinator.describeGroups` with the authorized subset. On success
 * it may enrich results with `authorizedOperations` when the wire version is
 * at least 3 and `includeAuthorizedOperations` is set.
 */
class HandleDescribeGroupsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  private def stubNoThrottle(): Unit = {
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  /**
   * Per-group DESCRIBE on GROUP; multi-action `authorize` calls (used when
   * filling `authorizedOperations`) return ALLOWED for every operation.
   */
  private def describeGroupsAuthorizer(groupIds: Array[String], allowed: Array[Boolean]): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer(invocation => {
        val actions = invocation.getArgument(1, classOf[util.List[Action]])
        if (actions.size == 1) {
          val name = actions.get(0).resourcePattern.name
          val idx = groupIds.indexOf(name)
          val ok = idx >= 0 && idx < allowed.length && allowed(idx)
          Collections.singletonList(
            if (ok) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED)
        } else {
          Collections.nCopies(actions.size, AuthorizationResult.ALLOWED)
        }
      })
    authorizer
  }

  private def buildRequestData(
    version: Short,
    groupIds: Array[String],
    includeAuthorizedOperations: Boolean
  ): DescribeGroupsRequestData = {
    val names = new util.ArrayList[String]
    groupIds.foreach(names.add)
    val data = new DescribeGroupsRequestData().setGroups(names)
    if (version >= 3)
      data.setIncludeAuthorizedOperations(includeAuthorizedOperations)
    data
  }

  /**
   * Some group ids denied, some allowed; coordinator receives only the
   * authorized ids in request order. Optionally sets
   * `includeAuthorizedOperations` on v3+ to exercise the enrichment loop.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeGroupsMixedAuthorization(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_GROUPS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(2, 5)
    val groupBase = safeString(data, "fuzz-dg")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val allowed = Array.tabulate(numGroups)(_ => data.consumeBoolean())
    val includeAuthorizedOperations = data.consumeBoolean()
    val coordErrIsTimeout = data.consumeBoolean()

    val reqData = buildRequestData(version, groupIds, includeAuthorizedOperations)
    val built = new DescribeGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val authorizedOrdered = (0 until numGroups).filter(allowed(_)).map(groupIds(_)).toList
    val coordinatorResults = authorizedOrdered.map { gid =>
      new DescribedGroup()
        .setGroupId(gid)
        .setErrorCode(
          if (coordErrIsTimeout) Errors.REQUEST_TIMED_OUT.code
          else Errors.NOT_COORDINATOR.code
        )
    }.asJava

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.describeGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis(authorizer = Some(describeGroupsAuthorizer(groupIds, allowed)))
    try {
      val handled = kafkaApis.handleDescribeGroupsRequest(request)
      future.complete(coordinatorResults)
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * All groups denied: coordinator runs with an empty id list; response
   * contains only per-group authorization errors.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeGroupsAllGroupsUnauthorized(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_GROUPS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(1, 4)
    val groupBase = safeString(data, "fuzz-deny-all")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val includeAuthorizedOperations = data.consumeBoolean()

    val reqData = buildRequestData(version, groupIds, includeAuthorizedOperations)
    val built = new DescribeGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val denyAll = mock(classOf[Authorizer])
    when(denyAll.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Collections.singletonList(AuthorizationResult.DENIED))

    when(groupCoordinator.describeGroups(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(new util.ArrayList[DescribedGroup]()))

    val kafkaApis = createKafkaApis(authorizer = Some(denyAll))
    try kafkaApis.handleDescribeGroupsRequest(request).join()
    finally kafkaApis.close()
  }

  /**
   * No authorizer: all ids are authorized; coordinator future completes
   * exceptionally so `getErrorResponse` runs.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeGroupsCoordinatorException(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_GROUPS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(1, 4)
    val groupBase = safeString(data, "fuzz-dg-err")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val includeAuthorizedOperations = data.consumeBoolean()
    val useRequestTimeout = data.consumeBoolean()

    val reqData = buildRequestData(version, groupIds, includeAuthorizedOperations)
    val built = new DescribeGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.describeGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      val handled = kafkaApis.handleDescribeGroupsRequest(request)
      val err =
        if (useRequestTimeout) Errors.REQUEST_TIMED_OUT
        else Errors.COORDINATOR_NOT_AVAILABLE
      future.completeExceptionally(err.exception)
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Successful describe with optional `includeAuthorizedOperations` on v3+;
   * at least one described group uses `NONE` so `authorizedOperations` may be
   * populated (no `Authorizer` so all operations are treated as allowed).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeGroupsSuccessWithAuthorizedOperations(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_GROUPS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(1, 3)
    val groupBase = safeString(data, "fuzz-dg-ok")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val includeAuthorizedOperations = data.consumeBoolean()
    val protocolType = safeString(data, "consumer")
    val protocolData = safeString(data, "range")
    val groupState = safeString(data, "Stable")

    val reqData = buildRequestData(version, groupIds, includeAuthorizedOperations)
    val built = new DescribeGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val coordinatorResults = groupIds.map { gid =>
      new DescribedGroup()
        .setGroupId(gid)
        .setErrorCode(Errors.NONE.code)
        .setProtocolType(protocolType)
        .setProtocolData(protocolData)
        .setGroupState(groupState)
    }.toList.asJava

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[util.List[DescribedGroup]]()
    when(groupCoordinator.describeGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      val handled = kafkaApis.handleDescribeGroupsRequest(request)
      future.complete(coordinatorResults)
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Non-zero request-quota throttle after a successful coordinator completion.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDescribeGroupsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_GROUPS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_GROUPS.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val numGroups = data.consumeInt(1, 3)
    val groupBase = safeString(data, "fuzz-dg-th")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val includeAuthorizedOperations = data.consumeBoolean()

    val reqData = buildRequestData(version, groupIds, includeAuthorizedOperations)
    val built = new DescribeGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val coordinatorResults = groupIds.map { gid =>
      new DescribedGroup().setGroupId(gid).setErrorCode(Errors.NONE.code)
    }.toList.asJava

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    when(groupCoordinator.describeGroups(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(coordinatorResults))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeGroupsRequest(request).join()
    finally kafkaApis.close()
  }
}
