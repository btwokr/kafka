package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import org.apache.kafka.common.message.{ListGroupsRequestData, ListGroupsResponseData}
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ListGroupsRequest, RequestContext}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleListGroupsRequest`.
 *
 * After checking DESCRIBE on CLUSTER (without logging denials), the handler
 * calls `groupCoordinator.listGroups`. On success it either returns the full
 * coordinator response when cluster describe is allowed, or filters listed
 * groups to those the caller may DESCRIBE.
 */
class HandleListGroupsRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  private def stubNoThrottle(): Unit = {
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  /**
   * DESCRIBE on CLUSTER vs GROUP; cluster describe result is fixed, per-group
   * DESCRIBE is resolved by membership in `groupIds` and `groupAllowed`.
   */
  private def listGroupsAuthorizer(
    clusterDescribeAllowed: Boolean,
    groupIds: Array[String],
    groupAllowed: Array[Boolean]
  ): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenAnswer(invocation => {
        val actions = invocation.getArgument(1, classOf[util.List[Action]])
        val a = actions.get(0)
        val pattern = a.resourcePattern
        val result =
          if (pattern.resourceType == ResourceType.CLUSTER) {
            if (clusterDescribeAllowed) AuthorizationResult.ALLOWED
            else AuthorizationResult.DENIED
          } else if (pattern.resourceType == ResourceType.GROUP) {
            val gid = pattern.name
            val idx = groupIds.indexOf(gid)
            val ok = idx >= 0 && idx < groupAllowed.length && groupAllowed(idx)
            if (ok) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
          } else {
            AuthorizationResult.DENIED
          }
        Collections.singletonList(result)
      })
    authorizer
  }

  private def buildListGroupsRequestData(
    version: Short,
    stateA: String,
    stateB: String,
    typeA: String,
    typeB: String
  ): ListGroupsRequestData = {
    val d = new ListGroupsRequestData()
    if (version >= 4)
      d.setStatesFilter(util.Arrays.asList(stateA, stateB))
    if (version >= 5)
      d.setTypesFilter(util.Arrays.asList(typeA, typeB))
    d
  }

  private def coordinatorListedGroups(
    version: Short,
    groupIds: Array[String],
    protoBase: String
  ): util.List[ListedGroup] = {
    groupIds.indices.map { i =>
      new ListedGroup()
        .setGroupId(groupIds(i))
        .setProtocolType(s"$protoBase-$i")
        .setGroupState(if (version >= 4) "Stable" else "")
        .setGroupType(if (version >= 5) "consumer" else "")
    }.asJava
  }

  /**
   * No `Authorizer`: cluster DESCRIBE is treated as allowed, so the full
   * coordinator listing is returned unchanged.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListGroupsPassthroughWithoutAuthorizer(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LIST_GROUPS.oldestVersion().toInt,
      ApiKeys.LIST_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(1, 5)
    val groupBase = safeString(data, "fuzz-lg")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val stateA = safeString(data, "Stable")
    val stateB = safeString(data, "Empty")
    val typeA = safeString(data, "classic")
    val typeB = safeString(data, "consumer")
    val protoBase = safeString(data, "proto")

    val reqData = buildListGroupsRequestData(version, stateA, stateB, typeA, typeB)
    val built = new ListGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val responseData = new ListGroupsResponseData()
      .setErrorCode(Errors.NONE.code)
      .setGroups(coordinatorListedGroups(version, groupIds, protoBase))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      val handled = kafkaApis.handleListGroupsRequest(request)
      future.complete(responseData)
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * CLUSTER DESCRIBE denied: response groups are filtered to those with GROUP
   * DESCRIBE allowed.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListGroupsFilteredWhenClusterDescribeDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LIST_GROUPS.oldestVersion().toInt,
      ApiKeys.LIST_GROUPS.latestVersion().toInt).toShort
    val numGroups = data.consumeInt(2, 5)
    val groupBase = safeString(data, "fuzz-lg-filter")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val groupAllowed = Array.tabulate(numGroups)(_ => data.consumeBoolean())
    val stateA = safeString(data, "Stable")
    val stateB = safeString(data, "Empty")
    val typeA = safeString(data, "classic")
    val typeB = safeString(data, "consumer")
    val protoBase = safeString(data, "proto")

    val reqData = buildListGroupsRequestData(version, stateA, stateB, typeA, typeB)
    val built = new ListGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val fullResponse = new ListGroupsResponseData()
      .setErrorCode(Errors.NONE.code)
      .setGroups(coordinatorListedGroups(version, groupIds, protoBase))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis(authorizer = Some(listGroupsAuthorizer(
      clusterDescribeAllowed = false,
      groupIds,
      groupAllowed
    )))
    try {
      val handled = kafkaApis.handleListGroupsRequest(request)
      future.complete(fullResponse)
      handled.join()
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Coordinator future completes exceptionally; error response is sent for
   * each listed group id from the request body.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListGroupsCoordinatorException(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LIST_GROUPS.oldestVersion().toInt,
      ApiKeys.LIST_GROUPS.latestVersion().toInt).toShort
    val stateA = safeString(data, "Stable")
    val stateB = safeString(data, "Empty")
    val typeA = safeString(data, "classic")
    val typeB = safeString(data, "consumer")
    val useRequestTimeout = data.consumeBoolean()

    val reqData = buildListGroupsRequestData(version, stateA, stateB, typeA, typeB)
    val built = new ListGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    stubNoThrottle()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(any(), any())).thenReturn(future)

    val kafkaApis = createKafkaApis()
    try {
      val handled = kafkaApis.handleListGroupsRequest(request)
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
   * Non-zero request-quota throttle on a successful list completion.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestListGroupsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.LIST_GROUPS.oldestVersion().toInt,
      ApiKeys.LIST_GROUPS.latestVersion().toInt).toShort
    val throttleMs = data.consumeInt(1, 500)
    val numGroups = data.consumeInt(1, 3)
    val groupBase = safeString(data, "fuzz-lg-th")
    val groupIds = Array.tabulate(numGroups)(i => s"$groupBase-$i")
    val stateA = safeString(data, "Stable")
    val stateB = safeString(data, "Empty")
    val typeA = safeString(data, "classic")
    val typeB = safeString(data, "consumer")
    val protoBase = safeString(data, "proto")

    val reqData = buildListGroupsRequestData(version, stateA, stateB, typeA, typeB)
    val built = new ListGroupsRequest.Builder(reqData).build(version)
    val request = buildRequest(built)

    val responseData = new ListGroupsResponseData()
      .setErrorCode(Errors.NONE.code)
      .setGroups(coordinatorListedGroups(version, groupIds, protoBase))

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, groupCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    when(groupCoordinator.listGroups(any(), any()))
      .thenReturn(CompletableFuture.completedFuture(responseData))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleListGroupsRequest(request).join()
    finally kafkaApis.close()
  }
}
