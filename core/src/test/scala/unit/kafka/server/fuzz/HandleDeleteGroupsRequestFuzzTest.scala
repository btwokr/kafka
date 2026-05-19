/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.coordinator.group.DeleteGroupsFuzzCoordinatorHarness
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, KafkaConfig, MetadataCache, ReplicaManager, RequestLocal, ZkBrokerEpochManager}
import kafka.utils.TestUtils
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DeleteGroupsRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteGroupsRequest, DeleteGroupsResponse}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests for `KafkaApis.handleDeleteGroupsRequest`.
 *
 * Uses a real classic [[GroupCoordinatorAdapter]] stack (no Mockito on
 * `org.apache.kafka.coordinator.group.GroupCoordinator`). A dedicated
 * [[ReplicaManager]] mock feeds only the coordinator; `KafkaApis` keeps the
 * usual harness `replicaManager` mock for unrelated paths.
 */
class HandleDeleteGroupsRequestFuzzTest extends KafkaApisTest {

  /** Same ids as [[kafka.coordinator.group.GroupCoordinatorTest]]: distinct `partitionFor` under two partitions. */
  private val CoordinatorPartitionGroupA = "groupId"
  private val CoordinatorPartitionGroupB = "otherGroup"

  private def resetHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  private def stubClientThrottle(throttleTimeMs: Int): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleTimeMs)
  }

  private def coordinatorConfig(iv: MetadataVersion): KafkaConfig = {
    val properties = TestUtils.createBrokerConfig(brokerId, "zk")
    TestUtils.setIbpAndMessageFormatVersions(properties, iv)
    new KafkaConfig(properties)
  }

  private def startClassicGroupCoordinator(
    coordConfig: KafkaConfig,
    coordReplicaManager: ReplicaManager
  ): org.apache.kafka.coordinator.group.GroupCoordinator = {
    when(zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME))
      .thenReturn(Some(2))
    DeleteGroupsFuzzCoordinatorHarness.newStartedAdapter(
      coordConfig,
      coordReplicaManager,
      time,
      metrics,
      zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(2))
  }

  private def installOwnedPartition(adapter: org.apache.kafka.coordinator.group.GroupCoordinator, groupId: String): Unit =
    DeleteGroupsFuzzCoordinatorHarness.installOwnedPartition(adapter, groupId)

  private def deleteResultsByGroupId(response: DeleteGroupsResponse): Map[String, Errors] =
    response.data.results.asScala.map(r => r.groupId -> Errors.forCode(r.errorCode)).toMap

  private def denyAllAuthorizer(): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      Collections.nCopies(actions.size, AuthorizationResult.DENIED)
    })
    authorizer
  }

  private def selectiveDeleteAuthorizer(allowed: Set[String]): Authorizer = {
    val authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { a =>
        if (allowed.contains(a.resourcePattern().name)) AuthorizationResult.ALLOWED
        else AuthorizationResult.DENIED
      }.asJava
    })
    authorizer
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsAuthorizedGroupIdNotFound(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val fuzzGroup = data.consumeString(48)
    val groupId = if (fuzzGroup == null || fuzzGroup.isEmpty) "fuzz-del-g" else fuzzGroup

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, groupId)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(Collections.singletonList(groupId))
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        assertEquals(Map(groupId -> Errors.GROUP_ID_NOT_FOUND), deleteResultsByGroupId(response))
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsAllUnauthorized(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val n = data.consumeInt(1, 5)
    val names = (0 until n).map(i => s"fuzz-deny-$i-${data.consumeString(8)}").toList.asJava

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(names)
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(
        interBrokerProtocolVersion = iv,
        authorizer = Some(denyAllAuthorizer()),
        groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        val expected = names.asScala.map(_ -> Errors.GROUP_AUTHORIZATION_FAILED).toMap
        assertEquals(expected, deleteResultsByGroupId(response))
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsMixedAuthGroupIdNotFound(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val gDenied = "fuzz-mix-denied"
    val g1 = "fuzz-mix-a"
    val g2 = "fuzz-mix-b"

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, g1)
      installOwnedPartition(gc, g2)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(List(gDenied, g1, g2).asJava)
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(
        interBrokerProtocolVersion = iv,
        authorizer = Some(selectiveDeleteAuthorizer(Set(g1, g2))),
        groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        val m = deleteResultsByGroupId(response)
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, m(gDenied))
        assertEquals(Errors.GROUP_ID_NOT_FOUND, m(g1))
        assertEquals(Errors.GROUP_ID_NOT_FOUND, m(g2))
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsDuplicateGroupIds(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val g = data.consumeString(32)
    val groupId = if (g == null || g.isEmpty) "fuzz-dup-g" else g

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, groupId)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(List(groupId, groupId).asJava)
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        assertEquals(Map(groupId -> Errors.GROUP_ID_NOT_FOUND), deleteResultsByGroupId(response))
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsNotCoordinatorOtherPartition(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, CoordinatorPartitionGroupA)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(
          List(CoordinatorPartitionGroupA, CoordinatorPartitionGroupB).asJava)
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        val m = deleteResultsByGroupId(response)
        assertEquals(Errors.GROUP_ID_NOT_FOUND, m(CoordinatorPartitionGroupA))
        assertEquals(Errors.NOT_COORDINATOR, m(CoordinatorPartitionGroupB))
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsEmptyGroupList(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()

    resetHarness()
    stubNoThrottle()

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(Collections.emptyList())
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try {
        kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
        val response = verifyNoThrottling[DeleteGroupsResponse](request)
        assertEquals(0, response.data.results.size)
      } finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsThrottledResponse(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val throttleMs = data.consumeInt(1, 500)
    val groupId = data.consumeString(24) match {
      case null | "" => "fuzz-del-throttle"
      case s => s
    }

    resetHarness()
    stubClientThrottle(throttleMs)

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, groupId)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(Collections.singletonList(groupId))
      ).build(wireVersion)
      val request = buildRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
      finally kafkaApis.close()
    } finally gc.shutdown()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestDeleteGroupsForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val wireVersion = data.consumeShort(
      ApiKeys.DELETE_GROUPS.oldestVersion(),
      ApiKeys.DELETE_GROUPS.latestVersion())
    val iv = MetadataVersion.latestTesting()
    val requestThrottleMs = data.consumeInt(0, 200)
    val groupId = data.consumeString(20) match {
      case null | "" => "fuzz-del-fwd"
      case s => s
    }

    resetHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottleMs)

    val coordRm = mock(classOf[ReplicaManager])
    val gc = startClassicGroupCoordinator(coordinatorConfig(iv), coordRm)
    try {
      installOwnedPartition(gc, groupId)

      val built = new DeleteGroupsRequest.Builder(
        new DeleteGroupsRequestData().setGroupsNames(Collections.singletonList(groupId))
      ).build(wireVersion)
      val request = buildForwardedRequest(built)

      val kafkaApis = createKafkaApis(interBrokerProtocolVersion = iv, groupCoordinatorForApis = Some(gc))
      try kafkaApis.handleDeleteGroupsRequest(request, RequestLocal.NoCaching).join()
      finally kafkaApis.close()
    } finally gc.shutdown()
  }
}
