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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unit.kafka.server.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.server.{KafkaApisTest, MetadataCache, ZkBrokerEpochManager}
import org.apache.kafka.common.Node
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractControlRequest, LeaderAndIsrRequest, LeaderAndIsrResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{KRaftVersion, MetadataVersion}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, never, reset, verify, verifyNoMoreInteractions, when}

import java.util
import java.util.Collections
import java.util.Arrays.asList

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleLeaderAndIsrRequest`.
 *
 * Covers ZooKeeper metadata path, `CLUSTER_ACTION` authorization, broker epoch
 * staleness vs `UNKNOWN_BROKER_EPOCH`, `replicaManager.becomeLeaderOrFollower`
 * success, KRaft controller flag interaction with `ZkBrokerEpochManager`
 * (missing `BrokerLifecycleManager`), KRaft metadata (`requireZkOrThrow` /
 * `shouldNeverReceive` message), and forwarded inner requests
 * (`sendResponseExemptThrottle`).
 */
class HandleLeaderAndIsrRequestFuzzTest extends KafkaApisTest {

  /** Matches `AuthHelper.authorizeClusterOperation` when cluster action is denied. */
  private def validateClusterAuthorizationExceptionMessage(e: ClusterAuthorizationException): Unit = {
    val message = e.getMessage
    if (message == null || !message.startsWith("Request ") || !message.endsWith(" is not authorized."))
      throw e
  }

  /** Matches `KafkaApis.shouldNeverReceive` for `ApiKeys.LEADER_AND_ISR` on KRaft. */
  private def validateUnsupportedVersionShouldNeverReceiveLeaderAndIsrMessage(e: UnsupportedVersionException): Unit = {
    val expectedMessage =
      s"Should never receive when using a Raft-based metadata quorum: ${ApiKeys.LEADER_AND_ISR.name()}"
    if (e.getMessage != expectedMessage) throw e
  }

  /** Matches `ZkBrokerEpochManager.isBrokerEpochStale` when `isKRaftController` is true without a lifecycle manager. */
  private def validateIllegalStateMissingBrokerLifecycleManagerMessage(e: IllegalStateException): Unit = {
    val expectedMessage = "Expected BrokerLifecycleManager to be non-null."
    if (e.getMessage != expectedMessage) throw e
  }

  private def safeTopicName(data: FuzzedDataProvider, fallback: String): String = {
    val raw = data.consumeString(48)
    val base = if (raw == null || raw.isEmpty) fallback else raw
    base.replaceAll("[^a-zA-Z0-9._-]", "_").take(200)
  }

  private def resetZkLeaderAndIsrHarness(): Unit = {
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.latestTesting())
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)
    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
  }

  private def partitionState(topicName: String, partitionIndex: Int): LeaderAndIsrPartitionState = {
    new LeaderAndIsrPartitionState()
      .setTopicName(topicName)
      .setPartitionIndex(partitionIndex)
      .setControllerEpoch(1)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setIsr(asList(0, 1))
      .setPartitionEpoch(2)
      .setReplicas(asList(0, 1, 2))
      .setIsNew(false)
  }

  private def liveLeaders(): util.List[Node] =
    asList(new Node(0, "host0", 9090), new Node(1, "host1", 9091))

  private def newLeaderAndIsrBuilder(
    version: Short,
    controllerId: Int,
    controllerEpoch: Int,
    brokerEpoch: Long,
    topicName: String,
    kraftController: Boolean
  ): LeaderAndIsrRequest.Builder = {
    val partitionStates = Collections.singletonList(partitionState(topicName, 1))
    val topicIds = Collections.singletonMap(topicName, Uuid.randomUuid())
    if (version >= 7) {
      new LeaderAndIsrRequest.Builder(
        version,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        partitionStates,
        topicIds,
        liveLeaders(),
        kraftController,
        AbstractControlRequest.Type.UNKNOWN
      )
    } else {
      new LeaderAndIsrRequest.Builder(
        version,
        controllerId,
        controllerEpoch,
        brokerEpoch,
        partitionStates,
        topicIds,
        liveLeaders()
      )
    }
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrBecomeLeaderOrFollowerSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.LEADER_AND_ISR.oldestVersion(), ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 32)
    val currentBrokerEpoch = data.consumeLong(1000L, 1L << 40)
    val brokerEpochInRequest = data.consumeLong(currentBrokerEpoch, currentBrokerEpoch + 1000L)
    val topicName = safeTopicName(data, "fuzz-lai-success")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = false
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val response = new LeaderAndIsrResponse(
      new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code),
      version
    )
    when(replicaManager.becomeLeaderOrFollower(
      ArgumentMatchers.eq(request.context.correlationId),
      any(),
      any()
    )).thenReturn(response)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleLeaderAndIsrRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrStaleBrokerEpoch(data: FuzzedDataProvider): Unit = {
    // Broker epoch exists on the wire only for v2+; older versions deserialize as `UNKNOWN_BROKER_EPOCH` (-1),
    // which is never considered stale.
    val version = data.consumeShort(2, ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 24)
    val currentBrokerEpoch = data.consumeLong(5000L, 1L << 39)
    val rawBrokerEpoch = data.consumeLong(0L, currentBrokerEpoch - 1)
    // FuzzedDataProvider may yield -1 (`UNKNOWN_BROKER_EPOCH`), which is never treated as stale.
    val brokerEpochInRequest =
      if (rawBrokerEpoch == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) 0L
      else rawBrokerEpoch
    val topicName = safeTopicName(data, "fuzz-lai-stale")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = false
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleLeaderAndIsrRequest(request)
      verify(replicaManager, never()).becomeLeaderOrFollower(anyInt(), any(), any())
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrUnknownBrokerEpoch(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.LEADER_AND_ISR.oldestVersion(), ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 20)
    val currentBrokerEpoch = data.consumeLong(8000L, 1L << 38)
    val topicName = safeTopicName(data, "fuzz-lai-unknown-epoch")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version,
      controllerId,
      controllerEpoch,
      AbstractControlRequest.UNKNOWN_BROKER_EPOCH,
      topicName,
      kraftController = false
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val response = new LeaderAndIsrResponse(
      new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code),
      version
    )
    when(replicaManager.becomeLeaderOrFollower(
      ArgumentMatchers.eq(request.context.correlationId),
      any(),
      any()
    )).thenReturn(response)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleLeaderAndIsrRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrClusterActionDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.LEADER_AND_ISR.oldestVersion(), ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 18)
    val brokerEpochInRequest = data.consumeLong(1L, 1L << 35)
    val topicName = safeTopicName(data, "fuzz-lai-cluster-deny")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(1L)

    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.CLUSTER_ACTION &&
          act.resourcePattern.resourceType == ResourceType.CLUSTER
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = false
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val kafkaApis = createKafkaApis(authorizer = Some(mockAuthorizer))
    try {
      try kafkaApis.handleLeaderAndIsrRequest(request)
      catch {
        case e: ClusterAuthorizationException => validateClusterAuthorizationExceptionMessage(e)
      }
      verifyNoMoreInteractions(replicaManager)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrRaftShouldNeverReceive(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.LEADER_AND_ISR.oldestVersion(), ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 12)
    val brokerEpochInRequest = data.consumeLong(1L, 1L << 34)
    val topicName = safeTopicName(data, "fuzz-lai-raft-never")

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = false
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val kafkaApis = createKafkaApis(raftSupport = true)
    try {
      try kafkaApis.handleLeaderAndIsrRequest(request)
      catch {
        case e: UnsupportedVersionException => validateUnsupportedVersionShouldNeverReceiveLeaderAndIsrMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrKRaftControllerMissingLifecycleManager(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(7, ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 14)
    val brokerEpochInRequest = data.consumeLong(100L, 1L << 33)
    val topicName = safeTopicName(data, "fuzz-lai-kraft-flag")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(1L)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = true
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleLeaderAndIsrRequest(request)
      catch {
        case e: IllegalStateException => validateIllegalStateMissingBrokerLifecycleManagerMessage(e)
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrKRaftControllerUnknownBrokerEpoch(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(7, ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 16)
    val topicName = safeTopicName(data, "fuzz-lai-kraft-unknown")

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(999L)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version,
      controllerId,
      controllerEpoch,
      AbstractControlRequest.UNKNOWN_BROKER_EPOCH,
      topicName,
      kraftController = true
    ).build(version)
    val request = buildRequest(leaderAndIsrRequest)

    val response = new LeaderAndIsrResponse(
      new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code),
      version
    )
    when(replicaManager.becomeLeaderOrFollower(
      ArgumentMatchers.eq(request.context.correlationId),
      any(),
      any()
    )).thenReturn(response)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleLeaderAndIsrRequest(request)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestLeaderAndIsrForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.LEADER_AND_ISR.oldestVersion(), ApiKeys.LEADER_AND_ISR.latestVersion())
    val controllerId = data.consumeInt(0, 5)
    val controllerEpoch = data.consumeInt(1, 30)
    val currentBrokerEpoch = data.consumeLong(3000L, 1L << 37)
    val brokerEpochInRequest = data.consumeLong(currentBrokerEpoch, currentBrokerEpoch + 200L)
    val splitSize = data.consumeInt(1, 32)
    val fuzzTail = data.consumeRemainingAsBytes()
    val topicSuffix = new String(fuzzTail.take(splitSize)).replaceAll("[^a-zA-Z0-9._-]", "_").take(80)
    val topicName = s"fuzz-lai-fwd-$topicSuffix".take(200)

    resetZkLeaderAndIsrHarness()
    when(controller.brokerEpoch).thenReturn(currentBrokerEpoch)

    val leaderAndIsrRequest = newLeaderAndIsrBuilder(
      version, controllerId, controllerEpoch, brokerEpochInRequest, topicName, kraftController = false
    ).build(version)
    val request = buildForwardedRequest(leaderAndIsrRequest)

    val response = new LeaderAndIsrResponse(
      new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code),
      version
    )
    when(replicaManager.becomeLeaderOrFollower(
      ArgumentMatchers.eq(request.context.correlationId),
      any(),
      any()
    )).thenReturn(response)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleLeaderAndIsrRequest(request)
    finally kafkaApis.close()
  }
}
