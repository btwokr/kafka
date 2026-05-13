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
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, MetadataCache, RequestLocal, ZkBrokerEpochManager}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleAddOffsetsToTxnRequest`.
 *
 * Covers transactional-id WRITE and group READ authorization, coordinator
 * callback success and `PRODUCER_FENCED` remapping for legacy wire versions,
 * non-fenced coordinator errors, throttling, forwarded inner requests, and
 * `ensureInterBrokerVersion` (`UnsupportedVersionException` with message check).
 */
class HandleAddOffsetsToTxnRequestFuzzTest extends KafkaApisTest {

  /** Matches `KafkaApis.ensureInterBrokerVersion` for this test's cache vs required `IBP_0_11_0_IV0`. */
  private def validateUnsupportedVersionInterBrokerGuardMessage(e: UnsupportedVersionException): Unit = {
    val expectedMessage =
      s"metadata.version: ${MetadataVersion.IBP_0_10_2_IV0} is less than the required version: ${MetadataVersion.IBP_0_11_0_IV0}"
    if (e.getMessage != expectedMessage) throw e
  }

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(64)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def resetOffsetsHarness(): Unit = {
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

  private def authorizerDenyTransactionalIdWrite(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.WRITE &&
          act.resourcePattern.resourceType == ResourceType.TRANSACTIONAL_ID
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def authorizerDenyGroupRead(): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.READ &&
          act.resourcePattern.resourceType == ResourceType.GROUP
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def stubTxnCoordinatorAddPartitionsCompletesWith(error: Errors): Unit = {
    when(txnCoordinator.handleAddPartitionsToTransaction(
      anyString(), anyLong(), anyShort(), any(), any(), any()
    )).thenAnswer(invocation => {
      val onComplete = invocation.getArgument(4).asInstanceOf[Errors => Unit]
      onComplete(error)
    })
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aotxn-ok")
    val groupId = safeString(data, "fuzz-aotxn-group")
    val producerId = data.consumeLong(1L, 1L << 40)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 49)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnTransactionalIdWriteDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aotxn-txn-deny")
    val groupId = safeString(data, "fuzz-aotxn-g1")
    val producerId = data.consumeLong(1L, 1L << 35)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 11)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTransactionalIdWrite()))
    try kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnGroupReadDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aotxn-g-allow")
    val groupId = safeString(data, "fuzz-aotxn-g-deny")
    val producerId = data.consumeLong(1L, 1L << 34)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 11)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyGroupRead()))
    try kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnProducerFencedLegacyClient(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), 1.toShort)
    val transactionalId = safeString(data, "fuzz-aotxn-fence-old")
    val groupId = safeString(data, "fuzz-aotxn-fence-g")
    val producerId = data.consumeLong(1L, 1L << 33)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 7)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.PRODUCER_FENCED)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnProducerFencedModernClient(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(2.toShort, ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aotxn-fence-new")
    val groupId = safeString(data, "fuzz-aotxn-fence-g2")
    val producerId = data.consumeLong(1L, 1L << 32)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 7)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.PRODUCER_FENCED)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnCoordinatorConcurrentTransactions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aotxn-conc")
    val groupId = safeString(data, "fuzz-aotxn-conc-g")
    val producerId = data.consumeLong(1L, 1L << 31)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 5)

    resetOffsetsHarness()
    stubNoThrottle()
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.CONCURRENT_TRANSACTIONS)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.NoCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val transactionalId = safeString(data, "fuzz-aotxn-thr")
    val groupId = safeString(data, "fuzz-aotxn-thr-g")
    val producerId = data.consumeLong(1L, 1L << 30)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 9)

    resetOffsetsHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(), ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion())
    val reqThrottleMs = data.consumeInt(0, 80)
    val transactionalId = safeString(data, "fuzz-aotxn-fwd")
    val groupId = safeString(data, "fuzz-aotxn-fwd-g")
    val producerId = data.consumeLong(1L, 1L << 29)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val coordinatorPartition = data.consumeInt(0, 9)

    resetOffsetsHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)
    when(groupCoordinator.partitionFor(ArgumentMatchers.eq(groupId))).thenReturn(coordinatorPartition)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val built = new AddOffsetsToTxnRequest.Builder(
      new AddOffsetsToTxnRequestData()
        .setGroupId(groupId)
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
    ).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddOffsetsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddOffsetsToTxnUnsupportedInterBrokerVersion(data: FuzzedDataProvider): Unit = {
    val useNoCaching = data.consumeBoolean()

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator, controller, adminManager)
    metadataCache = MetadataCache.zkMetadataCache(brokerId, MetadataVersion.IBP_0_10_2_IV0)
    brokerEpochManager = new ZkBrokerEpochManager(metadataCache, controller, None)

    val requestLocal =
      if (useNoCaching) RequestLocal.NoCaching
      else RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis(MetadataVersion.IBP_0_10_2_IV0)
    try {
      try kafkaApis.handleAddOffsetsToTxnRequest(null, requestLocal)
      catch {
        case e: UnsupportedVersionException =>
          validateUnsupportedVersionInterBrokerGuardMessage(e)
      }
    } finally kafkaApis.close()
  }
}
