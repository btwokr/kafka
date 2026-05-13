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
import org.apache.kafka.common.message.EndTxnRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{EndTxnRequest, TransactionResult}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleEndTxnRequest`.
 *
 * Covers transactional-id WRITE authorization, commit vs abort `TransactionResult`
 * wiring, coordinator callback success and `PRODUCER_FENCED` remapping for legacy
 * wire versions, other coordinator errors, throttling, forwarded inner requests, and
 * `ensureInterBrokerVersion` (`UnsupportedVersionException` with message check).
 */
class HandleEndTxnRequestFuzzTest extends KafkaApisTest {

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

  private def resetEndTxnHarness(): Unit = {
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

  private def stubTxnCoordinatorEndTransactionCompletesWith(error: Errors): Unit = {
    when(txnCoordinator.handleEndTransaction(
      anyString(), anyLong(), anyShort(), any(), any(), any()
    )).thenAnswer(invocation => {
      val onComplete = invocation.getArgument(4).asInstanceOf[Errors => Unit]
      onComplete(error)
    })
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnSuccessCommit(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-endtxn-commit")
    val producerId = data.consumeLong(1L, 1L << 40)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort

    resetEndTxnHarness()
    stubNoThrottle()

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(true)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try {
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(TransactionResult.COMMIT),
        any(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(invocation => {
        val onComplete = invocation.getArgument(4).asInstanceOf[Errors => Unit]
        onComplete(Errors.NONE)
      })
      kafkaApis.handleEndTxnRequest(request, requestLocal)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnSuccessAbort(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-endtxn-abort")
    val producerId = data.consumeLong(1L, 1L << 39)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort

    resetEndTxnHarness()
    stubNoThrottle()

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(false)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.NoCaching

    val kafkaApis = createKafkaApis()
    try {
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(producerEpoch),
        ArgumentMatchers.eq(TransactionResult.ABORT),
        any(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(invocation => {
        val onComplete = invocation.getArgument(4).asInstanceOf[Errors => Unit]
        onComplete(Errors.NONE)
      })
      kafkaApis.handleEndTxnRequest(request, requestLocal)
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnTransactionalIdWriteDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-endtxn-deny")
    val producerId = data.consumeLong(1L, 1L << 38)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    stubNoThrottle()

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTransactionalIdWrite()))
    try kafkaApis.handleEndTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnProducerFencedLegacyClient(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), 1.toShort)
    val transactionalId = safeString(data, "fuzz-endtxn-fence-old")
    val producerId = data.consumeLong(1L, 1L << 37)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    stubNoThrottle()
    stubTxnCoordinatorEndTransactionCompletesWith(Errors.PRODUCER_FENCED)

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleEndTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnProducerFencedModernClient(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(2.toShort, ApiKeys.END_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-endtxn-fence-new")
    val producerId = data.consumeLong(1L, 1L << 36)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    stubNoThrottle()
    stubTxnCoordinatorEndTransactionCompletesWith(Errors.PRODUCER_FENCED)

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleEndTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnCoordinatorConcurrentTransactions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-endtxn-conc")
    val producerId = data.consumeLong(1L, 1L << 35)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    stubNoThrottle()
    stubTxnCoordinatorEndTransactionCompletesWith(Errors.CONCURRENT_TRANSACTIONS)

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.NoCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleEndTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val throttleMs = data.consumeInt(1, 200)
    val transactionalId = safeString(data, "fuzz-endtxn-thr")
    val producerId = data.consumeLong(1L, 1L << 34)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    stubTxnCoordinatorEndTransactionCompletesWith(Errors.NONE)

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleEndTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.END_TXN.oldestVersion(), ApiKeys.END_TXN.latestVersion())
    val reqThrottleMs = data.consumeInt(0, 80)
    val transactionalId = safeString(data, "fuzz-endtxn-fwd")
    val producerId = data.consumeLong(1L, 1L << 33)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val committed = data.consumeBoolean()

    resetEndTxnHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)
    stubTxnCoordinatorEndTransactionCompletesWith(Errors.NONE)

    val built = new EndTxnRequest.Builder(
      new EndTxnRequestData()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setCommitted(committed)
    ).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleEndTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestEndTxnUnsupportedInterBrokerVersion(data: FuzzedDataProvider): Unit = {
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
      try kafkaApis.handleEndTxnRequest(null, requestLocal)
      catch {
        case e: UnsupportedVersionException =>
          validateUnsupportedVersionInterBrokerGuardMessage(e)
      }
    } finally kafkaApis.close()
  }
}
