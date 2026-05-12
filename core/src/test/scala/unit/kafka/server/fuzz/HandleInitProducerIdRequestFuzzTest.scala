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
import kafka.coordinator.transaction.InitProducerIdResult
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, RequestLocal}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.InitProducerIdRequest
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyInt, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleInitProducerIdRequest`.
 *
 * Covers transactional-id WRITE authorization, idempotent (null txn id)
 * cluster vs topic-by-type authorization, invalid producer id/epoch pairs,
 * `txnCoordinator.handleInitProducerId` success and `PRODUCER_FENCED` remapping
 * for clients below version 4, throttling, and forwarded inner requests.
 */
class HandleInitProducerIdRequestFuzzTest extends KafkaApisTest {

  /**
   * Wire versions below 3 omit producer id/epoch on the wire; `InitProducerIdRequestData` must keep
   * defaults (-1) for lower versions or request serialization throws. Fuzz cases that set non-default
   * id/epoch (or coordinator results with non-default epoch in the response path) therefore use v3+.
   */
  private def minInitProducerIdVersionWithProducerIdFields: Short =
    math.max(3, ApiKeys.INIT_PRODUCER_ID.oldestVersion().toInt).toShort

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val value = data.consumeString(64)
    if (value == null || value.isEmpty) fallback else value
  }

  private def stubNoThrottle(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  /** Denies only `WRITE` on `TRANSACTIONAL_ID`; other actions are allowed. */
  private def authorizerDenyTransactionalIdWrite(): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.WRITE &&
          act.resourcePattern.resourceType == ResourceType.TRANSACTIONAL_ID
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    when(a.authorizeByResourceType(any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(AuthorizationResult.ALLOWED)
    a
  }

  /**
   * For null transactional id: optionally deny `IDEMPOTENT_WRITE` on `CLUSTER`, and
   * independently control `authorizeByResourceType(WRITE, TOPIC)`.
   */
  private def authorizerClusterIdempotentAndTopicByType(
    denyClusterIdempotentWrite: Boolean,
    allowTopicByResourceType: Boolean
  ): Authorizer = {
    val a = mock(classOf[Authorizer])
    when(a.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = denyClusterIdempotentWrite &&
          act.operation == AclOperation.IDEMPOTENT_WRITE &&
          act.resourcePattern.resourceType == ResourceType.CLUSTER
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    when(a.authorizeByResourceType(
      any(),
      ArgumentMatchers.eq(AclOperation.WRITE),
      ArgumentMatchers.eq(ResourceType.TOPIC)
    )).thenReturn(
      if (allowTopicByResourceType) AuthorizationResult.ALLOWED else AuthorizationResult.DENIED
    )
    a
  }

  private def stubTxnCoordinatorInvoke(result: InitProducerIdResult): Unit = {
    when(txnCoordinator.handleInitProducerId(
      any(),
      anyInt(),
      any(),
      any(),
      any()
    )).thenAnswer(invocation => {
      val cb = invocation.getArgument(3).asInstanceOf[InitProducerIdResult => Unit]
      cb(result)
    })
  }

  /** `WRITE` on `TRANSACTIONAL_ID` denied => `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdTransactionalIdAuthorizationDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.INIT_PRODUCER_ID.oldestVersion(),
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val txnId = safeString(data, "fuzz-init-txn-deny")

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(txnId)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTransactionalIdWrite()))
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  /**
   * Null transactional id: `IDEMPOTENT_WRITE` on cluster denied and
   * `authorizeByResourceType(WRITE, TOPIC)` denied => `CLUSTER_AUTHORIZATION_FAILED`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdClusterAuthorizationDeniedNoTransactionalId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      ApiKeys.INIT_PRODUCER_ID.oldestVersion(),
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(null)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerClusterIdempotentAndTopicByType(
      denyClusterIdempotentWrite = true,
      allowTopicByResourceType = false
    )))
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  /**
   * Null transactional id: cluster `IDEMPOTENT_WRITE` denied but `WRITE` on `TOPIC`
   * by resource type allowed => `txnCoordinator.handleInitProducerId` runs.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdClusterDeniedTopicWriteByTypeAllowed(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      minInitProducerIdVersionWithProducerIdFields,
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val producerId = data.consumeLong(1L, 1L << 20)
    val producerEpoch = data.consumeShort(0, Short.MaxValue)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(null)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()
    stubTxnCoordinatorInvoke(InitProducerIdResult(producerId, producerEpoch, Errors.NONE))

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerClusterIdempotentAndTopicByType(
      denyClusterIdempotentWrite = true,
      allowTopicByResourceType = true
    )))
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  /** Invalid `(producerId, producerEpoch)` pairs => `INVALID_REQUEST` without coordinator. */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdInvalidProducerIdOrEpochCombination(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      minInitProducerIdVersionWithProducerIdFields,
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val idOnlyInvalid = data.consumeBoolean()
    val txnId = safeString(data, "fuzz-init-inv")

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(txnId)
      .setTransactionTimeoutMs(timeoutMs)
    if (idOnlyInvalid) {
      reqData.setProducerId(10L).setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    } else {
      reqData.setProducerId(RecordBatch.NO_PRODUCER_ID).setProducerEpoch(data.consumeShort(1, 100))
    }
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  /** Happy path: coordinator returns `NONE` with a known producer id / epoch. */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdTxnCoordinatorSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      minInitProducerIdVersionWithProducerIdFields,
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val txnId = safeString(data, "fuzz-init-ok")
    val existingPid = data.consumeLong(100L, 1L << 22)
    val existingEpoch = data.consumeShort(1, 200)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(txnId)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(existingPid)
      .setProducerEpoch(existingEpoch)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()
    stubTxnCoordinatorInvoke(InitProducerIdResult(existingPid, existingEpoch, Errors.NONE))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  /**
   * `PRODUCER_FENCED` is mapped to `INVALID_PRODUCER_EPOCH` for `InitProducerId` wire
   * versions below 4; v4+ keeps `PRODUCER_FENCED`.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdProducerFencedVersionRemap(data: FuzzedDataProvider): Unit = {
    val forceOldClient = data.consumeBoolean()
    val latest = ApiKeys.INIT_PRODUCER_ID.latestVersion()
    val minV = minInitProducerIdVersionWithProducerIdFields
    val version = if (forceOldClient) {
      val hi = math.min(3, latest.toInt).toShort
      if (minV <= hi) data.consumeShort(minV, hi)
      else data.consumeShort(minV, latest)
    } else {
      val lo = math.max(4, minV.toInt).toShort
      if (lo <= latest) data.consumeShort(lo, latest)
      else data.consumeShort(minV, latest)
    }
    val timeoutMs = data.consumeInt(1, 900_000)
    val txnId = safeString(data, "fuzz-init-fenced")
    val producerId = data.consumeLong(50L, 1L << 20)
    val producerEpoch = data.consumeShort(0, 50)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(txnId)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    stubNoThrottle()
    stubTxnCoordinatorInvoke(InitProducerIdResult(producerId, producerEpoch, Errors.PRODUCER_FENCED))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      minInitProducerIdVersionWithProducerIdFields,
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val throttleMs = data.consumeInt(1, 500)
    val txnId = safeString(data, "fuzz-init-throttle")
    val producerId = data.consumeLong(1L, 1L << 18)
    val producerEpoch = data.consumeShort(0, 10)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(txnId)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    stubTxnCoordinatorInvoke(InitProducerIdResult(producerId, producerEpoch, Errors.NONE))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestInitProducerIdForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(
      minInitProducerIdVersionWithProducerIdFields,
      ApiKeys.INIT_PRODUCER_ID.latestVersion())
    val timeoutMs = data.consumeInt(1, 900_000)
    val throttleMs = data.consumeInt(0, 200)
    val producerId = data.consumeLong(1L, 1L << 17)
    val producerEpoch = data.consumeShort(0, 5)

    val reqData = new InitProducerIdRequestData()
      .setTransactionalId(null)
      .setTransactionTimeoutMs(timeoutMs)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
    val built = new InitProducerIdRequest.Builder(reqData).build(version)
    val request = buildForwardedRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel,
      txnCoordinator, groupCoordinator)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    stubTxnCoordinatorInvoke(InitProducerIdResult(producerId, producerEpoch, Errors.NONE))

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleInitProducerIdRequest(request, requestLocal)
    finally kafkaApis.close()
  }
}
