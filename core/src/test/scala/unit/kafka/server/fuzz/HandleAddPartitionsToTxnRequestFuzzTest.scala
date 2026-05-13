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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.{ClusterAuthorizationException, InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.MetadataVersion
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort, anyString}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleAddPartitionsToTxnRequest`.
 *
 * Covers client (v0&ndash;3) and broker (v4+) paths, transactional-id and
 * topic WRITE authorization, mixed known/unknown partitions with
 * `OPERATION_NOT_ATTEMPTED`, `txnCoordinator` add vs verify-only callbacks
 * (including `PRODUCER_FENCED` remapping for legacy clients), batched broker
 * transactions, cluster-action denial on v4+, null transactional id,
 * unsupported inter-broker metadata (`ensureInterBrokerVersion`),
 * throttling, and forwarded inner requests.
 */
class HandleAddPartitionsToTxnRequestFuzzTest extends KafkaApisTest {

  private def safeString(data: FuzzedDataProvider, fallback: String): String = {
    val fuzzString = data.consumeString(64)
    if (fuzzString == null || fuzzString.isEmpty) fallback
    else fuzzString
  }

  private def resetTxnHarness(): Unit = {
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

  private def authorizerDenyTopicWrite(deniedTopicNames: Set[String]): Authorizer = {
    val mockAuthorizer = mock(classOf[Authorizer])
    when(mockAuthorizer.authorize(any(), any[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      val out = new util.ArrayList[AuthorizationResult](actions.size)
      actions.forEach { act =>
        val denied = act.operation == AclOperation.WRITE &&
          act.resourcePattern.resourceType == ResourceType.TOPIC &&
          deniedTopicNames.contains(act.resourcePattern.name)
        out.add(if (denied) AuthorizationResult.DENIED else AuthorizationResult.ALLOWED)
      }
      out
    })
    mockAuthorizer
  }

  private def authorizerDenyClusterAction(): Authorizer = {
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

  private def stubTxnCoordinatorVerifyPartitionsCompletes(): Unit = {
    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      anyString(), anyLong(), anyShort(), any(), any()
    )).thenAnswer(invocation => {
      val txnIdArg = invocation.getArgument(0, classOf[String])
      val partitionSet = invocation.getArgument(3, classOf[scala.collection.Set[TopicPartition]])
      val onComplete = invocation.getArgument(4).asInstanceOf[
        org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult => Unit]
      onComplete(AddPartitionsToTxnResponse.resultForTransaction(
        txnIdArg,
        partitionSet.map(_ -> Errors.NONE).toMap.asJava))
    })
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientSuccess(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-client-ok")
    val producerId = data.consumeLong(1L, 1L << 40)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-t1")
    val partitionIndex1 = data.consumeInt(0, 3)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 4)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildRequest(built)
    val requestLocal = RequestLocal.withThreadConfinedCaching

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientTransactionalIdDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-txn-deny")
    val producerId = data.consumeLong(1L, 1L << 35)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-meta")
    val partitionIndex1 = data.consumeInt(0, 2)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 3)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTransactionalIdWrite()))
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientTopicWriteDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-topic-deny")
    val producerId = data.consumeLong(1L, 1L << 36)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-allow")
    val topic2 = safeString(data, "fuzz-aptxn-deny")
    val partitionIndex1 = data.consumeInt(0, 2)
    val partitionIndex2 = data.consumeInt(0, 2)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 3)
    addTopicToMetadataCache(topic2, numPartitions = 3)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val topicPartition2 = new TopicPartition(topic2, partitionIndex2)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1, topicPartition2).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyTopicWrite(Set(topic2))))
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientUnknownPartitionMixed(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-unk")
    val producerId = data.consumeLong(1L, 1L << 34)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-known")
    val knownPartitionIndex = data.consumeInt(0, 0)
    val unknownPartitionIndex = data.consumeInt(3, 9)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 1)

    val knownTopicPartition = new TopicPartition(topic1, knownPartitionIndex)
    val unknownTopicPartition = new TopicPartition(topic1, unknownPartitionIndex)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(knownTopicPartition, unknownTopicPartition).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientProducerFencedRemappedLegacy(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 1.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-fence-old")
    val producerId = data.consumeLong(1L, 1L << 33)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-fence-t")
    val partitionIndex1 = data.consumeInt(0, 1)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 2)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.PRODUCER_FENCED)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnClientProducerFencedModern(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(2.toShort, 3.toShort)
    val transactionalId = safeString(data, "fuzz-aptxn-fence-new")
    val producerId = data.consumeLong(1L, 1L << 32)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-fence-t2")
    val partitionIndex1 = data.consumeInt(0, 1)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 2)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.PRODUCER_FENCED)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnBrokerAddPartitions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4.toShort, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aptxn-br-add")
    val producerId = data.consumeLong(1L, 1L << 38)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-br-t")
    val partitionIndex1 = data.consumeInt(0, 2)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 3)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val topics = new AddPartitionsToTxnTopicCollection()
    topics.add(new AddPartitionsToTxnTopic()
      .setName(topic1)
      .setPartitions(List[Integer](partitionIndex1).asJava))
    val transactions = new AddPartitionsToTxnTransactionCollection()
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(false)
      .setTopics(topics))

    val built = AddPartitionsToTxnRequest.Builder.forBroker(transactions).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnBrokerVerifyOnly(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4.toShort, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aptxn-br-ver")
    val producerId = data.consumeLong(1L, 1L << 37)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-br-vt")
    val partitionIndex1 = data.consumeInt(0, 2)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 3)
    stubTxnCoordinatorVerifyPartitionsCompletes()

    val topics = new AddPartitionsToTxnTopicCollection()
    topics.add(new AddPartitionsToTxnTopic()
      .setName(topic1)
      .setPartitions(List[Integer](partitionIndex1).asJava))
    val transactions = new AddPartitionsToTxnTransactionCollection()
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(true)
      .setTopics(topics))

    val built = AddPartitionsToTxnRequest.Builder.forBroker(transactions).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnBrokerBatchedTwoTransactions(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4.toShort, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())
    val transactionalId1 = safeString(data, "fuzz-aptxn-b1")
    val transactionalId2 = safeString(data, "fuzz-aptxn-b2")
    val producerId = data.consumeLong(1L, 1L << 39)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-bt1")
    val topic2 = safeString(data, "fuzz-aptxn-bt2")
    val partitionIndex1 = data.consumeInt(0, 1)
    val partitionIndex2 = data.consumeInt(0, 1)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 2)
    addTopicToMetadataCache(topic2, numPartitions = 2)

    val topics1 = new AddPartitionsToTxnTopicCollection()
    topics1.add(new AddPartitionsToTxnTopic()
      .setName(topic1)
      .setPartitions(List[Integer](partitionIndex1).asJava))
    val topics2 = new AddPartitionsToTxnTopicCollection()
    topics2.add(new AddPartitionsToTxnTopic()
      .setName(topic2)
      .setPartitions(List[Integer](partitionIndex2).asJava))

    val transactions = new AddPartitionsToTxnTransactionCollection()
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId1)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(false)
      .setTopics(topics1))
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId2)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(true)
      .setTopics(topics2))

    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq(transactionalId1), anyLong(), anyShort(), any(), any(), any()
    )).thenAnswer(invocation => {
      val onComplete = invocation.getArgument(4).asInstanceOf[Errors => Unit]
      onComplete(Errors.NONE)
    })
    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      ArgumentMatchers.eq(transactionalId2), anyLong(), anyShort(), any(), any()
    )).thenAnswer(invocation => {
      val txnIdArg = invocation.getArgument(0, classOf[String])
      val partitionSet = invocation.getArgument(3, classOf[scala.collection.Set[TopicPartition]])
      val onComplete = invocation.getArgument(4).asInstanceOf[
        org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult => Unit]
      onComplete(AddPartitionsToTxnResponse.resultForTransaction(
        txnIdArg,
        partitionSet.map(_ -> Errors.NONE).toMap.asJava))
    })

    val built = AddPartitionsToTxnRequest.Builder.forBroker(transactions).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnBrokerClusterActionDenied(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4.toShort, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())
    val transactionalId = safeString(data, "fuzz-aptxn-cl-deny")
    val producerId = data.consumeLong(1L, 1L << 31)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-cl-t")
    val partitionIndex1 = data.consumeInt(0, 1)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 2)

    val topics = new AddPartitionsToTxnTopicCollection()
    topics.add(new AddPartitionsToTxnTopic()
      .setName(topic1)
      .setPartitions(List[Integer](partitionIndex1).asJava))
    val transactions = new AddPartitionsToTxnTransactionCollection()
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(transactionalId)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(false)
      .setTopics(topics))

    val built = AddPartitionsToTxnRequest.Builder.forBroker(transactions).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizerDenyClusterAction()))
    try {
      try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
      catch {
        case _: ClusterAuthorizationException =>
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnBrokerNullTransactionalId(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(4.toShort, ApiKeys.ADD_PARTITIONS_TO_TXN.latestVersion())
    val producerId = data.consumeLong(1L, 1L << 30)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-null-t")
    val partitionIndex1 = data.consumeInt(0, 1)

    resetTxnHarness()
    stubNoThrottle()
    addTopicToMetadataCache(topic1, numPartitions = 2)

    val topics = new AddPartitionsToTxnTopicCollection()
    topics.add(new AddPartitionsToTxnTopic()
      .setName(topic1)
      .setPartitions(List[Integer](partitionIndex1).asJava))
    val transactions = new AddPartitionsToTxnTransactionCollection()
    transactions.add(new AddPartitionsToTxnTransaction()
      .setTransactionalId(null)
      .setProducerId(producerId)
      .setProducerEpoch(producerEpoch)
      .setVerifyOnly(false)
      .setTopics(topics))

    val built = AddPartitionsToTxnRequest.Builder.forBroker(transactions).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try {
      try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
      catch {
        case _: InvalidRequestException =>
      }
    } finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnThrottledResponse(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val throttleMs = data.consumeInt(1, 200)
    val transactionalId = safeString(data, "fuzz-aptxn-thr")
    val producerId = data.consumeLong(1L, 1L << 29)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-thr-t")
    val partitionIndex1 = data.consumeInt(0, 2)

    resetTxnHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)
    addTopicToMetadataCache(topic1, numPartitions = 3)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnForwardedInnerRequest(data: FuzzedDataProvider): Unit = {
    val version = data.consumeShort(ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion(), 3.toShort)
    val reqThrottleMs = data.consumeInt(0, 80)
    val transactionalId = safeString(data, "fuzz-aptxn-fwd")
    val producerId = data.consumeLong(1L, 1L << 28)
    val producerEpoch = data.consumeShort(0, Short.MaxValue).toShort
    val topic1 = safeString(data, "fuzz-aptxn-fwd-t")
    val partitionIndex1 = data.consumeInt(0, 2)

    resetTxnHarness()
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(reqThrottleMs)
    addTopicToMetadataCache(topic1, numPartitions = 3)
    stubTxnCoordinatorAddPartitionsCompletesWith(Errors.NONE)

    val topicPartition1 = new TopicPartition(topic1, partitionIndex1)
    val built = AddPartitionsToTxnRequest.Builder.forClient(
      transactionalId, producerId, producerEpoch, List(topicPartition1).asJava).build(version)
    val request = buildForwardedRequest(built)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.NoCaching)
    finally kafkaApis.close()
  }

  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestAddPartitionsToTxnUnsupportedInterBrokerVersion(data: FuzzedDataProvider): Unit = {
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
      try kafkaApis.handleAddPartitionsToTxnRequest(null, requestLocal)
      catch {
        case _: UnsupportedVersionException =>
      }
    } finally kafkaApis.close()
  }
}
