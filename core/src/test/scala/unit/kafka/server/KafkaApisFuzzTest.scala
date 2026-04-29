package unit.kafka.server

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.cluster.Partition
import kafka.network.RequestChannel
import kafka.server.{KafkaApisTest, RequestLocal}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, RequestContext}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort}
import org.mockito.Mockito.{mock, never, reset, verify, when}

import java.util
import java.util.Collections
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters.SeqHasAsJava

class KafkaApisFuzzTest extends KafkaApisTest {
  @FuzzTest(maxDuration = "100s")
  def fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower(data: FuzzedDataProvider): Unit = {
    // Arrange
    val version = data.consumeInt(10, ApiKeys.PRODUCE.latestVersion).toShort
    val newLeaderId = data.consumeInt()
    val newLeaderEpoch = data.consumeInt()
    val splitSize = data.consumeInt(10, 4096)
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val acks = data.consumeInt(0, 1).toShort
    val timeoutMs = data.consumeInt(0, 5000)

    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    val tp = new TopicPartition(topic, 0)
    val partition = mock(classOf[Partition])

    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
              .setName(tp.topic).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withRecords(compression, new SimpleRecord(record))))))
            .iterator))
        .setAcks(acks)
        .setTimeoutMs(timeoutMs))
      .build(version)
    val request = buildRequest(produceRequest)

    when(replicaManager.handleProduceAppend(anyLong,
      anyShort,
      ArgumentMatchers.eq(false),
      any(),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      any(),
      any())
    ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

    when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    val kafkaApis = createKafkaApis()

    // Act
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)
    } finally {
      kafkaApis.close()
    }
  }

  @FuzzTest(maxDuration = "100s")
  def fuzzTestTransactionalParametersSetCorrectly(data: FuzzedDataProvider): Unit = {
    val transactionalId = data.consumeString(5)
    val timeoutMs = data.consumeInt(0, 5000)
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val num = data.consumeInt(0, 1)
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)

    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, num)

    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
              .setName(tp.topic).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withTransactionalRecords(compression, num, num.toShort, num, new SimpleRecord(record))))))
            .iterator))
        .setAcks(num.toShort)
        .setTransactionalId(transactionalId)
        .setTimeoutMs(timeoutMs))
      .build(version)
    val request = buildRequest(produceRequest)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      verify(replicaManager).handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(transactionalId),
        any(),
        any(),
        any(),
        any(),
        any(),
        any())
    } finally {
      kafkaApis.close()
    }
  }

  @FuzzTest(maxDuration = "100s")
  def fuzzTestNullableTransactionalId(data: FuzzedDataProvider): Unit = {
    val timeoutMs = data.consumeInt(0, 5000)
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val num = data.consumeInt(0, 1)
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)

    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, num)

    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
              .setName(tp.topic).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withTransactionalRecords(compression, num, num.toShort, num, new SimpleRecord(record))))))
            .iterator))
        .setAcks(num.toShort)
        .setTransactionalId(null)
        .setTimeoutMs(timeoutMs))
      .build(version)
    val request = buildRequest(produceRequest)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      verify(replicaManager, never()).handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(null),
        any(),
        any(),
        any(),
        any(),
        any(),
        any())
    } finally {
      kafkaApis.close()
    }
  }

  @FuzzTest(maxDuration = "100s")
  def fuzzTestNoAuthorizedTransactionalRequest(data: FuzzedDataProvider): Unit = {
    val transactionalId = data.consumeString(5)
    val timeoutMs = data.consumeInt(0, 5000)
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val num = data.consumeInt(0, 1)
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)

    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, num)

    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
              .setName(tp.topic).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withTransactionalRecords(compression, num, num.toShort, num, new SimpleRecord(record))))))
            .iterator))
        .setAcks(num.toShort)
        .setTransactionalId(transactionalId)
        .setTimeoutMs(timeoutMs))
      .build(version)
    val request = buildRequest(produceRequest)
    val authorizer = mock(classOf[Authorizer])

    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      verify(replicaManager, never()).handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(transactionalId),
        any(),
        any(),
        any(),
        any(),
        any(),
        any())
    } finally {
      kafkaApis.close()
    }
  }

  @FuzzTest(maxDuration = "100s")
  def fuzzTestNoAuthorized(data: FuzzedDataProvider): Unit = {
    val timeoutMs = data.consumeInt(0, 5000)
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val num = data.consumeInt(0, 1)
    val acks = data.consumeInt(0, 1).toShort
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)

    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, num)

    val produceRequest = ProduceRequest.forCurrentMagic(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
              .setName(tp.topic).setPartitionData(Collections.singletonList(
                new ProduceRequestData.PartitionProduceData()
                  .setIndex(tp.partition)
                  .setRecords(MemoryRecords.withRecords(compression, new SimpleRecord(record))))))
            .iterator))
        .setAcks(acks)
        .setTimeoutMs(timeoutMs))
      .build(version)
    val request = buildRequest(produceRequest)

    val authorizer = mock(classOf[Authorizer])

    val kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      verify(replicaManager, never()).handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        ArgumentMatchers.eq(null),
        any(),
        any(),
        any(),
        any(),
        any(),
        any())
    } finally {
      kafkaApis.close()
    }
  }

  def helperSplitByteArray(byteArray: Array[Byte], splitSize: Int): (String, Array[Byte]) = {
    // Ensure the split size is valid
    require(splitSize >= 0, "Split size must be non-negative")

    // Split the byte array into two parts
    val (stringBytes, remainingBytes) = byteArray.splitAt(splitSize)

    // Convert the first part to a String
    val resultString = new String(stringBytes)

    // Return the result as a tuple
    (resultString, remainingBytes)
  }

  def buildCompression(input: Int, version: Int): Compression = {
    val compressionBuilder = input match {
      case 0 => Compression.none()
      case 1 => Compression.gzip()
      case 2 => Compression.snappy()
      case 3 => Compression.lz4()
      case 4 =>
        if (version >= 3) Compression.none()
        else Compression.zstd()
      case _ => throw new MatchError("Unreachable case")
    }
    compressionBuilder.build()
  }
}
