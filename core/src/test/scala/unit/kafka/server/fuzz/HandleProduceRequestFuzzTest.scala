package unit.kafka.server.fuzz

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
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{ProduceRequest, RequestContext}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong, anyShort}
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}

import java.util
import java.util.Collections
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters.SeqHasAsJava

class HandleProduceRequestFuzzTest extends KafkaApisTest {
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

  // ------------------------------------------------------------------------
  // Additional fuzz tests targeting branches that the original 5 tests did
  // not cover in handleProduceRequest. See core/src/test/fuzz/README.md.
  // ------------------------------------------------------------------------

  /**
   * Targets line 635: topic is authorized but NOT present in the metadata
   * cache => UNKNOWN_TOPIC_OR_PARTITION branch.
   *
   * Compared with the existing fuzz tests, this one deliberately does NOT
   * call addTopicToMetadataCache(topic, ...).
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestUnknownTopicOrPartition(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val acks = data.consumeInt(0, 1).toShort
    val timeoutMs = data.consumeInt(0, 5000)
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, 0)

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

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Targets lines 691-697: throttling branch in sendResponseCallback.
   * Drives both the bandwidth-throttle (line 694) and request-throttle
   * (line 696) sub-branches by varying which mocked quota manager
   * returns the larger value.
   *
   * Also exercises line 718 (sendNoOpResponseExemptThrottle) by routing
   * a non-error PartitionResponse back through the response callback
   * with acks == 0.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestThrottlingAndAckZeroNoOp(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val timeoutMs = data.consumeInt(0, 5000)
    // Always acks == 0 so we can also exercise line 718 (sendNoOpResponseExemptThrottle).
    val acks: Short = 0
    // We want maxThrottleTimeMs > 0 (line 691) deterministically so line
    // 692 and the `bandwidthThrottle > requestThrottle` branch (line 694)
    // are exercised on every iteration. With acks == 0 the request quota
    // is forced to 0 by line 688, so we just need bandwidth >= 1.
    val bandwidthThrottle = data.consumeInt(1, 100)
    val requestThrottle   = data.consumeInt(0, 100)

    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, 0)
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

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

    // Reply to handleProduceAppend with a successful (no-error) response so
    // errorInResponse stays false and line 718 (sendNoOpResponseExemptThrottle)
    // is reached.
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
    ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NONE))))

    // Non-zero throttle returns => maxThrottleTimeMs > 0 (line 691).
    // Note: when acks == 0 the request quota is forced to 0 by line 688,
    // so to exercise line 696 we run a sibling test invocation below
    // (acks == 1) - controlled by the next test.
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(bandwidthThrottle)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(requestThrottle)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)
    } finally {
      kafkaApis.close()
    }
  }

  /**
   * Targets line 696 specifically: requestThrottle > bandwidthThrottle, so
   * sendResponseCallback takes the else branch of `if (bandwidthThrottle...
   * > requestThrottle...)`.
   *
   * Uses acks == 1 so the request quota is actually consulted (see the
   * branch on line 688) and the non-acks==0 send path on line 721 is also
   * exercised. Throttle values are derived deterministically from the
   * fuzz input so every iteration triggers line 696 regardless of the
   * input length.
   */
  @FuzzTest(maxDuration = FUZZ_DURATION)
  def fuzzTestRequestThrottleDominates(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(3, ApiKeys.PRODUCE.latestVersion).toShort
    val compression = buildCompression(data.consumeInt(0, 4), version)
    val splitSize = data.consumeInt(10, 4096)
    val timeoutMs = data.consumeInt(0, 5000)
    val acks: Short = 1
    // Fix bandwidth low and pick request strictly higher so the
    // `requestThrottle > bandwidthThrottle` branch always wins.
    val bandwidthThrottle = 1
    val requestThrottle   = data.consumeInt(2, 100)

    val (topic, record) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    addTopicToMetadataCache(topic, numPartitions = 2)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

    val tp = new TopicPartition(topic, 0)
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

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
    ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NONE))))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(bandwidthThrottle)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), any[Long])).thenReturn(requestThrottle)

    val kafkaApis = createKafkaApis()
    try {
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)
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
