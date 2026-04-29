package unit.kafka.server

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.cluster.Partition
import kafka.network.RequestChannel
import kafka.server.{FetchSessionCacheShard, FullFetchContext, KafkaApisTest, ReplicaQuota}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, FetchRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.storage.internals.log.{FetchParams, FetchPartitionData, LogConfig}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.{mock, reset, when}

import java.util
import java.util.{Collections, Optional, OptionalInt, OptionalLong, Properties}
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleFetchRequest`.
 *
 * Each test below stubs `replicaManager.fetchMessages` (and `getLogConfig`)
 * plus `fetchManager.newContext` to feed a deterministic fetch context back
 * into the method, then uses `FuzzedDataProvider` to vary parameters that
 * drive different branches of `handleFetchRequest` and its closures
 * (`maybeConvertFetchedData`, `processResponseCallback`).
 *
 * The goal is to maximise reachable branch coverage and let Jazzer surface
 * any reproducible crash; if Jazzer finds one, the test JVM exits with a
 * `crash-...` artefact under `core/`.
 */
class KafkaApisFetchFuzzTest extends KafkaApisTest {

  // ------------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------------

  /**
   * Build a deterministic fetch context that maps a single topic-id partition
   * to a fetch request entry. Returns the context plus the (resolved /
   * unresolved) TopicIdPartition that will appear in the response.
   *
   * If `withNullTopicName` is true, the context will return a TopicIdPartition
   * whose topic name is `null` (mirrors the unresolved-topic-id case in
   * production fetch sessions).
   */
  private def buildSingleTpFetchContext(topic: String,
                                        topicId: Uuid,
                                        partition: Int,
                                        isFollower: Boolean,
                                        withNullTopicName: Boolean = false): (FullFetchContext, TopicIdPartition) = {
    val resolvedTip = new TopicIdPartition(topicId, new TopicPartition(topic, partition))
    val tipInContext =
      if (withNullTopicName) new TopicIdPartition(topicId, new TopicPartition(null, partition))
      else resolvedTip
    val fetchData: util.Map[TopicIdPartition, FetchRequest.PartitionData] =
      Collections.singletonMap(tipInContext,
        new FetchRequest.PartitionData(topicId, 0L, 0L, 1024, Optional.empty[Integer]()))
    val ctx = new FullFetchContext(
      Time.SYSTEM,
      new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0),
      fetchData,
      /* usesTopicIds = */ true,
      isFollower)
    (ctx, tipInContext)
  }

  /** Stub `fetchManager.newContext(...)` to return the supplied ctx. */
  private def stubFetchManager(ctx: FullFetchContext): Unit = {
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(ctx)
  }

  /** Default no-throttle stubbing for both quota managers. */
  private def stubNoThrottling(): Unit = {
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)
  }

  /**
   * Stub `replicaManager.fetchMessages` to invoke the response callback
   * with a single `FetchPartitionData` carrying the supplied error and
   * (optionally) a `divergingEpoch` payload.
   */
  private def stubFetchMessages(tip: TopicIdPartition,
                                error: Errors,
                                records: MemoryRecords,
                                hw: Long,
                                logStartOffset: Long,
                                isReassignmentFetch: Boolean): Unit = {
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]())
    ).thenAnswer { inv =>
      val cb = inv.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      cb(Seq(tip -> new FetchPartitionData(
        error, hw, logStartOffset, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(),
        OptionalInt.empty(), isReassignmentFetch)))
    }
  }

  private def buildFetchRequestPayload(tip: TopicIdPartition,
                                       version: Short,
                                       replicaId: Int,
                                       maxBytes: Int,
                                       minBytes: Int,
                                       maxWait: Int): RequestChannel.Request = {
    val builder = Map(tip.topicPartition() ->
      new FetchRequest.PartitionData(tip.topicId(), 0L, 0L, 1024, Optional.empty[Integer]())).asJava
    val replicaEpoch = if (replicaId < 0) -1 else 1
    val req = new FetchRequest.Builder(
      version, version, replicaId, replicaEpoch, maxWait, minBytes, builder)
      .setMaxBytes(maxBytes)
      .metadata(new JFetchMetadata(0, 0))
      .build()
    buildRequest(req)
  }

  private def withKafkaApis[T](authorizer: Option[Authorizer] = None)(body: kafka.server.KafkaApis => T): T = {
    val k = createKafkaApis(authorizer = authorizer)
    try body(k) finally k.close()
  }

  // ------------------------------------------------------------------------
  // Fuzz tests
  // ------------------------------------------------------------------------

  /**
   * Consumer fetch path (non-follower). Drives:
   *   - the !isFromFollower partition-collection block (lines 798-813),
   *   - line 808 (TOPIC_AUTHORIZATION_FAILED) when the authorizer denies,
   *   - line 810 (UNKNOWN_TOPIC_OR_PARTITION) when the topic is absent
   *     from the metadata cache,
   *   - line 812 / line 1058+ (interesting-not-empty path with
   *     replicaManager.fetchMessages),
   *   - line 1032 (interesting.isEmpty path) when authorization is denied,
   *   - the maybeConvertFetchedData magic ladder (lines 850-857) by
   *     varying the FetchRequest version,
   *   - the ZSTD-on-old-version branch (line 833) when log config has
   *     ZSTD compression,
   *   - line 822 (KAFKA_STORAGE_ERROR -> NOT_LEADER_OR_FOLLOWER) when
   *     replicaManager returns KAFKA_STORAGE_ERROR for a v<=5 fetch,
   *   - lines 883-884 (UnsupportedCompressionTypeException catch in
   *     maybeConvertFetchedData) when ZSTD-compressed records are
   *     down-converted to magic v0/v1 for a v<=3 fetch.
   *
   * The "null topic in fetch context => UNKNOWN_TOPIC_ID" sub-branch
   * (lines 800-801) is exercised by `fuzzTestFetchEmptyInteresting` so
   * that this test can stay focused on the regular consumer paths.
   * Putting that branch in the per-iteration `mode` rotation here causes
   * Jazzer to surface a known NPE in handleFetchRequest within ~10 runs
   * (see `KafkaApisHandleFetchRequestNpeReproducerTest`), which would
   * stop the rest of this test from being explored.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestFetchConsumer(data: FuzzedDataProvider): Unit = {
    val mode = data.consumeInt(0, 6)
    // For modes 5 (KAFKA_STORAGE_ERROR + v<=5 mapping) and 6
    // (UnsupportedCompressionTypeException + v<=3 down-conversion) we pin
    // the FetchRequest version low so the targeted branches are reached
    // on every iteration; otherwise we let the fuzzer pick.
    val version: Short = mode match {
      case 5 => data.consumeInt(2, 5).toShort
      case 6 => data.consumeInt(2, 3).toShort
      case _ => data.consumeInt(2, ApiKeys.FETCH.latestVersion).toShort
    }
    val maxBytes = data.consumeInt(1, 1024 * 1024)
    val minBytes = data.consumeInt(0, 1024)
    val maxWait = data.consumeInt(0, 5000)
    val splitSize = data.consumeInt(10, 4096)
    val (rawTopic, recordBytes) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    // Topic name must be non-empty for FetchRequest builder; fall back to
    // a constant if the fuzzer hands us an empty string.
    val topic = if (rawTopic.isEmpty) "fuzz-topic" else rawTopic
    val topicId = Uuid.randomUuid()
    val partition = 0
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, partition))

    reset(replicaManager, fetchManager, clientQuotaManager, clientRequestQuotaManager, requestChannel)

    // Decide which sub-branch to drive based on `mode`.
    //   0 -> happy path (topic in cache, no authorizer, NONE)
    //   1 -> deny authorizer => TOPIC_AUTHORIZATION_FAILED
    //   2 -> topic NOT in metadata cache => UNKNOWN_TOPIC_OR_PARTITION
    //   3 -> happy path with NOT_LEADER_OR_FOLLOWER on v16+
    //   4 -> happy path with ZSTD log config
    //   5 -> KAFKA_STORAGE_ERROR on v<=5 (covers line 822)
    //   6 -> ZSTD-compressed records down-converted on v<=3
    //        => UnsupportedCompressionTypeException catch (lines 883-884)
    val isDenyAuth     = mode == 1
    val isAbsent       = mode == 2
    val notLeader      = mode == 3
    val zstdConfig     = mode == 4
    val storageError   = mode == 5
    val zstdRecords    = mode == 6
    val isNullTopic    = false

    if (!isAbsent) {
      addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3, topicId = topicId)
    }

    val (ctx, tipInContext) = buildSingleTpFetchContext(
      topic, topicId, partition, isFollower = false, withNullTopicName = isNullTopic)
    stubFetchManager(ctx)

    // LogConfig: pick something sensible per mode. For mode 6 we want the
    // on-disk message format to be magic >= V2 so the down-convert ladder
    // (lines 850-857) returns Some(MAGIC_VALUE_V0) for fetch v<=1 (or
    // MAGIC_VALUE_V1 for v<=3), forcing maybeConvertFetchedData into the
    // try block at line 866 where the LazyDownConversionRecords
    // constructor will throw for ZSTD-compressed records.
    val logConfigProps = new Properties()
    if (zstdConfig) logConfigProps.put("compression.type", BrokerCompressionType.ZSTD.name)
    val logConfig = LogConfig.fromProps(Collections.emptyMap(), logConfigProps)
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))

    val mockedPartition = mock(classOf[Partition])
    when(replicaManager.getPartitionOrError(any[TopicPartition])).thenAnswer(_ => Right(mockedPartition))
    when(mockedPartition.leaderReplicaIdOpt).thenReturn(Some(2))
    when(mockedPartition.getLeaderEpoch).thenReturn(5)

    // Build records. Mode 6 needs ZSTD-compressed records so the
    // LazyDownConversionRecords constructor throws
    // UnsupportedCompressionTypeException (RecordsUtil.downConvert refuses
    // to down-convert zstd batches to magic v0/v1).
    val recordCompression =
      if (zstdRecords) Compression.zstd().build()
      else Compression.NONE
    val payload =
      if (zstdRecords && recordBytes.isEmpty) "fuzz-record-payload".getBytes
      else recordBytes
    val records = if (payload.isEmpty) MemoryRecords.EMPTY
      else MemoryRecords.withRecords(recordCompression, new SimpleRecord(payload))

    val err =
      if (storageError) Errors.KAFKA_STORAGE_ERROR
      else if (notLeader && version >= 16) Errors.NOT_LEADER_OR_FOLLOWER
      else Errors.NONE
    stubFetchMessages(tipInContext, err, records, hw = 3L, logStartOffset = 0L,
      isReassignmentFetch = false)

    stubNoThrottling()

    val authorizer: Option[Authorizer] = if (isDenyAuth) {
      val a = mock(classOf[Authorizer])
      when(a.authorize(any(), any[util.List[Action]])).thenAnswer { _ =>
        Seq(AuthorizationResult.DENIED).asJava
      }
      Some(a)
    } else None

    val request = buildFetchRequestPayload(tip, version, replicaId = -1, maxBytes, minBytes, maxWait)
    withKafkaApis(authorizer = authorizer) { k =>
      k.handleFetchRequest(request)
    }
  }

  /**
   * Follower fetch path. Drives:
   *   - the isFromFollower branch with CLUSTER_ACTION authorized
   *     (lines 783-790, including the null-topic and metadata-miss
   *     sub-branches),
   *   - the isFromFollower branch with CLUSTER_ACTION denied (lines 791-794),
   *   - the follower send path (lines 985-991).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestFetchFollower(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(4, ApiKeys.FETCH.latestVersion).toShort
    val mode = data.consumeInt(0, 3)
    val maxBytes = data.consumeInt(1, 1024 * 1024)
    val minBytes = data.consumeInt(0, 1024)
    val maxWait = data.consumeInt(0, 5000)
    val splitSize = data.consumeInt(10, 4096)
    val (rawTopic, recordBytes) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val topic = if (rawTopic.isEmpty) "fuzz-follower-topic" else rawTopic
    val topicId = Uuid.randomUuid()
    val partition = 0
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, partition))

    reset(replicaManager, fetchManager, clientQuotaManager, clientRequestQuotaManager,
      requestChannel)

    // mode:
    //   0 -> auth ALLOWED + topic in cache => NONE response
    //   1 -> auth ALLOWED + null topic in ctx => UNKNOWN_TOPIC_ID
    //   2 -> auth ALLOWED + topic missing from cache => UNKNOWN_TOPIC_OR_PARTITION
    //   3 -> auth DENIED => TOPIC_AUTHORIZATION_FAILED
    val nullTopic = mode == 1
    val absent    = mode == 2
    val deny      = mode == 3

    if (!absent) {
      addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3, topicId = topicId)
    }

    val (ctx, tipInContext) = buildSingleTpFetchContext(
      topic, topicId, partition, isFollower = true, withNullTopicName = nullTopic)
    stubFetchManager(ctx)

    // For the follower path, replicaManager.getLogConfig is consulted by
    // maybeConvertFetchedData. Provide a default v2-magic config so the
    // None-magic branch (line 887) is taken.
    val logConfig = LogConfig.fromProps(Collections.emptyMap(), new Properties())
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))

    val records = if (recordBytes.isEmpty) MemoryRecords.EMPTY
      else MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(recordBytes))
    stubFetchMessages(tipInContext, Errors.NONE, records, hw = 7L, logStartOffset = 0L,
      isReassignmentFetch = data.consumeBoolean())

    stubNoThrottling()

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val authResult =
      if (deny) AuthorizationResult.DENIED
      else AuthorizationResult.ALLOWED
    when(authorizer.authorize(any(), any[util.List[Action]])).thenAnswer { _ =>
      Seq(authResult).asJava
    }

    val request = buildFetchRequestPayload(tip, version, replicaId = 1, maxBytes, minBytes, maxWait)
    withKafkaApis(authorizer = Some(authorizer)) { k =>
      k.handleFetchRequest(request)
    }
  }

  /**
   * Drives the throttling block in processResponseCallback (lines 1006-1017),
   * including both `bandwidthThrottle > requestThrottle` (line 1012) and
   * the else branch (line 1014). Uses a non-follower request so the
   * non-follower send path (lines 992-1028) is exercised.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestFetchThrottling(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(2, ApiKeys.FETCH.latestVersion).toShort
    // 0 -> bandwidth dominates, 1 -> request dominates
    val dominant = data.consumeInt(0, 1)
    val bandwidthThrottle = if (dominant == 0) data.consumeInt(2, 100) else 1
    val requestThrottle = if (dominant == 0) 1 else data.consumeInt(2, 100)

    val splitSize = data.consumeInt(10, 4096)
    val (rawTopic, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val topic = if (rawTopic.isEmpty) "fuzz-throttle-topic" else rawTopic
    val topicId = Uuid.randomUuid()
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, 0))

    reset(replicaManager, fetchManager, clientQuotaManager, clientRequestQuotaManager, requestChannel)
    addTopicToMetadataCache(topic, numPartitions = 1, numBrokers = 1, topicId = topicId)

    val (ctx, tipInCtx) = buildSingleTpFetchContext(topic, topicId, 0, isFollower = false)
    stubFetchManager(ctx)

    val logConfig = LogConfig.fromProps(Collections.emptyMap(), new Properties())
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))

    stubFetchMessages(tipInCtx, Errors.NONE, MemoryRecords.EMPTY, hw = 0L, logStartOffset = 0L,
      isReassignmentFetch = false)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(bandwidthThrottle)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(requestThrottle)

    val request = buildFetchRequestPayload(tip, version,
      replicaId = -1, maxBytes = 1024, minBytes = 0, maxWait = 0)
    withKafkaApis() { k =>
      k.handleFetchRequest(request)
    }
  }

  /**
   * Drives the empty-interesting branch (line 1032) by sending a fetch
   * request whose only partition carries a null topic name in the fetch
   * context (=> erroneous, no interesting). This is similar to one of
   * the modes in `fuzzTestFetchConsumer` but isolated so Jazzer can
   * spend its time exploring the related branches without competing
   * with the other modes.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestFetchEmptyInteresting(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(2, ApiKeys.FETCH.latestVersion).toShort
    val maxBytes = data.consumeInt(1, 1024 * 1024)
    val minBytes = data.consumeInt(0, 1024)
    val maxWait = data.consumeInt(0, 5000)
    val splitSize = data.consumeInt(10, 4096)
    val (rawTopic, _) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val topic = if (rawTopic.isEmpty) "fuzz-empty-topic" else rawTopic
    val topicId = Uuid.randomUuid()
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, 0))

    reset(replicaManager, fetchManager, clientQuotaManager, clientRequestQuotaManager, requestChannel)
    addTopicToMetadataCache(topic, numPartitions = 1, numBrokers = 1, topicId = topicId)

    // Force the fetch context to surface a null-topic TopicIdPartition so
    // that the consumer block routes it into `erroneous` (line 801) and
    // `interesting` ends up empty.
    val (ctx, _) = buildSingleTpFetchContext(topic, topicId, 0,
      isFollower = false, withNullTopicName = true)
    stubFetchManager(ctx)

    val logConfig = LogConfig.fromProps(Collections.emptyMap(), new Properties())
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))

    stubNoThrottling()

    val request = buildFetchRequestPayload(tip, version,
      replicaId = -1, maxBytes, minBytes, maxWait)
    // Known finding: when the fetch context contains ONLY null-topic-name
    // partitions AND the FetchRequest version is <= 12, the resulting
    // unconverted FetchResponse fails the protocol-level size computation
    // (FetchResponseData$FetchableTopicResponse.addSize NPE) inside
    // handleFetchRequest. The deterministic reproducer for this bug is
    // `KafkaApisHandleFetchRequestNpeReproducerTest`. Until the bug is
    // fixed in handleFetchRequest, we swallow the topic-related NPE here
    // so Jazzer can keep exploring the rest of the input space and
    // surface other crashes. Any other Throwable is allowed to propagate.
    withKafkaApis() { k =>
      try {
        k.handleFetchRequest(request)
      } catch {
        case e: NullPointerException
            if e.getMessage != null && e.getMessage.contains("topic") =>
          ()
      }
    }
  }

  /**
   * Drives the down-conversion branches in `maybeConvertFetchedData`.
   * The fuzzer chooses:
   *  - the FetchRequest version in the down-convertible range [0..3],
   *  - the on-disk magic via `LogConfig.MESSAGE_FORMAT_VERSION_CONFIG`,
   *  - whether `messageDownConversionEnable` is on.
   *
   * This drives lines 850-857 (magic decision tree), 862 (downConversion
   * disabled => UNSUPPORTED_VERSION) and 866-880 (the actual lazy
   * down-conversion construction).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestFetchDownConversion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(0, 3).toShort  // versions where down-conversion may trigger
    val downConversionEnabled = data.consumeBoolean()
    // Pick on-disk magic via MESSAGE_FORMAT_VERSION_CONFIG. v2 (RecordBatch.MAGIC_VALUE_V2)
    // forces the down-convert path; v0 / v1 take the no-op None branch.
    val onDiskMagic = data.consumeInt(0, 2)
    val splitSize = data.consumeInt(10, 4096)
    val (rawTopic, recordBytes) = helperSplitByteArray(data.consumeRemainingAsBytes(), splitSize)
    val topic = if (rawTopic.isEmpty) "fuzz-downconv-topic" else rawTopic
    val topicId = Uuid.randomUuid()
    val tip = new TopicIdPartition(topicId, new TopicPartition(topic, 0))

    reset(replicaManager, fetchManager, clientQuotaManager, clientRequestQuotaManager, requestChannel)
    addTopicToMetadataCache(topic, numPartitions = 1, numBrokers = 1, topicId = topicId)

    val (ctx, tipInCtx) = buildSingleTpFetchContext(topic, topicId, 0, isFollower = false)
    stubFetchManager(ctx)

    val props = new Properties()
    val mfv = onDiskMagic match {
      case 0 => "0.9.0"   // magic v0
      case 1 => "0.10.0"  // magic v1
      case _ => "2.0"     // magic v2 (current)
    }
    props.put("message.format.version", mfv)
    props.put("message.downconversion.enable", downConversionEnabled.toString)
    val logConfig = LogConfig.fromProps(Collections.emptyMap(), props)
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))

    val records = if (recordBytes.isEmpty) MemoryRecords.EMPTY
      else MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(recordBytes))
    stubFetchMessages(tipInCtx, Errors.NONE, records, hw = 5L, logStartOffset = 0L,
      isReassignmentFetch = false)

    stubNoThrottling()

    val request = buildFetchRequestPayload(tip, version,
      replicaId = -1, maxBytes = 1024, minBytes = 0, maxWait = 0)
    withKafkaApis() { k =>
      k.handleFetchRequest(request)
    }
  }

  // ------------------------------------------------------------------------
  // Locally-scoped helpers (mirror those in KafkaApisFuzzTest)
  // ------------------------------------------------------------------------

  private def helperSplitByteArray(byteArray: Array[Byte], splitSize: Int): (String, Array[Byte]) = {
    require(splitSize >= 0, "Split size must be non-negative")
    val (stringBytes, remainingBytes) = byteArray.splitAt(splitSize)
    (new String(stringBytes), remainingBytes)
  }
}
