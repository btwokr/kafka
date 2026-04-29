package unit.kafka.server

import kafka.network.RequestChannel
import kafka.server.{FetchSessionCacheShard, FullFetchContext, KafkaApisTest}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, FetchRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions.{assertNotNull, assertNull, fail}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyDouble, anyLong}
import org.mockito.Mockito.when

import java.util
import java.util.{Collections, Optional, Properties}
import scala.jdk.CollectionConverters._

/**
 * Crash reproducer for an NPE inside `KafkaApis#handleFetchRequest`.
 *
 * == Summary ==
 * `handleFetchRequest` crashes with a `NullPointerException` thrown deep
 * inside the protocol-encode path (`FetchResponseData$FetchableTopicResponse
 * .addSize -> String.getBytes(charset)`) whenever the entry produced by
 * `FetchManager#newContext` is a `TopicIdPartition` whose topic name is
 * `null` (the conventional encoding of an unresolved topic id in a fetch
 * session) AND the FetchRequest version is <= 12 (versions for which the
 * response schema still carries the topic name on the wire; from v13
 * onwards the topic name field is replaced by topicId, so a null name
 * is harmless for sizing - see
 * `FetchResponseData.FetchableTopicResponse.addSize` line 755 in the
 * generated message class).
 *
 * == Trigger flow ==
 *   handleFetchRequest                  KafkaApis.scala:757
 *     - foreachPartition sees `topic == null` for the consumer block
 *       and routes the entry to `erroneous` with UNKNOWN_TOPIC_ID
 *       (line 800-801).
 *     - `interesting` is empty so `processResponseCallback(Seq.empty)`
 *       is invoked (line 1033).
 *     - inside processResponseCallback the erroneous map is copied into
 *       `partitions` (line 939) and then `fetchContext.getResponseSize(
 *       partitions, versionId)` is called at line 1000.
 *     - `FullFetchContext.getResponseSize` builds an unconverted
 *       `FetchResponse` with a `FetchableTopicResponse` whose `topic` is
 *       null (because the id was never resolved). Computing
 *       `unconvertedFetchResponse.sizeOf(versionId)` walks the message
 *       and hits `String.getBytes(charset)` on the null topic name.
 *
 * == How this was discovered ==
 * Surfaced by Jazzer fuzzing of `KafkaApisFetchFuzzTest` (run #16 of
 * `fuzzTestFetchConsumer`, mode == 1). The 2-byte crash input
 * (0x3d 0x0a) selected the "null topic in fetch context" sub-mode of
 * that test.
 *
 * == How a remote client can reach this code path ==
 * In production, an unresolved `TopicIdPartition` (null topic name) ends
 * up in the FetchManager's session via the IncrementalFetchContext path
 * when a topicId carried in the cached fetch session can no longer be
 * resolved against the metadata cache (e.g. the topic was deleted between
 * fetch sessions). If a client subsequently issues a FetchRequest at a
 * version `<= 12` against such a session - or if the broker otherwise
 * exposes the unresolved entry to a v<=12 sizer - the `handleFetchRequest`
 * code path crashes with NullPointerException instead of returning a
 * proper UNKNOWN_TOPIC_ID error response.
 *
 * Whether a remote client can actually reach this with a v<=12 request
 * (since fetch sessions and topic-id encoding are tightly coupled in
 * v>=12) is up to the maintainers to decide. Either way, the broker
 * dying with an unhandled NPE inside request processing is fragile and
 * worth fixing.
 *
 * This reproducer narrows the scenario down to a deterministic mocked
 * test so the bug can be fixed without depending on the fuzzer
 * environment.
 *
 * == Suggested fixes (for the fix author) ==
 *   - In `handleFetchRequest`, do not place null-topic-name partitions
 *     into `partitions` before the response-size computation.
 *   - Or, in `FetchableTopicResponse.addSize`, treat a null `topic` as
 *     a 0-byte string for sizing purposes (consistent with the
 *     null-tolerant write path elsewhere in the message protocol).
 *   - Or, eagerly translate the unresolved topic id into a stable
 *     placeholder before letting it leave handleFetchRequest.
 */
class KafkaApisHandleFetchRequestNpeReproducerTest extends KafkaApisTest {

  @Test
  def reproducesNpeWhenFetchSessionReturnsNullTopicName(): Unit = {
    val topicId = Uuid.randomUuid()
    val unresolvedTopicIdPartition =
      new TopicIdPartition(topicId, new TopicPartition(/* topic = */ null, 0))

    // Stub fetchManager.newContext(...) to return a FullFetchContext that
    // contains a single partition whose topic name is null. This mirrors the
    // production behaviour of FetchManager when the topic id in the request
    // can't be resolved against the metadata cache.
    val fetchData: util.Map[TopicIdPartition, FetchRequest.PartitionData] =
      Collections.singletonMap(unresolvedTopicIdPartition,
        new FetchRequest.PartitionData(topicId, 0L, 0L, 1024, Optional.empty[Integer]()))
    val ctx = new FullFetchContext(
      Time.SYSTEM,
      new FetchSessionCacheShard(1000, 100),
      new JFetchMetadata(0, 0),
      fetchData,
      /* usesTopicIds = */ true,
      /* isFromFollower = */ false)
    when(fetchManager.newContext(
      any[Short], any[JFetchMetadata], any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(ctx)

    // Default LogConfig + no quota throttling - keeps everything else on
    // the happy path so we isolate the response-size NPE.
    val logConfig = LogConfig.fromProps(Collections.emptyMap(), new Properties())
    when(replicaManager.getLogConfig(any[TopicPartition])).thenReturn(Some(logConfig))
    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)

    // Note: replicaManager.fetchMessages is intentionally NOT stubbed here
    // because the offending code path falls through to
    // `processResponseCallback(Seq.empty)` (KafkaApis.scala:1033) - the
    // `interesting` collection is empty since the only partition is
    // routed into the `erroneous` map via the null-topic check at
    // KafkaApis.scala:800-801. fetchMessages is never invoked.

    // Build a real FetchRequest at version 12. The NPE only triggers
    // when sizing the response uses the topic *name* (FetchResponse
    // schema versions <= 12); for versions >= 13 the topic name field
    // is replaced by topicId in the wire schema so the same null-topic
    // entry serialises without NPE-ing. (See
    // FetchResponseData.FetchableTopicResponse.addSize at line 755 of
    // the generated message class.)
    val builderTp = new TopicPartition("placeholder-topic", 0)
    val requestData = Map(builderTp ->
      new FetchRequest.PartitionData(topicId, 0L, 0L, 1024, Optional.empty[Integer]())).asJava
    val fetchRequest = new FetchRequest.Builder(12, 12, /* replicaId = */ -1,
      /* replicaEpoch = */ -1, /* maxWait = */ 0, /* minBytes = */ 0, requestData)
      .metadata(new JFetchMetadata(0, 0))
      .build()
    val request = buildRequest(fetchRequest)

    val kafkaApis = createKafkaApis()
    try {
      val npe: NullPointerException = try {
        kafkaApis.handleFetchRequest(request)
        null
      } catch {
        case e: NullPointerException => e
      }

      assertNotNull(npe,
        "Expected handleFetchRequest to throw NullPointerException when the " +
        "fetch context returns a TopicIdPartition with a null topic name.")

      // Pin down the exact crash signature so this test fails loudly if the
      // implementation moves but the underlying bug is left unfixed.
      val msg = npe.getMessage
      assertNotNull(msg, "NPE has no message")
      if (!msg.contains("topic")) {
        fail(s"Unexpected NPE message (expected one referencing 'topic'): $msg")
      }

      val frames = npe.getStackTrace.map(_.toString).mkString("\n")
      val expectedFrames = Seq(
        "org.apache.kafka.common.message.FetchResponseData",
        "org.apache.kafka.common.protocol.Message.size",
        "kafka.server.FullFetchContext.getResponseSize",
        "kafka.server.KafkaApis"
      )
      expectedFrames.foreach { f =>
        if (!frames.contains(f)) {
          fail(s"NPE stack trace did not contain expected frame '$f'.\n" +
            s"Full stack:\n$frames")
        }
      }
    } finally {
      kafkaApis.close()
    }
    // Sanity check: we never even exited handleFetchRequest cleanly, so
    // the topic name *must* still be null in the unresolved fetch entry.
    assertNull(unresolvedTopicIdPartition.topic,
      "Pre-condition broken: TopicIdPartition.topic should be null in this reproducer.")
  }
}
