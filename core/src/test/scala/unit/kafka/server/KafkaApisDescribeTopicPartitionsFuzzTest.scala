package unit.kafka.server

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import kafka.network.RequestChannel
import kafka.server.KafkaApisTest
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{reset, when}

/**
 * Jazzer fuzz tests targeting `KafkaApis.handleDescribeTopicPartitionsRequest`.
 *
 * The method has two arms:
 *
 *   - `Some(handler)` &mdash; only set when the broker uses a
 *     `KRaftMetadataCache`. Out of scope: this branch (and
 *     `DescribeTopicPartitionsRequestHandler`) is only reachable on KRaft.
 *
 *   - `None` &mdash; the ZooKeeper-backed broker path. `handleDescribeTopic
 *     PartitionsRequest` calls
 *     `request.body[DescribeTopicPartitionsRequest]
 *        .getErrorResponse(Errors.UNSUPPORTED_VERSION.exception)`
 *     and then forwards the result through `RequestHandlerHelper.send
 *     MaybeThrottle`, which records / applies the request quota and
 *     finally sends the response on the channel.
 *
 * `KafkaApisTest` defaults to a `ZkMetadataCache` so these fuzz tests
 * naturally take the `None` arm. They drive `getErrorResponse`'s loop
 * over the request's topic list (varying topic count and topic names,
 * including pathological inputs the fuzzer will produce) and they vary
 * the request quota throttle return value to drive both
 * `throttleTimeMs == 0` and `throttleTimeMs > 0` paths through
 * `RequestHandlerHelper.sendMaybeThrottle` / `throttle` /
 * `requestChannel.sendResponse`.
 */
class KafkaApisDescribeTopicPartitionsFuzzTest extends KafkaApisTest {

  /**
   * Drives the ZooKeeper-broker arm of `handleDescribeTopicPartitionsRequest`
   * with an arbitrary list of topic names (including the empty list) and a
   * non-throttling request quota. Exercises:
   *   - `case None` arm (KafkaApis.scala line 1457)
   *   - `getErrorResponse(UNSUPPORTED_VERSION.exception)` loop over topics
   *     (DescribeTopicPartitionsRequest.java lines 79-92)
   *   - `RequestHandlerHelper.sendMaybeThrottle`:
   *       maybeRecordAndGetThrottleTimeMs (line 116, 141-145),
   *       !request.isForwarded => throttle(...) call (lines 118-119),
   *       response.maybeSetThrottleTimeMs (line 120),
   *       requestChannel.sendResponse (line 121).
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestZkUnsupportedVersion(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion().toInt).toShort
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(0)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeTopicPartitionsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Drives the `request.isForwarded == true` branch of
   * `RequestHandlerHelper.sendMaybeThrottle` (line 118). Wraps the
   * DescribeTopicPartitionsRequest in an envelope so the resulting
   * RequestChannel.Request reports isForwarded == true; in that case
   * sendMaybeThrottle skips the local throttle() call and goes
   * straight to maybeSetThrottleTimeMs + sendResponse.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestZkUnsupportedVersionForwarded(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion().toInt).toShort
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)
    val throttleMs = data.consumeInt(0, 100)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-fwd-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildForwardedRequest(req)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeTopicPartitionsRequest(request)
    finally kafkaApis.close()
  }

  /**
   * Same as `fuzzTestZkUnsupportedVersion` but pins the request quota to
   * a positive throttle value so `RequestHandlerHelper.throttle(...)`
   * actually mutes the channel before the response is sent.
   */
  @FuzzTest(maxDuration = "20s")
  def fuzzTestZkUnsupportedVersionThrottled(data: FuzzedDataProvider): Unit = {
    val version = data.consumeInt(
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion().toInt,
      ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion().toInt).toShort
    val numTopics = data.consumeInt(0, 16)
    val nameLen = data.consumeInt(0, 64)
    val throttleMs = data.consumeInt(1, 100)

    val requestData = new DescribeTopicPartitionsRequestData()
    var i = 0
    while (i < numTopics) {
      val rawName = new String(data.consumeBytes(nameLen))
      val safeName = if (rawName.isEmpty) s"fuzz-throttled-topic-$i" else rawName
      requestData.topics().add(
        new DescribeTopicPartitionsRequestData.TopicRequest().setName(safeName))
      i += 1
    }

    val req = new DescribeTopicPartitionsRequest.Builder(requestData).build(version)
    val request = buildRequest(req)

    reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyLong)).thenReturn(throttleMs)

    val kafkaApis = createKafkaApis()
    try kafkaApis.handleDescribeTopicPartitionsRequest(request)
    finally kafkaApis.close()
  }
}
