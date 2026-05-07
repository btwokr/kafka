# Fuzz testing `KafkaApis` (produce, fetch, describe-topic-partitions, offset-fetch)

This directory documents how to run the Jazzer fuzz tests for
`kafka.server.KafkaApis` and how to collect line/branch coverage of the
fuzz targets with JaCoCo.

## Test classes

### `HandleProduceRequestFuzzTest` (target: `handleProduceRequest`)

Eight `@FuzzTest`-annotated cases live in
`core/src/test/scala/unit/kafka/server/fuzz/HandleProduceRequestFuzzTest.scala`.

### `HandleDescribeTopicPartitionsRequestFuzzTest` (target: `handleDescribeTopicPartitionsRequest`, ZK arm)

Three `@FuzzTest` cases (`maxDuration = "20s"` each) live in
`core/src/test/scala/unit/kafka/server/fuzz/HandleDescribeTopicPartitionsRequestFuzzTest.scala`.
Only the ZooKeeper arm of `handleDescribeTopicPartitionsRequest` is in
scope (the KRaft `Some(handler)` arm is unreachable from a
`ZkMetadataCache` so it is left to the dedicated
`DescribeTopicPartitionsRequestHandlerTest`):

### `HandleFetchRequestFuzzTest` (target: `handleFetchRequest`)

Five `@FuzzTest` cases (`maxDuration = "20s"` each) live in
`core/src/test/scala/unit/kafka/server/fuzz/HandleFetchRequestFuzzTest.scala`:

Each test feeds a `FuzzedDataProvider` into
`kafkaApis.handleFetchRequest` through a fully-mocked `KafkaApisTest`
harness (with `fetchManager` and `replicaManager.fetchMessages` stubbed
to surface specific branches).

### `HandleOffsetFetchRequestFuzzTest` (target: `handleOffsetFetchRequest`)

Five `@FuzzTest` cases (`maxDuration = "20s"` each) live in
`core/src/test/scala/unit/kafka/server/fuzz/HandleOffsetFetchRequestFuzzTest.scala`.
They cover OffsetFetch protocol version 0 (ZooKeeper-backed reads with
`KafkaZkClient.getConsumerOffset` stubbed in tests), single-group
coordinator requests for versions 1&ndash;7, batched multi-group
requests for version 8 and above, and the asynchronous coordinator
completion path (`sendMaybeThrottle`) including forwarded requests.

## How fuzz mode is enabled

`jazzer-junit` only fuzzes (rather than running a single regression iteration
against the corpus) when the env var `JAZZER_FUZZ=1` is set during the test
JVM's lifetime. Fuzzing duration is controlled by the `maxDuration` argument
of the `@FuzzTest` annotation; in this branch the produce baseline tests
run for `100s` each, the produce coverage-improvement tests, all fetch,
describe-topic-partitions, and offset-fetch tests run for `20s` each.

> Important: Jazzer only fuzzes the **first** `@FuzzTest` it sees in a JVM;
> the rest run a single regression pass. To get every fuzz method to fuzz,
> each one must be invoked in its own Gradle test JVM, hence the loop in the
> reproduction script below.

## Attaching the JaCoCo agent

Instead of touching `build.gradle`, the JaCoCo runtime agent is attached via
the JDK-standard `JAVA_TOOL_OPTIONS` env var. Every JVM the JDK launches
(including Gradle's forked test JVMs) automatically picks this up, so no
build-script changes are needed.

```bash
# JaCoCo distribution (any 0.8.x works; we used 0.8.12)
mkdir -p /tmp/jacoco && cd /tmp/jacoco
curl -sSLo jacoco.zip \
    https://repo1.maven.org/maven2/org/jacoco/jacoco/0.8.12/jacoco-0.8.12.zip
unzip -q jacoco.zip   # produces lib/jacocoagent.jar and lib/jacococli.jar

export JAVA_TOOL_OPTIONS="-javaagent:/tmp/jacoco/lib/jacocoagent.jar=\
destfile=/tmp/jacoco/coverage.exec,append=true,\
excludes=*junit*:*mockito.*:com.code_intelligence.*:org.jacoco.*"
```

The `excludes` filter prevents JUnit/Mockito and Jazzer's own
`com.code_intelligence.*` runtime as well as JaCoCo itself from being
instrumented (the user request specifies these exclusions).

## Reproduce coverage results

From the repository root:

```bash
./core/src/test/fuzz/run_fuzz_with_coverage.sh
```

What the script does (≈ 60-70 minutes wall time on a developer machine):

1. Downloads JaCoCo 0.8.12 to `/tmp/jacoco/` if it isn't there yet.
2. Sets `JAVA_TOOL_OPTIONS` to attach `jacocoagent.jar` (writing to
   `/tmp/jacoco/coverage.exec`, `append=true`).
3. Sets `JAZZER_FUZZ=1` so `jazzer-junit` actually fuzzes.
4. Loops over the 21 fuzz tests (8 in `HandleProduceRequestFuzzTest`, 5 in
   `HandleFetchRequestFuzzTest`, 3 in
   `HandleDescribeTopicPartitionsRequestFuzzTest`, and 5 in
   `HandleOffsetFetchRequestFuzzTest`) and runs each in its
   **own** `./gradlew :core:test --no-daemon --rerun-tasks --tests ...`
   invocation, so every test reaches Jazzer's fuzzing mode (one fuzz
   target per JVM). Each test runs for the duration declared in its
   `@FuzzTest` annotation.
5. Generates HTML / XML / CSV reports with `jacococli.jar report ...`
   into `/tmp/jacoco/report/`.
6. Prints a focused summary of `KafkaApis.scala` for
   `handleProduceRequest` (lines 606-752),
   `handleFetchRequest` (lines 757-1077),
   `handleDescribeTopicPartitionsRequest` (lines 1445-1461),
   `handleOffsetFetchRequest` (lines 1466-1630, including private helpers
   `handleOffsetFetchRequestFromZookeeper` / `handleOffsetFetchRequestFromCoordinator`
   and related closures in that span), and
   `RequestHandlerHelper.sendMaybeThrottle` (lines 112-122).

Open `/tmp/jacoco/report/html/index.html` and navigate to
`kafka.server → KafkaApis.scala` to see the colour-annotated source for the
whole class.

The XML report (`/tmp/jacoco/report/coverage.xml`) provides per-method
counters; the entries of interest are
`<method name="handle{Produce,Fetch,DescribeTopicPartitions,OffsetFetch}Request">` under
`class kafka/server/KafkaApis`, plus the `$anonfun$...$N` siblings
(Scala-generated closures of the same method).

### Persisting `coverage.exec` across VM recycles

The runner script writes its exec file to `/tmp/jacoco/coverage.exec`,
which lives on the agent VM's `/tmp` and is wiped whenever the VM is
recycled. To avoid having to re-run the full fuzz suite (roughly an hour)
every time, the most recent exec file is committed to the repo at
[`coverage_results/coverage.exec.xz`](./coverage_results/coverage.exec.xz)
(xz-compressed, ~500 KB; the underlying exec file is mostly zero bytes
and compresses ≈ 80×).

The runner has three modes, selectable via the `FUZZ_MODE` env var:

| `FUZZ_MODE` | Pre-run                                                 | Post-run                                                                 |
| ----------- | ------------------------------------------------------- | ------------------------------------------------------------------------ |
| `fresh` (default) | `rm -f /tmp/jacoco/coverage.exec`                  | nothing                                                                  |
| `resume`    | `xz -dc coverage_results/coverage.exec.xz > /tmp/jacoco/coverage.exec` | nothing                                          |
| `snapshot`  | `rm -f /tmp/jacoco/coverage.exec`                       | `xz -9e -c /tmp/jacoco/coverage.exec > coverage_results/coverage.exec.xz` |

Optional **`FUZZ_SUITE`** (default `all`): set `FUZZ_SUITE=offset-fetch` to run only
`HandleOffsetFetchRequestFuzzTest` (four tests). Useful with `FUZZ_MODE=resume` to
append JaCoCo data for the new targets without re-running produce/fetch/describe.

Typical workflow:

```bash
# Fresh full run that also updates the in-tree snapshot for next time.
FUZZ_MODE=snapshot ./core/src/test/fuzz/run_fuzz_with_coverage.sh

# Later (possibly on a different agent VM): only run the tests you care
# about and have their coverage append on top of the committed snapshot.
FUZZ_MODE=resume ./core/src/test/fuzz/run_fuzz_with_coverage.sh
```

When you want the in-tree snapshot updated again after a `resume` run,
re-snapshot manually:

```bash
xz -9e -c /tmp/jacoco/coverage.exec > core/src/test/fuzz/coverage_results/coverage.exec.xz
```

## Last recorded result

After running the produce + fetch + describe-topic-partitions fuzz tests
(11,743 produce-method iterations + 3,333 fetch-method iterations =
15,076 fuzz iterations for those targets; no Jazzer-discovered crashes
after extracting the NPE finding into
`KafkaApisHandleFetchRequestNpeReproducerTest`), JaCoCo reports:

### `handleProduceRequest` (KafkaApis.scala lines 606&ndash;752)

| Metric                | Covered / Total | %      |
| --------------------- | --------------- | ------ |
| Source lines          | 75 / 84         | 89.3%  |
| Bytecode instructions | 505 / 626       | 80.7%  |
| Branches              | 36 / 42         | 85.7%  |

### `handleFetchRequest` (KafkaApis.scala lines 757&ndash;1077)

| Metric                | Covered / Total | %      |
| --------------------- | --------------- | ------ |
| Source lines          | 177 / 187       | 94.7%  |
| Bytecode instructions | 1109 / 1352     | 82.0%  |
| Branches              | 69 / 94         | 73.4%  |

### `handleDescribeTopicPartitionsRequest` (KafkaApis.scala lines 1445&ndash;1461, ZK arm only)

The KRaft `Some(handler)` arm (lines 1448-1454) is intentionally out of
scope. The reachable ZK arm is fully covered:

| Metric                                | Covered / Total | %       |
| ------------------------------------- | --------------- | ------- |
| Source lines (full method)            | 4 / 10          | 40.0%   |
| Source lines (ZK arm only)            | 4 / 4           | 100.0%  |
| Branches (full method)                | 2 / 4           | 50.0%   |
| Branches (ZK arm only)                | 2 / 2           | 100.0%  |

### `RequestHandlerHelper.sendMaybeThrottle` (RequestHandlerHelper.scala 112&ndash;122)

| Metric                | Covered / Total | %       |
| --------------------- | --------------- | ------- |
| Source lines          | 5 / 5           | 100.0%  |
| Bytecode instructions | 24 / 24         | 100.0%  |
| Branches              | 2 / 2           | 100.0%  |

### `handleOffsetFetchRequest` (KafkaApis.scala lines 1466&ndash;1630)

| Metric                | Covered / Total | %       |
| --------------------- | --------------- | ------- |
| Source lines          | 93 / 104        | 89.4%   |
| Bytecode instructions | 489 / 602       | 81.2%   |
| Branches              | 24 / 30         | 80.0%   |

Per-line gaps are listed in
[`coverage_results/coverage_summary.txt`](./coverage_results/coverage_summary.txt).

The remaining uncovered lines in both methods are documented per-line in
[`coverage_results/coverage_summary.txt`](./coverage_results/coverage_summary.txt).
They are predominantly logging format-arg lambdas plus a few branches
that are reachable only through specialized
record-batch / compression-codec inputs not produced by the current
`FuzzedDataProvider`-based harness.

## In-tree coverage artefacts

A trimmed slice of the most recent JaCoCo report ships with this branch under
[`coverage_results/`](./coverage_results) so reviewers can browse it without
re-running the fuzzer:

* `coverage_results/html/index.html` &mdash; small landing page linking the
  annotated `KafkaApis.scala` source view and the per-method table.
* `coverage_results/html/kafka.server/KafkaApis.scala.html` &mdash; JaCoCo's
  green/yellow/red annotated `KafkaApis.scala` (scroll to lines
  606&ndash;752 for `handleProduceRequest`, 757&ndash;1077 for
  `handleFetchRequest`, or 1466&ndash;1630 for `handleOffsetFetchRequest`).
* `coverage_results/html/kafka.server/KafkaApis.html` &mdash; per-method
  counters for `KafkaApis`, including every
  `$anonfun$handleProduceRequest$N`, `$anonfun$handleFetchRequest$N`, and
  offset-fetch-related Scala-generated closures.
* `coverage_results/coverage_KafkaApis.xml` &mdash; subset of the JaCoCo XML
  report scoped to the `kafka/server` package (machine-readable).
* `coverage_results/coverage.csv` &mdash; CSV rows for `KafkaApis` and
  `RequestHandlerHelper` only (trimmed from the full report).
* `coverage_results/coverage_summary.txt` &mdash; human-readable summary
  including per-line breakdown for the fuzz targets,
  plus the description of the NPE crash finding.
* `refresh_in_repo_coverage.py` &mdash; copies the trimmed HTML/XML/CSV slice
  from `/tmp/jacoco/report/`; invoked automatically at the end of
  `run_fuzz_with_coverage.sh` after JaCoCo report generation.

The full HTML report (≈ 12 MB across hundreds of classes) is **not**
committed; rerun `run_fuzz_with_coverage.sh` to regenerate it under
`/tmp/jacoco/report/`.
