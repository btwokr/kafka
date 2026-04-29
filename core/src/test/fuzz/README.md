# Fuzz testing `KafkaApis.handleProduceRequest`

This directory documents how to run the Jazzer fuzz tests in
`core/src/test/scala/unit/kafka/server/KafkaApisFuzzTest.scala` and how to
collect line/branch coverage of the fuzz target
(`kafka.server.KafkaApis#handleProduceRequest`) with JaCoCo.

There are five `@FuzzTest`-annotated cases in `KafkaApisFuzzTest`:

* `fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower`
* `fuzzTestTransactionalParametersSetCorrectly`
* `fuzzTestNullableTransactionalId`
* `fuzzTestNoAuthorizedTransactionalRequest`
* `fuzzTestNoAuthorized`

Each one feeds a `FuzzedDataProvider` into `kafkaApis.handleProduceRequest`
through a fully-mocked `KafkaApisTest` harness.

## How fuzz mode is enabled

`jazzer-junit` only fuzzes (rather than running a single regression iteration
against the corpus) when the env var `JAZZER_FUZZ=1` is set during the test
JVM's lifetime. Fuzzing duration is controlled by the `maxDuration` argument
of the `@FuzzTest` annotation; in this branch all five cases are set to
`maxDuration = "100s"` (≈100 seconds of fuzzing per test).

> Important: Jazzer only fuzzes the **first** `@FuzzTest` it sees in a JVM;
> the rest run a single regression pass. To get all five tests to fuzz, each
> one must be invoked in its own Gradle test JVM, hence the loop in the
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

What the script does (≈ 25-30 minutes wall time on a developer machine):

1. Downloads JaCoCo 0.8.12 to `/tmp/jacoco/` if it isn't there yet.
2. Sets `JAVA_TOOL_OPTIONS` to attach `jacocoagent.jar` (writing to
   `/tmp/jacoco/coverage.exec`, `append=true`).
3. Sets `JAZZER_FUZZ=1` so `jazzer-junit` actually fuzzes.
4. Loops over the 5 fuzz tests and runs each in its **own**
   `./gradlew :core:test --no-daemon --rerun-tasks --tests ...` invocation,
   so every test reaches Jazzer's fuzzing mode (one fuzz target per JVM).
5. Generates HTML / XML / CSV reports with `jacococli.jar report ...`
   into `/tmp/jacoco/report/`.
6. Prints a focused summary of `KafkaApis.scala` lines 606-752
   (the body of `handleProduceRequest`, including its nested closures
   `sendResponseCallback` and `processingStatsCallback`).

Open `/tmp/jacoco/report/html/index.html` and navigate to
`kafka.server → KafkaApis.scala` to see the colour-annotated source for the
whole class.

The XML report (`/tmp/jacoco/report/coverage.xml`) provides per-method
counters; the entry of interest is the `<method name="handleProduceRequest">`
under `class kafka/server/KafkaApis`, plus the
`$anonfun$handleProduceRequest$N` siblings (Scala-generated closures of the
same method).

## Last recorded result

With `maxDuration = "100s"` per test (10,009 total fuzz iterations, no
crashes), JaCoCo reports the following coverage of
`KafkaApis#handleProduceRequest` (lines 606-752, including its closures):

| Metric                | Covered / Total | %      |
| --------------------- | --------------- | ------ |
| Source lines          | 69 / 84         | 82.1%  |
| Bytecode instructions | 461 / 626       | 73.6%  |
| Branches              | 30 / 42         | 71.4%  |

The 15 missed lines are documented in
[`coverage_results/handleProduceRequest_coverage_summary.txt`](./coverage_results/handleProduceRequest_coverage_summary.txt)
and stem from either lazy log-message lambdas or paths that require
non-mocked behaviour from `replicaManager` (real append, non-zero
throttle return, real record processing stats).

## In-tree coverage artefacts

A trimmed slice of the most recent JaCoCo report ships with this branch under
[`coverage_results/`](./coverage_results) so reviewers can browse it without
re-running the fuzzer:

* `coverage_results/html/index.html` &mdash; small landing page linking the
  annotated `KafkaApis.scala` source view and the per-method table.
* `coverage_results/html/kafka.server/KafkaApis.scala.html` &mdash; JaCoCo's
  green/yellow/red annotated `KafkaApis.scala` (open this and scroll to
  lines 606&ndash;752 for `handleProduceRequest`).
* `coverage_results/html/kafka.server/KafkaApis.html` &mdash; per-method
  counters for `KafkaApis`, including every `$anonfun$handleProduceRequest$N`
  Scala-generated closure.
* `coverage_results/coverage_KafkaApis.xml` &mdash; subset of the JaCoCo XML
  report scoped to the `kafka/server` package (machine-readable).
* `coverage_results/coverage.csv` &mdash; full per-class CSV from the run.
* `coverage_results/handleProduceRequest_coverage_summary.txt` &mdash;
  human-readable summary including per-line and per-closure breakdown.

The full HTML report (≈ 12 MB across hundreds of classes) is **not**
committed; rerun `run_fuzz_with_coverage.sh` to regenerate it under
`/tmp/jacoco/report/`.
