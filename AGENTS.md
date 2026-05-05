# AGENTS.md

## Cursor Cloud specific instructions

This is the **Apache Kafka** source repository (v3.9.1), on the `KafkaApis-fuzzing` branch that adds Jazzer-based fuzz tests for `KafkaApis`.

### Build system
- **Gradle 8.10.2** via `./gradlew` wrapper (no install needed).
- **Java 21**, **Scala 2.13.15**.
- No external services (databases, brokers, ZooKeeper) are needed for building or running tests — everything is embedded/mocked.

### Key commands
| Task | Command |
|---|---|
| Build all JARs | `./gradlew jar` |
| Compile core + tests | `./gradlew :core:compileScala :core:compileTestScala` |
| Lint (checkstyle) | `./gradlew :core:checkstyleMain :core:checkstyleTest` |
| Run a single fuzz test (regression mode) | `./gradlew :core:test --tests "unit.kafka.server.fuzz.HandleProduceRequestFuzzTest.fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower"` |
| Run a single fuzz test (fuzz mode) | `JAZZER_FUZZ=1 ./gradlew :core:test --no-daemon --rerun-tasks --tests "unit.kafka.server.fuzz.HandleProduceRequestFuzzTest.fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower"` |
| Full fuzz suite with coverage | `./core/src/test/fuzz/run_fuzz_with_coverage.sh` (≈50 min) |

### Gotchas
- Jazzer only fuzzes the **first** `@FuzzTest` per JVM. To fuzz all tests, each must be invoked in its own `./gradlew` call (use `--no-daemon --rerun-tasks`).
- Without `JAZZER_FUZZ=1`, fuzz tests run as single-iteration regression tests against the corpus (fast, good for CI verification).
- `gradle.properties` sets `org.gradle.jvmargs=-Xmx2g -Xss4m -XX:+UseParallelGC`. The build is memory-intensive.
- Fuzz test classes live under `core/src/test/scala/unit/kafka/server/fuzz/`.
- See `core/src/test/fuzz/README.md` for coverage collection details with JaCoCo.
