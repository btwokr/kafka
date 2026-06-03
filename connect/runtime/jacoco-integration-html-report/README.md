# Connect integration test coverage (partial)

JaCoCo HTML report for a **subset** of `org.apache.kafka.connect.integration` tests (full suite was not run due to runtime).

## Tests included

- `ExampleConnectIntegrationTest`
- `StartAndStopLatchTest`
- `StartAndStopCounterTest`

## How to reproduce

```bash
export JAVA_TOOL_OPTIONS='-javaagent:/path/to/jacocoagent.jar=destfile=connect/runtime/coverage.exec,append=true,excludes=*junit*:*mockito.*'

./gradlew :connect:runtime:test -Dorg.gradle.parallel=false --max-workers=1 -PmaxParallelForks=1 \
  --tests "org.apache.kafka.connect.integration.ExampleConnectIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.StartAndStopLatchTest" \
  --tests "org.apache.kafka.connect.integration.StartAndStopCounterTest"

# Generate HTML (requires jacococli.jar and compiled classes)
java -jar jacococli.jar report connect/runtime/coverage.exec \
  --html connect/runtime/jacoco-integration-html-report \
  --classfiles <module>/build/classes/java/main ... \
  --sourcefiles <module>/src/main/java ...
```

Open [index.html](index.html) in a browser for line-by-line coverage.
