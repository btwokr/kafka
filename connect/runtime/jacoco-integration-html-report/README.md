# Connect integration test coverage (with Jetty Server)

JaCoCo HTML report for a subset of `org.apache.kafka.connect.integration` tests, including **line coverage for `org.eclipse.jetty:jetty-server:12.0.34`** (classes from the dependency JAR and sources from the Maven sources artifact).

## Tests included

- `ExampleConnectIntegrationTest`
- `RestExtensionIntegrationTest`
- `RestForwardingIntegrationTest`
- `ConnectWorkerIntegrationTest`
- `StandaloneWorkerIntegrationTest`

## Jetty in the report

Browse from [index.html](index.html) under **`org.eclipse.jetty.server`** (and related packages such as `org.eclipse.jetty.server.handler`) for Jetty Server source line coverage.

## Reproduce

```bash
./connect/runtime/generate-integration-coverage-report.sh
```

Or manually:

```bash
export JAVA_TOOL_OPTIONS='-javaagent:/path/to/jacocoagent.jar=destfile=connect/runtime/coverage.exec,append=true,excludes=*junit*:*mockito.*'

./gradlew :connect:runtime:test -Dorg.gradle.parallel=false --max-workers=1 -PmaxParallelForks=1 \
  --tests "org.apache.kafka.connect.integration.ExampleConnectIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.RestExtensionIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.RestForwardingIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.ConnectWorkerIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.StandaloneWorkerIntegrationTest"

java -jar jacococli.jar report connect/runtime/coverage.exec \
  --html connect/runtime/jacoco-integration-html-report \
  --classfiles ~/.gradle/caches/.../jetty-server-12.0.34.jar \
  --sourcefiles .jacoco/jetty-server-12.0.34 \
  --classfiles <kafka-module>/build/classes/java/main ... \
  --sourcefiles <kafka-module>/src/main/java ...
```

`coverage.exec` is not committed; regenerate locally.
