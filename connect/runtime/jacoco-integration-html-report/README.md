# Connect integration test coverage

JaCoCo HTML report for selected `org.apache.kafka.connect.integration` tests, including **all external libraries** on `:connect:runtime` `runtimeClasspath` (see `../runtimeClasspath-external-libraries.txt`).

By default, third-party libraries appear as **class/package coverage only** (no downloaded Maven sources). Use `--with-library-sources` on the generator script for line-level source on dependencies.

## Tests included

- `ExampleConnectIntegrationTest`
- `RestExtensionIntegrationTest`
- `RestForwardingIntegrationTest`
- `ConnectWorkerIntegrationTest`
- `StandaloneWorkerIntegrationTest`

## Regenerate

```bash
./connect/runtime/generate-integration-coverage-report.sh
```

With third-party source lines in the report:

```bash
./connect/runtime/generate-integration-coverage-report.sh --with-library-sources
```

Reuse existing `coverage.exec`:

```bash
SKIP_TESTS=1 ./connect/runtime/generate-integration-coverage-report.sh
```

Open [index.html](index.html) for the summary.
