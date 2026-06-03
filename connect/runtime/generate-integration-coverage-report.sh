#!/usr/bin/env bash
# Generate JaCoCo HTML coverage for Connect integration tests, including Jetty Server.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="${ROOT_DIR}/connect/runtime"
JACOCO_DIR="${ROOT_DIR}/.jacoco"
JETTY_DIR="${JACOCO_DIR}/jetty-server-12.0.34"
JACOCO_ZIP="${JACOCO_DIR}/jacoco-0.8.14.zip"
JACOCO_AGENT_JAR="${JACOCO_DIR}/org.jacoco.agent-0.8.14.jar"
JACOCO_AGENT="${JACOCO_DIR}/jacocoagent.jar"
JACOCO_CLI="${JACOCO_DIR}/jacococli.jar"
COVERAGE_EXEC="${RUNTIME_DIR}/coverage.exec"
REPORT_DIR="${RUNTIME_DIR}/jacoco-integration-html-report"

JACOCO_VERSION="0.8.14"
JETTY_VERSION="12.0.34"
MAVEN_BASE="https://repo1.maven.org/maven2"

mkdir -p "${JACOCO_DIR}"

if [[ ! -f "${JACOCO_AGENT}" ]]; then
  if [[ ! -f "${JACOCO_AGENT_JAR}" ]]; then
    curl -fsSL -o "${JACOCO_AGENT_JAR}" \
      "${MAVEN_BASE}/org/jacoco/org.jacoco.agent/${JACOCO_VERSION}/org.jacoco.agent-${JACOCO_VERSION}.jar"
  fi
  unzip -p "${JACOCO_AGENT_JAR}" jacocoagent.jar > "${JACOCO_AGENT}"
fi

if [[ ! -f "${JACOCO_CLI}" ]]; then
  if [[ ! -f "${JACOCO_ZIP}" ]]; then
    curl -fsSL -o "${JACOCO_ZIP}" \
      "${MAVEN_BASE}/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip"
  fi
  unzip -j -o "${JACOCO_ZIP}" lib/jacococli.jar -d "${JACOCO_DIR}"
fi

# Resolve jetty-server jar from Gradle cache (after :connect:runtime:testClasses)
cd "${ROOT_DIR}"
./gradlew :connect:runtime:testClasses -q
JETTY_SERVER_JAR="$(find "${HOME}/.gradle/caches/modules-2/files-2.1/org.eclipse.jetty/jetty-server/${JETTY_VERSION}" \
  -name "jetty-server-${JETTY_VERSION}.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" 2>/dev/null | head -1)"
if [[ -z "${JETTY_SERVER_JAR}" || ! -f "${JETTY_SERVER_JAR}" ]]; then
  echo "jetty-server jar not found; run ./gradlew :connect:runtime:testClasses first" >&2
  exit 1
fi

JETTY_SOURCES_JAR="${JACOCO_DIR}/jetty-server-${JETTY_VERSION}-sources.jar"
if [[ ! -f "${JETTY_SOURCES_JAR}" ]]; then
  curl -fsSL -o "${JETTY_SOURCES_JAR}" \
    "${MAVEN_BASE}/org/eclipse/jetty/jetty-server/${JETTY_VERSION}/jetty-server-${JETTY_VERSION}-sources.jar"
fi

if [[ ! -d "${JETTY_DIR}/src" ]]; then
  rm -rf "${JETTY_DIR}"
  mkdir -p "${JETTY_DIR}"
  unzip -q "${JETTY_SOURCES_JAR}" -d "${JETTY_DIR}"
fi

rm -f "${COVERAGE_EXEC}"
export JAVA_TOOL_OPTIONS="-javaagent:${JACOCO_AGENT}=destfile=${COVERAGE_EXEC},append=true,excludes=*junit*:*mockito.*"

# Tests that exercise Connect REST / Jetty more heavily than latch-only tests
./gradlew :connect:runtime:test \
  -Dorg.gradle.parallel=false \
  --max-workers=1 \
  -PmaxParallelForks=1 \
  --tests "org.apache.kafka.connect.integration.ExampleConnectIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.RestExtensionIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.RestForwardingIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.ConnectWorkerIntegrationTest" \
  --tests "org.apache.kafka.connect.integration.StandaloneWorkerIntegrationTest"

rm -rf "${REPORT_DIR}"
REPORT_ARGS=(
  report "${COVERAGE_EXEC}"
  --name "Connect Integration Tests (with Jetty Server)"
  --html "${REPORT_DIR}"
  --classfiles "${JETTY_SERVER_JAR}"
  --sourcefiles "${JETTY_DIR}"
)

while IFS= read -r dir; do
  REPORT_ARGS+=(--classfiles "${dir}")
done < <(find "${ROOT_DIR}" -path "*/build/classes/java/main" -type d | sort)

while IFS= read -r dir; do
  REPORT_ARGS+=(--classfiles "${dir}")
done < <(find "${ROOT_DIR}" -path "*/build/classes/scala/main" -type d | sort)

while IFS= read -r dir; do
  REPORT_ARGS+=(--sourcefiles "${dir}")
done < <(find "${ROOT_DIR}" \( -path "*/src/main/java" -o -path "*/src/main/scala" \) -type d | sort)

java -jar "${JACOCO_CLI}" "${REPORT_ARGS[@]}"

echo "Coverage data: ${COVERAGE_EXEC}"
echo "Jetty classes: ${JETTY_SERVER_JAR}"
echo "Jetty sources: ${JETTY_DIR}"
echo "HTML report: ${REPORT_DIR}/index.html"
