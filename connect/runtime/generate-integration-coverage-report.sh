#!/usr/bin/env bash
# Generate JaCoCo HTML coverage for Connect integration tests, including all
# external libraries on :connect:runtime runtimeClasspath.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="${ROOT_DIR}/connect/runtime"
JACOCO_DIR="${ROOT_DIR}/.jacoco"
DEPS_LIST="${RUNTIME_DIR}/runtimeClasspath-external-libraries.txt"
DEPS_SOURCES_DIR="${JACOCO_DIR}/dependency-sources"
JACOCO_ZIP="${JACOCO_DIR}/jacoco-0.8.14.zip"
JACOCO_AGENT_JAR="${JACOCO_DIR}/org.jacoco.agent-0.8.14.jar"
JACOCO_AGENT="${JACOCO_DIR}/jacocoagent.jar"
JACOCO_CLI="${JACOCO_DIR}/jacococli.jar"
COVERAGE_EXEC="${RUNTIME_DIR}/coverage.exec"
REPORT_DIR="${RUNTIME_DIR}/jacoco-integration-html-report"

JACOCO_VERSION="0.8.14"
MAVEN_BASE="https://repo1.maven.org/maven2"

# Default: external library sources are not added to the report (classfiles only).
INCLUDE_LIBRARY_SOURCES=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Generate JaCoCo HTML coverage for Connect integration tests.

Options:
  --with-library-sources   Download Maven -sources.jar artifacts for runtimeClasspath
                           libraries and add them to the report (line-level source view).
                           By default, only library classfiles/JARs are included.
  -h, --help               Show this help

Environment:
  SKIP_TESTS=1             Reuse existing coverage.exec instead of running tests.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-library-sources)
      INCLUDE_LIBRARY_SOURCES=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Coordinates omitted from --classfiles (BOM-only, or duplicate classes vs Jakarta deps).
SKIP_CLASSFILE_COORDINATES=(
  "com.fasterxml.jackson:jackson-bom:2.21.2"
  "javax.activation:activation:1.1.1"
  "javax.activation:javax.activation-api:1.2.0"
  "com.sun.activation:jakarta.activation:2.0.1"
  "javax.xml.bind:jaxb-api:2.3.1"
)

should_skip_classfiles() {
  local coordinate="$1"
  local skip
  for skip in "${SKIP_CLASSFILE_COORDINATES[@]}"; do
    [[ "${coordinate}" == "${skip}" ]] && return 0
  done
  return 1
}

mkdir -p "${JACOCO_DIR}" "${DEPS_SOURCES_DIR}"

if [[ ! -f "${DEPS_LIST}" ]]; then
  echo "Missing ${DEPS_LIST}; regenerate with:" >&2
  echo "  ./gradlew :connect:runtime:listExternalRuntimeClasspathLibs -I gradle/list-connect-runtime-external-deps.init.gradle" >&2
  exit 1
fi

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

find_gradle_binary_jar() {
  local group="$1" module="$2" version="$3"
  find "${HOME}/.gradle/caches/modules-2/files-2.1/${group}/${module}/${version}" \
    -name "${module}-${version}.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" 2>/dev/null | head -1
}

maven_coordinate_path() {
  local group="$1"
  echo "${group//.//}"
}

extract_jar_classes() {
  local jar="$1"
  local dest="$2"
  if [[ ! -f "${dest}/.extracted" ]]; then
    mkdir -p "${dest}"
    # Exclude multi-release JAR entries; JaCoCo errors on duplicate versioned classes.
    unzip -q "${jar}" -d "${dest}" -x 'META-INF/versions/*'
    touch "${dest}/.extracted"
  fi
}

prepare_external_library() {
  local coordinate="$1"
  local group module version
  IFS=':' read -r group module version <<< "${coordinate}"

  local binary_jar
  binary_jar="$(find_gradle_binary_jar "${group}" "${module}" "${version}")"
  if [[ -z "${binary_jar}" || ! -f "${binary_jar}" ]]; then
    echo "WARN: no binary jar in Gradle cache for ${coordinate} (skipped)" >&2
    return 1
  fi

  local safe_id="${group}__${module}__${version}"
  safe_id="${safe_id//[^a-zA-Z0-9._-]/_}"

  if ! should_skip_classfiles "${coordinate}"; then
    # Extract classes so multi-release JAR entries (META-INF/versions/*) do not confuse JaCoCo.
    local classes_dir="${JACOCO_DIR}/dependency-classes/${safe_id}"
    extract_jar_classes "${binary_jar}" "${classes_dir}"
    EXTERNAL_CLASSFILES+=("${classes_dir}")
  fi

  if [[ "${INCLUDE_LIBRARY_SOURCES}" != "1" ]]; then
    return 0
  fi

  local sources_dir="${DEPS_SOURCES_DIR}/${safe_id}"
  local sources_jar="${JACOCO_DIR}/${safe_id}-sources.jar"
  local maven_path
  maven_path="$(maven_coordinate_path "${group}")"

  if [[ ! -d "${sources_dir}" ]]; then
    if [[ ! -f "${sources_jar}" ]]; then
      if ! curl -fsSL -o "${sources_jar}" \
        "${MAVEN_BASE}/${maven_path}/${module}/${version}/${module}-${version}-sources.jar"; then
        rm -f "${sources_jar}"
        echo "WARN: no sources jar for ${coordinate} (classes only in report)" >&2
        return 0
      fi
    fi
    mkdir -p "${sources_dir}"
    unzip -q "${sources_jar}" -d "${sources_dir}"
  fi

  EXTERNAL_SOURCEFILES+=("${sources_dir}")
}

EXTERNAL_CLASSFILES=()
EXTERNAL_SOURCEFILES=()

cd "${ROOT_DIR}"
./gradlew :connect:runtime:testClasses -q

while IFS= read -r line || [[ -n "${line}" ]]; do
  line="${line%%#*}"
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [[ -z "${line}" ]] && continue
  prepare_external_library "${line}" || true
done < "${DEPS_LIST}"

if [[ "${INCLUDE_LIBRARY_SOURCES}" == "1" ]]; then
  echo "External libraries: ${#EXTERNAL_CLASSFILES[@]} classfiles, ${#EXTERNAL_SOURCEFILES[@]} source trees"
else
  echo "External libraries: ${#EXTERNAL_CLASSFILES[@]} classfiles (library sources disabled)"
fi

if [[ "${SKIP_TESTS:-0}" != "1" ]]; then
  rm -f "${COVERAGE_EXEC}"
  export JAVA_TOOL_OPTIONS="-javaagent:${JACOCO_AGENT}=destfile=${COVERAGE_EXEC},append=true,excludes=*junit*:*mockito.*"

  ./gradlew :connect:runtime:test \
    -Dorg.gradle.parallel=false \
    --max-workers=1 \
    -PmaxParallelForks=1 \
    --tests "org.apache.kafka.connect.integration.ExampleConnectIntegrationTest" \
    --tests "org.apache.kafka.connect.integration.RestExtensionIntegrationTest" \
    --tests "org.apache.kafka.connect.integration.RestForwardingIntegrationTest" \
    --tests "org.apache.kafka.connect.integration.ConnectWorkerIntegrationTest" \
    --tests "org.apache.kafka.connect.integration.StandaloneWorkerIntegrationTest"
else
  if [[ ! -f "${COVERAGE_EXEC}" ]]; then
    echo "SKIP_TESTS=1 but ${COVERAGE_EXEC} does not exist" >&2
    exit 1
  fi
  echo "SKIP_TESTS=1: reusing existing ${COVERAGE_EXEC}"
fi

rm -rf "${REPORT_DIR}"
REPORT_ARGS=(
  report "${COVERAGE_EXEC}"
  --name "Connect Integration Tests (Kafka + runtimeClasspath libraries)"
  --html "${REPORT_DIR}"
)

for jar in "${EXTERNAL_CLASSFILES[@]}"; do
  REPORT_ARGS+=(--classfiles "${jar}")
done

if [[ "${INCLUDE_LIBRARY_SOURCES}" == "1" ]]; then
  for src in "${EXTERNAL_SOURCEFILES[@]}"; do
    REPORT_ARGS+=(--sourcefiles "${src}")
  done
fi

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
echo "Dependency list: ${DEPS_LIST}"
echo "Include library sources: ${INCLUDE_LIBRARY_SOURCES}"
echo "External classfiles: ${#EXTERNAL_CLASSFILES[@]}"
echo "External source trees: ${#EXTERNAL_SOURCEFILES[@]}"
echo "HTML report: ${REPORT_DIR}/index.html"
