#!/usr/bin/env bash
# Run all @FuzzTest cases over the KafkaApis fuzz targets with the JaCoCo
# agent attached, then render an HTML/XML/CSV coverage report.
#
# See ./README.md for design notes (why JAVA_TOOL_OPTIONS, why one Gradle
# invocation per fuzz test, etc.).
#
# Modes (selectable via the FUZZ_MODE env var):
#
#   fresh     [default] start with an empty coverage.exec and run all
#             fuzz tests from scratch.
#   resume    seed coverage.exec from the committed snapshot
#             ./coverage_results/coverage.exec.xz (xz-compressed,
#             ~500 KB) and append today's runs on top of it. Useful
#             when the previous fuzz pass already cost ~50 minutes
#             and you only want to add incremental coverage.
#   snapshot  same as fresh, but at the very end re-compresses the
#             produced coverage.exec back into
#             ./coverage_results/coverage.exec.xz so the in-tree
#             snapshot stays in sync. Use this whenever you commit
#             a new run.
#
# Output:
#   /tmp/jacoco/coverage.exec            JaCoCo execution data (binary)
#   /tmp/jacoco/report/html/index.html   HTML coverage report
#   /tmp/jacoco/report/coverage.xml      XML coverage report (per-method)
#   /tmp/jacoco/report/coverage.csv      CSV coverage report (per-class)
#
# Optional: `FUZZ_SUITE=all` (default) runs every fuzz target; set
# `FUZZ_SUITE=offset-fetch` to run only `HandleOffsetFetchRequestFuzzTest`
# (useful with `FUZZ_MODE=resume` to append coverage for the new tests only).
#
set -u

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMMITTED_EXEC_XZ="$SCRIPT_DIR/coverage_results/coverage.exec.xz"
cd "$REPO_ROOT"

FUZZ_MODE="${FUZZ_MODE:-fresh}"
case "$FUZZ_MODE" in
    fresh|resume|snapshot) ;;
    *)
        echo "[fuzz] unknown FUZZ_MODE=$FUZZ_MODE (expected: fresh|resume|snapshot)" >&2
        exit 2
        ;;
esac

# Optional: run a subset of fuzz tests (default: full suite). Use with
# FUZZ_MODE=resume to append only new targets onto the committed snapshot.
#   FUZZ_SUITE=all            8 produce + 5 fetch + 3 describe + 4 offset-fetch
#   FUZZ_SUITE=offset-fetch   4 offset-fetch tests only
FUZZ_SUITE="${FUZZ_SUITE:-all}"
case "$FUZZ_SUITE" in
    all|offset-fetch) ;;
    *)
        echo "[fuzz] unknown FUZZ_SUITE=$FUZZ_SUITE (expected: all|offset-fetch)" >&2
        exit 2
        ;;
esac

JACOCO_VERSION="${JACOCO_VERSION:-0.8.12}"
JACOCO_DIR="/tmp/jacoco"
AGENT="$JACOCO_DIR/lib/jacocoagent.jar"
CLI="$JACOCO_DIR/lib/jacococli.jar"
EXEC="$JACOCO_DIR/coverage.exec"
REPORT_DIR="$JACOCO_DIR/report"

# 1. Fetch JaCoCo if not already present.
if [[ ! -f "$AGENT" || ! -f "$CLI" ]]; then
    mkdir -p "$JACOCO_DIR"
    cd "$JACOCO_DIR"
    echo "[fuzz] downloading JaCoCo $JACOCO_VERSION..."
    curl -fsSLo jacoco.zip \
        "https://repo1.maven.org/maven2/org/jacoco/jacoco/${JACOCO_VERSION}/jacoco-${JACOCO_VERSION}.zip"
    unzip -q -o jacoco.zip
    cd "$REPO_ROOT"
fi

# 1b. Either start fresh or seed from the committed snapshot.
if [[ "$FUZZ_MODE" == "resume" ]]; then
    if [[ ! -f "$COMMITTED_EXEC_XZ" ]]; then
        echo "[fuzz] FUZZ_MODE=resume but $COMMITTED_EXEC_XZ does not exist" >&2
        exit 2
    fi
    echo "[fuzz] resuming from $COMMITTED_EXEC_XZ"
    mkdir -p "$JACOCO_DIR"
    xz -dc "$COMMITTED_EXEC_XZ" > "$EXEC"
else
    rm -f "$EXEC"
fi

# 2. Attach the JaCoCo agent to every JVM the JDK launches (including
#    Gradle's forked test JVMs) via JAVA_TOOL_OPTIONS, and tell jazzer-junit
#    to actually fuzz.
export JAVA_TOOL_OPTIONS="-javaagent:${AGENT}=destfile=${EXEC},append=true,\
excludes=*junit*:*mockito.*:com.code_intelligence.*:org.jacoco.*"
export JAZZER_FUZZ=1

# 3. Run each fuzz test in its own JVM (Jazzer fuzzes only the first
#    @FuzzTest per JVM lifetime).
# handleProduceRequest fuzz targets (HandleProduceRequestFuzzTest)
PRODUCE_TESTS=(
    # Original tests (maxDuration = 100s each)
    fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower
    fuzzTestTransactionalParametersSetCorrectly
    fuzzTestNullableTransactionalId
    fuzzTestNoAuthorizedTransactionalRequest
    fuzzTestNoAuthorized
    # Coverage-improvement tests (maxDuration = 20s each)
    fuzzTestUnknownTopicOrPartition
    fuzzTestThrottlingAndAckZeroNoOp
    fuzzTestRequestThrottleDominates
)

# handleFetchRequest fuzz targets (HandleFetchRequestFuzzTest, maxDuration = 20s each)
FETCH_TESTS=(
    fuzzTestFetchConsumer
    fuzzTestFetchFollower
    fuzzTestFetchThrottling
    fuzzTestFetchEmptyInteresting
    fuzzTestFetchDownConversion
)

# handleDescribeTopicPartitionsRequest fuzz targets (ZK arm only;
# HandleDescribeTopicPartitionsRequestFuzzTest, maxDuration = 20s each)
DESCRIBE_TP_TESTS=(
    fuzzTestZkUnsupportedVersion
    fuzzTestZkUnsupportedVersionThrottled
    fuzzTestZkUnsupportedVersionForwarded
)

# handleHeartbeatRequest fuzz targets (HandleHeartbeatRequestFuzzTest,
# maxDuration = 20s each)
HEARTBEAT_TESTS=(
    fuzzTestHeartbeatCoordinatorPath
    fuzzTestHeartbeatStaticMembershipOldIbp
    fuzzTestHeartbeatStaticMembershipSupportedIbp
    fuzzTestHeartbeatAuthorizationDenied
    fuzzTestHeartbeatThrottled
)

# handleOffsetFetchRequest fuzz targets (HandleOffsetFetchRequestFuzzTest,
# maxDuration = 20s each)
OFFSET_FETCH_TESTS=(
    fuzzTestOffsetFetchZk
    fuzzTestOffsetFetchCoordinatorMultiGroup
    fuzzTestOffsetFetchCoordinatorV1To7
    fuzzTestOffsetFetchCoordinatorThrottleAndForwarded
    fuzzTestOffsetFetchCoordinatorAuthAndHandleExceptions
)

mkdir -p "$JACOCO_DIR"

run_one() {
    local fqcn="$1"
    local t="$2"
    echo "=========================================="
    echo "[fuzz] running ${fqcn}.${t}"
    echo "=========================================="
    set +e
    ./gradlew :core:test --no-daemon --rerun-tasks \
        --tests "${fqcn}.${t}" \
        -i 2>&1 | tee "$JACOCO_DIR/run_${t}.log" | \
        grep -E "(Fuzzing|fuzzTest.*PASSED|fuzzTest.*FAILED|Done [0-9]+ runs|<empty input>|exec/s: [0-9]+ |BUILD |^FAILURE|crash-)" || true
    local st=${PIPESTATUS[0]}
    set -e
    if [[ $st -ne 0 ]]; then
        echo "[fuzz] ERROR: gradlew exited with status $st for ${fqcn}.${t}" >&2
        exit $st
    fi
}

if [[ "$FUZZ_SUITE" == "all" ]]; then
    for t in "${PRODUCE_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleProduceRequestFuzzTest" "$t"
    done
    for t in "${FETCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleFetchRequestFuzzTest" "$t"
    done
    for t in "${DESCRIBE_TP_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleDescribeTopicPartitionsRequestFuzzTest" "$t"
    done
    for t in "${HEARTBEAT_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleHeartbeatRequestFuzzTest" "$t"
    done
    for t in "${OFFSET_FETCH_TESTS[@]}"; do
        run_one "unit.kafka.server.fuzz.HandleOffsetFetchRequestFuzzTest" "$t"
    done
fi


# 4. Generate HTML/XML/CSV reports against the freshly compiled core classes.
mkdir -p "$REPORT_DIR"
java -jar "$CLI" report "$EXEC" \
    --classfiles "$REPO_ROOT/core/build/classes/scala/main/kafka/server" \
    --sourcefiles "$REPO_ROOT/core/src/main/scala" \
    --html "$REPORT_DIR/html" \
    --xml  "$REPORT_DIR/coverage.xml" \
    --csv  "$REPORT_DIR/coverage.csv" \
    --name "KafkaApis fuzz coverage (Handle*RequestFuzzTest)"

echo
echo "[fuzz] HTML report : $REPORT_DIR/html/index.html"
echo "[fuzz] XML  report : $REPORT_DIR/coverage.xml"
echo "[fuzz] exec data   : $EXEC"

# 4a. Refresh the trimmed in-tree report under core/src/test/fuzz/coverage_results/
#     (KafkaApis + RequestHandlerHelper only; see refresh_in_repo_coverage.py).
if [[ -f "$REPORT_DIR/coverage.xml" ]]; then
    python3 "$SCRIPT_DIR/refresh_in_repo_coverage.py" || echo "[fuzz] warning: refresh_in_repo_coverage.py failed" >&2
fi

# 4b. Optionally re-snapshot the freshly produced exec file into the
#     committed in-tree location so it survives a Cloud Agent VM
#     recycle and a future `FUZZ_MODE=resume` can pick up where this
#     run left off. xz -9e gives ~80x compression on the mostly-zero
#     coverage data (40 MB -> ~500 KB) which is fine to commit.
if [[ "$FUZZ_MODE" == "snapshot" ]]; then
    echo "[fuzz] re-snapshotting $EXEC -> $COMMITTED_EXEC_XZ"
    mkdir -p "$(dirname "$COMMITTED_EXEC_XZ")"
    xz -9e -f -c "$EXEC" > "$COMMITTED_EXEC_XZ"
    ls -la "$COMMITTED_EXEC_XZ"
fi

# 5. Print a focused summary of both fuzz targets' coverage.
python3 - "$REPORT_DIR/coverage.xml" <<'PY'
import sys, xml.etree.ElementTree as ET

TARGETS = [
    ("KafkaApis.scala",          "handleProduceRequest",                  606, 752),
    ("KafkaApis.scala",          "handleFetchRequest",                    757, 1077),
    ("KafkaApis.scala",          "handleDescribeTopicPartitionsRequest", 1445, 1461),
    ("KafkaApis.scala",          "handleHeartbeatRequest",               1924, 1948),
    ("KafkaApis.scala",          "handleOffsetFetchRequest",             1466, 1630),
    ("RequestHandlerHelper.scala", "sendMaybeThrottle",                   112,  122),
]

tree = ET.parse(sys.argv[1])
root = tree.getroot()

def find_src(filename):
    for pkg in root.findall('package'):
        if pkg.get('name') == 'kafka/server':
            for sf in pkg.findall('sourcefile'):
                if sf.get('name') == filename:
                    return sf
    return None

for filename, name, start, end in TARGETS:
    src = find_src(filename)
    if src is None:
        print("%s: source not found in report" % filename); continue
    lines_in = [l for l in src.findall('line') if start <= int(l.get('nr')) <= end]
    mi = ci = mb = cb = 0
    covered, missed = [], []
    for l in lines_in:
        mi_l = int(l.get('mi')); ci_l = int(l.get('ci'))
        mb_l = int(l.get('mb')); cb_l = int(l.get('cb'))
        mi += mi_l; ci += ci_l; mb += mb_l; cb += cb_l
        nr = int(l.get('nr'))
        if ci_l > 0:    covered.append(nr)
        elif mi_l > 0:  missed.append(nr)
    total_lines = len(covered) + len(missed)
    total_instr = mi + ci
    total_br = mb + cb
    print()
    print("%s (%s lines %d-%d):" % (name, filename, start, end))
    if total_lines:
        print("  Source lines  : %d / %d  (%.1f%%)" % (
            len(covered), total_lines, 100.0*len(covered)/total_lines))
    if total_instr:
        print("  Instructions  : %d / %d  (%.1f%%)" % (
            ci, total_instr, 100.0*ci/total_instr))
    if total_br:
        print("  Branches      : %d / %d  (%.1f%%)" % (
            cb, total_br, 100.0*cb/total_br))
    print("  Missed lines  : %s" % missed)
PY
