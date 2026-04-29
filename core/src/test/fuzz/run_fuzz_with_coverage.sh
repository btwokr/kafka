#!/usr/bin/env bash
# Run all @FuzzTest cases in KafkaApisFuzzTest.scala with the JaCoCo agent
# attached, then render an HTML/XML/CSV coverage report.
#
# See ./README.md for design notes (why JAVA_TOOL_OPTIONS, why one Gradle
# invocation per fuzz test, etc.).
#
# Output:
#   /tmp/jacoco/coverage.exec            JaCoCo execution data (binary)
#   /tmp/jacoco/report/html/index.html   HTML coverage report
#   /tmp/jacoco/report/coverage.xml      XML coverage report (per-method)
#   /tmp/jacoco/report/coverage.csv      CSV coverage report (per-class)
#
# Wall-clock time: roughly 25-30 minutes for 5 x 100s fuzz tests
# plus Gradle startup overhead.
#
set -u

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$REPO_ROOT"

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

rm -f "$EXEC"

# 2. Attach the JaCoCo agent to every JVM the JDK launches (including
#    Gradle's forked test JVMs) via JAVA_TOOL_OPTIONS, and tell jazzer-junit
#    to actually fuzz.
export JAVA_TOOL_OPTIONS="-javaagent:${AGENT}=destfile=${EXEC},append=true,\
excludes=*junit*:*mockito.*:com.code_intelligence.*:org.jacoco.*"
export JAZZER_FUZZ=1

# 3. Run each fuzz test in its own JVM (Jazzer fuzzes only the first
#    @FuzzTest per JVM lifetime).
TESTS=(
    fuzzTestProduceResponseContainsNewLeaderOnNotLeaderOrFollower
    fuzzTestTransactionalParametersSetCorrectly
    fuzzTestNullableTransactionalId
    fuzzTestNoAuthorizedTransactionalRequest
    fuzzTestNoAuthorized
)

mkdir -p "$JACOCO_DIR"
for t in "${TESTS[@]}"; do
    echo "=========================================="
    echo "[fuzz] running $t"
    echo "=========================================="
    ./gradlew :core:test --no-daemon --rerun-tasks \
        --tests "unit.kafka.server.KafkaApisFuzzTest.${t}" \
        -i 2>&1 | tee "$JACOCO_DIR/run_${t}.log" | \
        grep -E "(Fuzzing|fuzzTest.*PASSED|fuzzTest.*FAILED|Done [0-9]+ runs|<empty input>|exec/s: [0-9]+ |BUILD |^FAILURE)" || true
done

# 4. Generate HTML/XML/CSV reports against the freshly compiled core classes.
mkdir -p "$REPORT_DIR"
java -jar "$CLI" report "$EXEC" \
    --classfiles "$REPO_ROOT/core/build/classes/scala/main/kafka/server" \
    --sourcefiles "$REPO_ROOT/core/src/main/scala" \
    --html "$REPORT_DIR/html" \
    --xml  "$REPORT_DIR/coverage.xml" \
    --csv  "$REPORT_DIR/coverage.csv" \
    --name "KafkaApisFuzzTest fuzz coverage"

echo
echo "[fuzz] HTML report : $REPORT_DIR/html/index.html"
echo "[fuzz] XML  report : $REPORT_DIR/coverage.xml"
echo "[fuzz] exec data   : $EXEC"

# 5. Print a focused summary of handleProduceRequest coverage.
python3 - "$REPORT_DIR/coverage.xml" <<'PY'
import sys, xml.etree.ElementTree as ET

START_LINE, END_LINE = 606, 752  # body of handleProduceRequest in KafkaApis.scala

tree = ET.parse(sys.argv[1])
root = tree.getroot()

src = None
for pkg in root.findall('package'):
    if pkg.get('name') == 'kafka/server':
        for sf in pkg.findall('sourcefile'):
            if sf.get('name') == 'KafkaApis.scala':
                src = sf
if src is None:
    print("KafkaApis.scala source coverage not found in report")
    sys.exit(1)

lines_in = [l for l in src.findall('line') if START_LINE <= int(l.get('nr')) <= END_LINE]
mi = ci = mb = cb = 0
covered, missed = [], []
for l in lines_in:
    mi_l = int(l.get('mi')); ci_l = int(l.get('ci'))
    mb_l = int(l.get('mb')); cb_l = int(l.get('cb'))
    mi += mi_l; ci += ci_l; mb += mb_l; cb += cb_l
    nr = int(l.get('nr'))
    if ci_l > 0:        covered.append(nr)
    elif mi_l > 0:      missed.append(nr)

total_lines = len(covered) + len(missed)
total_instr = mi + ci
total_br = mb + cb

print()
print("handleProduceRequest (KafkaApis.scala lines %d-%d):" % (START_LINE, END_LINE))
print("  Source lines covered     : %d / %d  (%.1f%%)" % (len(covered), total_lines, 100.0*len(covered)/max(total_lines,1)))
print("  Bytecode instructions    : %d / %d  (%.1f%%)" % (ci, total_instr, 100.0*ci/max(total_instr,1)))
if total_br:
    print("  Branches                 : %d / %d  (%.1f%%)" % (cb, total_br, 100.0*cb/max(total_br,1)))
print()
print("  Missed source lines      : %s" % missed)
PY
