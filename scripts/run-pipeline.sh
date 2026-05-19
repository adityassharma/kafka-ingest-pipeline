#!/usr/bin/env bash
# =============================================================================
# run-pipeline.sh
# Starts the pipeline in combined mode (sources AND sinks in one JVM).
#
# Usage:
#   ./scripts/run-pipeline.sh [path-to-properties]
#
# Defaults to config/pipeline.properties when no argument is given.
# For separate source/sink JVMs use run-source.sh and run-sink.sh instead.
#
# See run-sink.sh for GC rationale and flags documentation.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROPS_FILE="${1:-${APP_HOME}/config/pipeline.properties}"

if [[ ! -f "${PROPS_FILE}" ]]; then
    echo "ERROR: Properties file not found: ${PROPS_FILE}" >&2
    exit 1
fi

echo "Using properties file: ${PROPS_FILE}"

get_prop() {
    local key="$1"
    local default="${2:-}"
    local val
    val=$(grep -E "^${key}\s*=" "${PROPS_FILE}" 2>/dev/null \
          | head -1 \
          | sed 's/^[^=]*=\s*//' \
          | tr -d '\r')
    echo "${val:-${default}}"
}

XMS=$(get_prop "jvm.heap.xms" "512m")
XMX=$(get_prop "jvm.heap.xmx" "512m")
GC_OPTS=$(get_prop "jvm.gc.opts" "-XX:+UseZGC -XX:+ZGenerational -XX:SoftMaxHeapSize=400m -XX:ZCollectionInterval=120 -XX:ConcGCThreads=2")
GC_LOG_DIR=$(get_prop "jvm.gc.log.dir" "logs/gc")
APP_LOG_DIR=$(get_prop "app.log.dir" "logs")

[[ "${GC_LOG_DIR}" != /* ]] && GC_LOG_DIR="${APP_HOME}/${GC_LOG_DIR}"
[[ "${APP_LOG_DIR}" != /* ]] && APP_LOG_DIR="${APP_HOME}/${APP_LOG_DIR}"

mkdir -p "${GC_LOG_DIR}"
mkdir -p "${APP_LOG_DIR}"

JAR="${APP_HOME}/target/kafka-ingest-pipeline-1.0.0-fat.jar"
if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: Fat jar not found at ${JAR}. Run 'mvn clean package' first." >&2
    exit 1
fi

GC_LOG_FILE="${GC_LOG_DIR}/gc-pipeline-%t.log"
GC_LOG_FLAGS="-Xlog:gc*,safepoint:file=${GC_LOG_FILE}:time,uptime,pid,level,tags:filesize=10m,filecount=6"

JVM_ARGS=(
    "-Xms${XMS}"
    "-Xmx${XMX}"
    ${GC_OPTS}
    ${GC_LOG_FLAGS}
    "-Dapp.log.dir=${APP_LOG_DIR}"
    "-Dlog4j2.configurationFile=classpath:log4j2.xml"
    "-Djava.awt.headless=true"
    "--enable-native-access=ALL-UNNAMED"   # suppress Snappy native-access warning
)

MAIN_CLASS="io.github.adityassharma.kafka.pipeline.PipelineMain"

echo "============================================================"
echo "  Kafka Pipeline (combined source + sink)"
echo "  Properties : ${PROPS_FILE}"
echo "  Heap       : Xms=${XMS} Xmx=${XMX}"
echo "  GC         : ${GC_OPTS}"
echo "  GC logs    : ${GC_LOG_DIR}"
echo "  App logs   : ${APP_LOG_DIR}"
echo "============================================================"

exec java "${JVM_ARGS[@]}" -cp "${JAR}" "${MAIN_CLASS}" "${PROPS_FILE}"
