#!/usr/bin/env bash
# =============================================================================
# run-producer.sh
# Starts the multi-threaded Kafka producer.
#
# Usage:
#   ./scripts/run-producer.sh [path-to-producer.properties]
#
# Defaults to config/producer.properties when no argument is given.
# See run-consumer.sh for GC rationale and flags documentation.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROPS_FILE="${1:-${APP_HOME}/config/producer.properties}"

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

XMS=$(get_prop "jvm.heap.xms" "256m")
XMX=$(get_prop "jvm.heap.xmx" "256m")
GC_OPTS=$(get_prop "jvm.gc.opts" "-XX:+UseZGC -XX:+ZGenerational -XX:SoftMaxHeapSize=200m -XX:ZCollectionInterval=120 -XX:ConcGCThreads=2")
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

GC_LOG_FILE="${GC_LOG_DIR}/gc-producer-%t.log"
GC_LOG_FLAGS="-Xlog:gc*,safepoint:file=${GC_LOG_FILE}:time,uptime,pid,level,tags:filesize=10m,filecount=6"

JVM_ARGS=(
    "-Xms${XMS}"
    "-Xmx${XMX}"
    ${GC_OPTS}
    ${GC_LOG_FLAGS}
    "-Dapp.log.dir=${APP_LOG_DIR}"
    "-Dlog4j2.configurationFile=classpath:log4j2.xml"
    "-Djava.awt.headless=true"
)

MAIN_CLASS="io.github.adityassharma.kafka.producer.ProducerMain"

echo "============================================================"
echo "  Kafka Producer"
echo "  Properties : ${PROPS_FILE}"
echo "  Heap       : Xms=${XMS} Xmx=${XMX}"
echo "  GC         : ${GC_OPTS}"
echo "  GC logs    : ${GC_LOG_DIR}"
echo "  App logs   : ${APP_LOG_DIR}"
echo "============================================================"

exec java "${JVM_ARGS[@]}" -cp "${JAR}" "${MAIN_CLASS}" "${PROPS_FILE}"
