#!/usr/bin/env bash
# =============================================================================
# run-consumer.sh
# Starts the pipeline in sink-only mode (consumer / indexing node).
#
# Usage:
#   ./scripts/run-consumer.sh [path-to-properties]
#
# Defaults to config/sink.properties when no argument is given.
# Pass config/pipeline.properties to run sources AND sinks in the same JVM.
#
# Properties read from the properties file:
#   jvm.heap.xms        - initial heap size (e.g. 512m)
#   jvm.heap.xmx        - max heap size     (e.g. 512m)  — must equal xms
#   jvm.gc.opts         - full GC flags (e.g. -XX:+UseZGC ...)
#   jvm.gc.log.dir      - directory for GC logs (created if missing)
#   app.log.dir         - directory for application logs
#
# GC recommendation: Generational ZGC (Java 21+)
#   Generational ZGC extends ZGC with separate young/old generations, giving
#   higher throughput than non-generational ZGC while keeping sub-millisecond
#   pause targets.  For a streaming consumer application where poll() latency
#   and consistent throughput matter more than peak throughput, Generational
#   ZGC is the right choice over Shenandoah or G1GC (higher pauses).
#
#   Key flags used:
#     -XX:+UseZGC                  Enable ZGC
#     -XX:+ZGenerational           Enable generational mode (required on Java 21;
#                                  default on Java 23+, harmless to keep explicit)
#     -XX:SoftMaxHeapSize=400m     Soft limit — ZGC will try to stay below this
#                                  but can grow to Xmx if needed.  Keeps 20%
#                                  headroom so GC doesn't trigger under burst.
#     -XX:ZCollectionInterval=120  Force a GC cycle every 120s even if heap
#                                  isn't full.  Prevents long GC pauses after
#                                  idle periods.
#     -XX:ConcGCThreads=2          Parallel GC threads (tune to available cores)
#
#   Always set Xms == Xmx to avoid resizing pauses and OS memory pressure on
#   containerised hosts.
# =============================================================================

set -euo pipefail

# ---- Locate script directory and project root --------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_HOME="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ---- Properties file ---------------------------------------------------------
PROPS_FILE="${1:-${APP_HOME}/config/sink.properties}"

if [[ ! -f "${PROPS_FILE}" ]]; then
    echo "ERROR: Properties file not found: ${PROPS_FILE}" >&2
    exit 1
fi

echo "Using properties file: ${PROPS_FILE}"

# ---- Helper: read a property value from the file ----------------------------
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

# ---- Read JVM settings from properties file ----------------------------------
XMS=$(get_prop "jvm.heap.xms" "512m")
XMX=$(get_prop "jvm.heap.xmx" "512m")
GC_OPTS=$(get_prop "jvm.gc.opts" "-XX:+UseZGC -XX:+ZGenerational -XX:SoftMaxHeapSize=400m -XX:ZCollectionInterval=120 -XX:ConcGCThreads=2")
GC_LOG_DIR=$(get_prop "jvm.gc.log.dir" "logs/gc")
APP_LOG_DIR=$(get_prop "app.log.dir" "logs")

# Make GC log dir relative to APP_HOME if not absolute
[[ "${GC_LOG_DIR}" != /* ]] && GC_LOG_DIR="${APP_HOME}/${GC_LOG_DIR}"
[[ "${APP_LOG_DIR}" != /* ]] && APP_LOG_DIR="${APP_HOME}/${APP_LOG_DIR}"

mkdir -p "${GC_LOG_DIR}"
mkdir -p "${APP_LOG_DIR}"

# ---- FAT JAR -----------------------------------------------------------------
JAR="${APP_HOME}/target/kafka-ingest-pipeline-1.0.0-fat.jar"
if [[ ! -f "${JAR}" ]]; then
    echo "ERROR: Fat jar not found at ${JAR}. Run 'mvn clean package' first." >&2
    exit 1
fi

# ---- GC logging flags --------------------------------------------------------
# -Xlog:gc* — unified JVM logging, covers GC, heap, safepoints
# filesize=10m — roll each GC log at 10 MB
# filecount=6  — keep 6 rotated files (older auto-deleted)
GC_LOG_FILE="${GC_LOG_DIR}/gc-consumer-%t.log"
GC_LOG_FLAGS="-Xlog:gc*,safepoint:file=${GC_LOG_FILE}:time,uptime,pid,level,tags:filesize=10m,filecount=6"

# ---- Build full JVM arguments ------------------------------------------------
JVM_ARGS=(
    "-Xms${XMS}"
    "-Xmx${XMX}"
    ${GC_OPTS}                   # word-split intentional — multiple flags
    ${GC_LOG_FLAGS}
    "-Dapp.log.dir=${APP_LOG_DIR}"
    "-Dlog4j2.configurationFile=classpath:log4j2.xml"
    "-Djava.awt.headless=true"
    "--enable-native-access=ALL-UNNAMED"   # suppress Snappy native-access warning
)

# ---- Main class --------------------------------------------------------------
MAIN_CLASS="io.github.adityassharma.kafka.pipeline.PipelineMain"

echo "============================================================"
echo "  Kafka Pipeline (sink node)"
echo "  Properties : ${PROPS_FILE}"
echo "  Heap       : Xms=${XMS} Xmx=${XMX}"
echo "  GC         : ${GC_OPTS}"
echo "  GC logs    : ${GC_LOG_DIR}"
echo "  App logs   : ${APP_LOG_DIR}"
echo "============================================================"

exec java "${JVM_ARGS[@]}" -cp "${JAR}" "${MAIN_CLASS}" "${PROPS_FILE}"
