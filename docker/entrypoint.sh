#!/bin/bash
set -e

# ============================================================
# KafkaSortPipeline — Entrypoint Orchestrator
# Runs the full pipeline inside the Docker container:
#   1. Start Kafka (KRaft)
#   2. Create topics
#   3. Generate data
#   4-6. Sort by ID, NAME, CONTINENT
#   7. Verify results
#   8. Print timing report
# ============================================================

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
RECORD_COUNT="${RECORD_COUNT:-50000000}"

# --- Timing helpers -----------------------------------------
declare -a PHASE_NAMES
declare -a PHASE_TIMES_NS

now_ns() {
    date +%s%N
}

# Format nanoseconds → "X.Xs"
format_ns() {
    local ns=$1
    local secs=$(( ns / 1000000000 ))
    local frac=$(( (ns % 1000000000) / 100000000 ))   # one decimal
    echo "${secs}.${frac}s"
}

record_phase() {
    local name="$1"
    local start_ns="$2"
    local end_ns="$3"
    local elapsed=$(( end_ns - start_ns ))
    PHASE_NAMES+=("$name")
    PHASE_TIMES_NS+=("$elapsed")
}

PIPELINE_START=$(now_ns)

# ============================================================
# Phase 1 — Kafka Startup
# ============================================================
echo "=========================================="
echo "[Phase 1] Starting Kafka broker (KRaft)..."
echo "=========================================="
PHASE_START=$(now_ns)

# Generate cluster ID and format storage
KAFKA_CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
echo "Cluster ID: $KAFKA_CLUSTER_ID"
$KAFKA_HOME/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /app/config/kafka-kraft.properties

# Set broker heap options
export KAFKA_HEAP_OPTS="-Xmx384m -Xms384m -XX:+UseG1GC -XX:MaxGCPauseMillis=20"

# Start broker in background
$KAFKA_HOME/bin/kafka-server-start.sh /app/config/kafka-kraft.properties &
KAFKA_PID=$!

# Wait for broker readiness (max 60s)
echo "Waiting for Kafka broker to become ready..."
WAIT_START=$(date +%s)
while true; do
    if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; then
        echo "Kafka broker is ready."
        break
    fi
    ELAPSED=$(( $(date +%s) - WAIT_START ))
    if [ "$ELAPSED" -ge 60 ]; then
        echo "ERROR: Kafka broker did not start within 60 seconds." >&2
        exit 1
    fi
    sleep 2
done

PHASE_END=$(now_ns)
record_phase "Kafka startup" "$PHASE_START" "$PHASE_END"

# ============================================================
# Phase 2 — Topic Creation
# ============================================================
echo ""
echo "=========================================="
echo "[Phase 2] Creating Kafka topics..."
echo "=========================================="
PHASE_START=$(now_ns)

create_topic() {
    local name=$1
    local partitions=$2
    echo "  Creating topic: $name (partitions=$partitions)"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
        --create --topic "$name" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config compression.type=zstd \
        --config retention.ms=3600000
}

create_topic "source"    4
create_topic "id"        1
create_topic "name"      1
create_topic "continent" 1

echo "Topics created successfully."

PHASE_END=$(now_ns)
record_phase "Topic creation" "$PHASE_START" "$PHASE_END"

# ============================================================
# Phase 3 — Data Generation
# ============================================================
echo ""
echo "=========================================="
echo "[Phase 3] Generating $RECORD_COUNT records..."
echo "=========================================="
PHASE_START=$(now_ns)

java -Xmx256m -Xms128m -XX:+UseG1GC \
    -cp /app/kafka-sort-pipeline.jar \
    com.pipeline.generate.DataGeneratorApp "$RECORD_COUNT"

PHASE_END=$(now_ns)
record_phase "Data generation (${RECORD_COUNT})" "$PHASE_START" "$PHASE_END"

# ============================================================
# Phases 4-6 — Sorting (sequential)
# ============================================================
for SORT_KEY in ID NAME CONTINENT; do
    PHASE_NUM=$((${#PHASE_NAMES[@]} + 1))
    SORT_LOWER=$(echo "$SORT_KEY" | tr '[:upper:]' '[:lower:]')
    echo ""
    echo "=========================================="
    echo "[Phase $PHASE_NUM] Sorting by $SORT_LOWER..."
    echo "=========================================="
    PHASE_START=$(now_ns)

    java -Xmx512m -Xms256m -XX:+UseG1GC \
        -cp /app/kafka-sort-pipeline.jar \
        com.pipeline.sort.ExternalSortApp "$SORT_KEY"

    PHASE_END=$(now_ns)
    record_phase "Sort by $SORT_LOWER" "$PHASE_START" "$PHASE_END"
done

# ============================================================
# Phase 7 — Verification
# ============================================================
echo ""
echo "=========================================="
echo "[Phase 7] Verifying results..."
echo "=========================================="
PHASE_START=$(now_ns)

java -Xmx128m -Xms64m -XX:+UseG1GC \
    -cp /app/kafka-sort-pipeline.jar \
    com.pipeline.verify.VerifyApp

PHASE_END=$(now_ns)
record_phase "Verification" "$PHASE_START" "$PHASE_END"

# ============================================================
# Phase 8 — Timing Report
# ============================================================
PIPELINE_END=$(now_ns)
TOTAL_NS=$(( PIPELINE_END - PIPELINE_START ))

echo ""
echo ""
echo "=== Pipeline Run Report ==="
printf "%-35s %s\n" "Phase" "Wall Time"
echo "-----------------------------------------"
for i in "${!PHASE_NAMES[@]}"; do
    printf "%-35s %s\n" "${PHASE_NAMES[$i]}" "$(format_ns "${PHASE_TIMES_NS[$i]}")"
done
echo "-----------------------------------------"
printf "%-35s %s\n" "TOTAL WALL CLOCK" "$(format_ns "$TOTAL_NS")"
echo ""

# ============================================================
# Cleanup — Stop Kafka broker
# ============================================================
echo "Stopping Kafka broker..."
$KAFKA_HOME/bin/kafka-server-stop.sh || true
# Wait for broker process to exit
if [ -n "$KAFKA_PID" ]; then
    wait "$KAFKA_PID" 2>/dev/null || true
fi
echo "Kafka broker stopped. Pipeline complete."
