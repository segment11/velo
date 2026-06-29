#!/usr/bin/env bash
# =============================================================================
# End-to-end master/slave replication smoke for Velo (with Redis as reference).
#
# Spawns four server processes:
#   - Velo master (slotNumber=1)  + Velo slave (slotNumber=$SLAVE_SLOT_NUMBER)
#   - Redis master               + Redis slave
# Sends SLAVEOF on both slaves, runs a mixed read/write workload via the Python
# helper, then verifies master and slave key/value maps match for both systems.
#
# Topology:
#   SLAVE_SLOT_NUMBER=1 → 1N baseline (slave slotNumber == master slotNumber)
#   SLAVE_SLOT_NUMBER=2 → 2N scale-up (slave slotNumber == 2 × master slotNumber)
#
# Usage:
#   ./scripts/e2e/e2e_2n_replication.sh                 # 2N default
#   SLAVE_SLOT_NUMBER=1 ./scripts/e2e/e2e_2n_replication.sh   # 1N baseline
#   RUN_SECONDS=60 TARGET_QPS=500 ./scripts/e2e/e2e_2n_replication.sh
#
# Requires:
#   - velo jar:    ./gradlew jar
#   - python3 with: redis-py  (pip install redis)
# =============================================================================
set -euo pipefail

# ------------------------- config (env-overridable) -------------------------
SLAVE_SLOT_NUMBER="${SLAVE_SLOT_NUMBER:-2}"          # 1 = 1N, 2 = 2N
RUN_SECONDS="${RUN_SECONDS:-30}"
TARGET_QPS="${TARGET_QPS:-1000}"
WORKER_THREADS="${WORKER_THREADS:-10}"
READ_RATIO="${READ_RATIO:-0.8}"
DATA_LENGTH="${DATA_LENGTH:-100}"
SETTLE_SECONDS="${SETTLE_SECONDS:-10}"
KEEP_DATA="${KEEP_DATA:-false}"

# ------------------------- paths ----------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VELO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VELO_JAR="$VELO_DIR/build/libs/velo-1.0.0.jar"
REDIS_BIN="${REDIS_BIN:-$HOME/.proof/bin/redis-server}"
REDIS_CLI="${REDIS_CLI:-$HOME/.proof/bin/redis-cli}"
JAVA_BIN="${JAVA_BIN:-java}"

HOST=127.0.0.1
SUFFIX="$(date +%s%N)"
BASE_DIR="$(mktemp -d "/tmp/velo-e2e-${SLAVE_SLOT_NUMBER}n-XXXXXX")"

if [ "$SLAVE_SLOT_NUMBER" -eq 1 ]; then
    MODE_DESC="1N baseline"
else
    MODE_DESC="2N scale-up"
fi

echo "==== e2e replication smoke ($MODE_DESC) ===="
echo "velo jar       = $VELO_JAR"
echo "redis bin      = $REDIS_BIN"
echo "topology       = master slot=1, slave slot=$SLAVE_SLOT_NUMBER ($MODE_DESC)"
echo "run seconds    = $RUN_SECONDS"
echo "target qps     = $TARGET_QPS (per system)"
echo "read ratio     = $READ_RATIO"
echo "worker threads = $WORKER_THREADS (per system)"
echo "data length    = $DATA_LENGTH"
echo "settle seconds = $SETTLE_SECONDS"
echo "base dir       = $BASE_DIR"
echo

# ------------------------- preflight ------------------------------------------------
if [ ! -f "$VELO_JAR" ]; then
    echo "ABORT: velo jar not found at $VELO_JAR — run './gradlew jar' first" >&2
    exit 2
fi
if [ ! -x "$REDIS_BIN" ]; then
    echo "ABORT: redis-server not found at $REDIS_BIN" >&2
    exit 2
fi
if [ ! -x "$REDIS_CLI" ]; then
    echo "ABORT: redis-cli not found at $REDIS_CLI" >&2
    exit 2
fi
python3 -c "import redis" 2>/dev/null || {
    echo "ABORT: python3 needs 'redis' package (pip install redis)" >&2
    exit 2
}

# velo subprocesses must run with CWD = velo project root so dyn/src/ is visible
VELO_ROOT="$VELO_DIR"

# ------------------------- helpers --------------------------------------------------
pick_port() {
    python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('$HOST', 0))
print(s.getsockname()[1])
s.close()
"
}

wait_ping() {
    local port=$1 timeout=${2:-60}
    for ((i=0; i<timeout*2; i++)); do
        if "$REDIS_CLI" -h "$HOST" -p "$port" ping >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

wait_replication_up() {
    local port=$1 master_host=$2 master_port=$3 timeout=${4:-90}
    for ((i=0; i<timeout*2; i++)); do
        local info
        info="$("$REDIS_CLI" -h "$HOST" -p "$port" info replication 2>/dev/null || true)"
        if echo "$info" | grep -q 'role:slave' \
           && echo "$info" | grep -q "master_host:$master_host" \
           && echo "$info" | grep -q "master_port:$master_port" \
           && echo "$info" | grep -q 'master_link_status:up'; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

wait_key_value() {
    local port=$1 key=$2 expected=$3 timeout=${4:-90}
    for ((i=0; i<timeout*2; i++)); do
        local val
        val="$("$REDIS_CLI" -h "$HOST" -p "$port" get "$key" 2>/dev/null || true)"
        if [ "$val" = "$expected" ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# ------------------------- process tracking -----------------------------------------
PIDS=()
VELO_PIDS=()
REDIS_PIDS=()

cleanup() {
    echo
    echo "---- teardown ----"
    # kill redis first (fast), then velo (slow JVM shutdown)
    for pid in "${REDIS_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${VELO_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    # wait up to 8s, then force kill anything still alive
    sleep 3
    for pid in "${PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  force killing pid $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    if [ "$KEEP_DATA" = "false" ]; then
        rm -rf "$BASE_DIR"
        echo "  removed $BASE_DIR"
    else
        echo "  keep data at $BASE_DIR"
    fi
}
trap cleanup EXIT

# ------------------------- pick ports ------------------------------------------------
VELO_MASTER_PORT=$(pick_port)
VELO_SLAVE_PORT=$(pick_port)
REDIS_MASTER_PORT=$(pick_port)
REDIS_SLAVE_PORT=$(pick_port)

# ------------------------- write velo properties -------------------------------------
write_velo_props() {
    local name=$1 dir=$2 port=$3 slot_number=$4 binlog_on=$5
    local props_file="$BASE_DIR/$name.properties"
    cat > "$props_file" <<EOF
slotNumber=$slot_number
slotWorkers=1
netWorkers=1
dir=$dir
net.listenAddresses=0.0.0.0:$port
estimateKeyNumber=1000000
kv.lru.maxSize=100000
persist.binlogOn=$binlog_on
EOF
    echo "$props_file"
}

echo "---- starting velo master (slot=1) ----"
VELO_MASTER_DIR="$BASE_DIR/velo-master"
mkdir -p "$VELO_MASTER_DIR"
VELO_MASTER_PROPS=$(write_velo_props velo-master "$VELO_MASTER_DIR" "$VELO_MASTER_PORT" 1 true)
java -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m \
     -Xmx512m -Xms512m \
     -jar "$VELO_JAR" "$VELO_MASTER_PROPS" \
     > "$VELO_MASTER_DIR/server.log" 2>&1 &
VELO_MASTER_PID=$!
VELO_PIDS+=("$VELO_MASTER_PID"); PIDS+=("$VELO_MASTER_PID")
echo "  pid=$VELO_MASTER_PID port=$VELO_MASTER_PORT"

echo "---- starting velo slave (slot=$SLAVE_SLOT_NUMBER) ----"
VELO_SLAVE_DIR="$BASE_DIR/velo-slave"
mkdir -p "$VELO_SLAVE_DIR"
VELO_SLAVE_PROPS=$(write_velo_props velo-slave "$VELO_SLAVE_DIR" "$VELO_SLAVE_PORT" "$SLAVE_SLOT_NUMBER" false)
java -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m \
     -Xmx512m -Xms512m \
     -jar "$VELO_JAR" "$VELO_SLAVE_PROPS" \
     > "$VELO_SLAVE_DIR/server.log" 2>&1 &
VELO_SLAVE_PID=$!
VELO_PIDS+=("$VELO_SLAVE_PID"); PIDS+=("$VELO_SLAVE_PID")
echo "  pid=$VELO_SLAVE_PID port=$VELO_SLAVE_PORT"

echo "---- starting redis master ----"
REDIS_MASTER_DIR="$BASE_DIR/redis-master"
mkdir -p "$REDIS_MASTER_DIR"
"$REDIS_BIN" --port "$REDIS_MASTER_PORT" --bind "$HOST" \
     --save "" --appendonly no \
     --dir "$REDIS_MASTER_DIR" --logfile "$REDIS_MASTER_DIR/redis.log" \
     --daemonize no > "$REDIS_MASTER_DIR/stdout.log" 2>&1 &
REDIS_MASTER_PID=$!
REDIS_PIDS+=("$REDIS_MASTER_PID"); PIDS+=("$REDIS_MASTER_PID")
echo "  pid=$REDIS_MASTER_PID port=$REDIS_MASTER_PORT"

echo "---- starting redis slave ----"
REDIS_SLAVE_DIR="$BASE_DIR/redis-slave"
mkdir -p "$REDIS_SLAVE_DIR"
"$REDIS_BIN" --port "$REDIS_SLAVE_PORT" --bind "$HOST" \
     --save "" --appendonly no \
     --dir "$REDIS_SLAVE_DIR" --logfile "$REDIS_SLAVE_DIR/redis.log" \
     --daemonize no > "$REDIS_SLAVE_DIR/stdout.log" 2>&1 &
REDIS_SLAVE_PID=$!
REDIS_PIDS+=("$REDIS_SLAVE_PID"); PIDS+=("$REDIS_SLAVE_PID")
echo "  pid=$REDIS_SLAVE_PID port=$REDIS_SLAVE_PORT"

# ------------------------- wait for all servers to be ready --------------------------
echo
echo "---- waiting for all servers to respond to PING ----"
for port_label in "velo-master:$VELO_MASTER_PORT" "velo-slave:$VELO_SLAVE_PORT" \
                   "redis-master:$REDIS_MASTER_PORT" "redis-slave:$REDIS_SLAVE_PORT"; do
    label="${port_label%%:*}"
    port="${port_label##*:}"
    if wait_ping "$port"; then
        echo "  $label (port $port) ready"
    else
        echo "ABORT: $label on port $port never answered PING" >&2
        cat "$BASE_DIR/$label/server.log" "$BASE_DIR/$label/stdout.log" 2>/dev/null | tail -30 >&2
        exit 1
    fi
done

# ------------------------- SLAVEOF ---------------------------------------------------
echo
echo "---- SLAVEOF: velo slave -> velo master ----"
"$REDIS_CLI" -h "$HOST" -p "$VELO_SLAVE_PORT" slaveof "$HOST" "$VELO_MASTER_PORT"
if ! wait_replication_up "$VELO_SLAVE_PORT" "$HOST" "$VELO_MASTER_PORT"; then
    echo "ABORT: velo slave never reached master_link_status:up" >&2
    exit 1
fi
echo "  velo slave link up"

echo "---- SLAVEOF: redis slave -> redis master ----"
"$REDIS_CLI" -h "$HOST" -p "$REDIS_SLAVE_PORT" slaveof "$HOST" "$REDIS_MASTER_PORT"
if ! wait_replication_up "$REDIS_SLAVE_PORT" "$HOST" "$REDIS_MASTER_PORT"; then
    echo "ABORT: redis slave never reached master_link_status:up" >&2
    exit 1
fi
echo "  redis slave link up"

# ------------------------- pre-workload markers --------------------------------------
echo
echo "---- writing pre-workload marker keys ----"
VELO_MARKER_KEY="marker:before"
VELO_MARKER_VAL="before-$SUFFIX"
REDIS_MARKER_KEY="$VELO_MARKER_KEY"
REDIS_MARKER_VAL="$VELO_MARKER_VAL"
"$REDIS_CLI" -h "$HOST" -p "$VELO_MASTER_PORT" set "$VELO_MARKER_KEY" "$VELO_MARKER_VAL"
"$REDIS_CLI" -h "$HOST" -p "$REDIS_MASTER_PORT" set "$REDIS_MARKER_KEY" "$REDIS_MARKER_VAL"

echo "  waiting for markers on slaves ..."
if ! wait_key_value "$VELO_SLAVE_PORT" "$VELO_MARKER_KEY" "$VELO_MARKER_VAL"; then
    echo "ABORT: velo slave did not see pre-workload marker (gate may not have opened)" >&2
    exit 1
fi
if ! wait_key_value "$REDIS_SLAVE_PORT" "$REDIS_MARKER_KEY" "$REDIS_MARKER_VAL"; then
    echo "ABORT: redis slave did not see pre-workload marker" >&2
    exit 1
fi
echo "  both slaves readable past initial sync"

# ------------------------- run workload + verify (python) ----------------------------
echo
echo "---- workload + verification (python) ----"
python3 "$SCRIPT_DIR/e2e_replication_workload.py" \
    --host "$HOST" \
    --velo-master-port "$VELO_MASTER_PORT" \
    --velo-slave-port  "$VELO_SLAVE_PORT" \
    --redis-master-port "$REDIS_MASTER_PORT" \
    --redis-slave-port  "$REDIS_SLAVE_PORT" \
    --run-seconds  "$RUN_SECONDS" \
    --target-qps   "$TARGET_QPS" \
    --worker-threads "$WORKER_THREADS" \
    --read-ratio   "$READ_RATIO" \
    --data-length  "$DATA_LENGTH" \
    --settle-seconds "$SETTLE_SECONDS"

WORKLOAD_RC=$?
echo
if [ "$WORKLOAD_RC" -eq 0 ]; then
    echo "RESULT: PASS ($MODE_DESC)"
else
    echo "RESULT: FAIL ($MODE_DESC)"
fi
exit "$WORKLOAD_RC"
