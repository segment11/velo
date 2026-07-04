#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VELO_DIR=$(cd "$SCRIPT_DIR/../../.." && pwd)

HOST=${HOST:-127.0.0.1}
NODE_COUNT=${NODE_COUNT:-4}
SLOT_NUMBER=${SLOT_NUMBER:-4}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-90}
KEEP_DATA=${KEEP_DATA:-false}
RUN_MIGRATION=${RUN_MIGRATION:-true}
RUN_FAILOVER=${RUN_FAILOVER:-true}
JAVA_BIN=${JAVA_BIN:-java}
VELO_JAR=${VELO_JAR:-"$VELO_DIR/build/libs/velo-1.0.0.jar"}
BASE_DIR=${BASE_DIR:-$(mktemp -d /tmp/velo-cluster-e2e.XXXXXX)}

PIDS=()

cleanup() {
    local status=$?
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1 || true
        fi
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" >/dev/null 2>&1 || true
    done
    if [[ "$KEEP_DATA" != "true" ]]; then
        rm -rf "$BASE_DIR"
    else
        echo "Keeping e2e data under: $BASE_DIR"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

pick_ports() {
    python3 - "$NODE_COUNT" <<'PY'
import socket
import sys

count = int(sys.argv[1])
sockets = []
ports = []
try:
    for _ in range(count):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
        ports.append(sock.getsockname()[1])
    print(" ".join(str(port) for port in ports))
finally:
    for sock in sockets:
        sock.close()
PY
}

require_file() {
    if [[ ! -f "$1" ]]; then
        echo "Missing required file: $1" >&2
        echo "Build it first with: ./gradlew jar" >&2
        exit 2
    fi
}

require_redis_py() {
    python3 - <<'PY'
try:
    import redis  # noqa: F401
except Exception as exc:
    raise SystemExit(f"redis-py is required: {exc}")
PY
}

write_node_props() {
    local idx=$1
    local port=$2
    local node_dir="$BASE_DIR/node-$idx"
    mkdir -p "$node_dir"
    cat > "$node_dir/velo.properties" <<EOF
slotNumber=$SLOT_NUMBER
slotWorkers=1
netWorkers=1
dir=$node_dir/data
net.listenAddresses=0.0.0.0:$port
replicaAnnounceIp=$HOST
replicaAnnouncePort=$port
estimateKeyNumber=1000000
kv.lru.maxSize=100000
persist.binlogOn=true
clusterEnabled=true
EOF
}

start_node() {
    local idx=$1
    local port=$2
    local node_dir="$BASE_DIR/node-$idx"
    local props_file="$node_dir/velo.properties"
    (
        cd "$VELO_DIR"
        exec "$JAVA_BIN" \
            -XX:+UseZGC \
            -XX:+ZGenerational \
            -XX:MaxDirectMemorySize=64m \
            -Xmx512m \
            -Xms512m \
            -jar "$VELO_JAR" "$props_file"
    ) > "$node_dir/server.log" 2>&1 &
    PIDS+=("$!")
    echo "Started Velo node-$idx on $HOST:$port pid=${PIDS[-1]}"
}

main() {
    if [[ "$NODE_COUNT" -lt 4 ]]; then
        echo "NODE_COUNT must be at least 4 for full slot coverage plus failover checks" >&2
        exit 2
    fi

    require_file "$VELO_JAR"
    require_redis_py
    mkdir -p "$BASE_DIR"

    read -r -a PORTS <<< "$(pick_ports)"
    echo "E2E workspace: $BASE_DIR"
    echo "Ports: ${PORTS[*]}"

    for i in "${!PORTS[@]}"; do
        write_node_props "$i" "${PORTS[$i]}"
        start_node "$i" "${PORTS[$i]}"
    done

    python3 "$SCRIPT_DIR/velo_cluster_e2e.py" \
        --host "$HOST" \
        --ports "${PORTS[@]}" \
        --base-dir "$BASE_DIR" \
        --timeout "$TIMEOUT_SECONDS" \
        --slot-number "$SLOT_NUMBER" \
        --run-migration "$RUN_MIGRATION" \
        --run-failover "$RUN_FAILOVER"
}

main "$@"
