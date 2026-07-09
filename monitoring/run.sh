#!/usr/bin/env bash
# Start the Velo monitoring stack: relay (host) + Prometheus + Grafana (docker).
#
#   ./run.sh            start everything
#   ./run.sh stop       stop everything
#
# Edit prometheus.yml to point at your Velo instance(s). Ports:
#   relay :9200   prometheus :19090   grafana :13000
set -euo pipefail
cd "$(dirname "$0")"

RELAY_PORT=9200
RELAY_PID_FILE=/tmp/velo_slot_relay.pid

if [[ "${1:-start}" == "stop" ]]; then
  docker compose down || true
  [[ -f "$RELAY_PID_FILE" ]] && kill "$(cat "$RELAY_PID_FILE")" 2>/dev/null || true
  rm -f "$RELAY_PID_FILE"
  echo "stopped."
  exit 0
fi

# (re)start relay
[[ -f "$RELAY_PID_FILE" ]] && kill "$(cat "$RELAY_PID_FILE")" 2>/dev/null || true
python3 velo_slot_relay.py "$RELAY_PORT" >/tmp/velo_slot_relay.log 2>&1 &
echo $! > "$RELAY_PID_FILE"
echo "relay started on :$RELAY_PORT (pid $(cat "$RELAY_PID_FILE"))"

python3 gen_dashboard.py
docker compose up -d

cat <<EOF

Velo monitoring is up:
  Prometheus  http://localhost:19090
  Grafana     http://localhost:13000   (anonymous admin; login admin/admin)

Dashboards (folder "Velo"):
  Velo — Global
  Velo — Slot Storage
  Velo — Slot I/O & Replication
EOF
