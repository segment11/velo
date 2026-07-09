#!/usr/bin/env bash
# Start the Velo monitoring stack: Prometheus + Grafana (docker).
#
#   ./run.sh            start everything
#   ./run.sh stop       stop everything
#
# Edit prometheus.yml to point at your Velo instance(s). Ports:
#   prometheus :19090   grafana :13000
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-start}" == "stop" ]]; then
  docker compose down || true
  echo "stopped."
  exit 0
fi

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
