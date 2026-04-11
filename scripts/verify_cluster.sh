#!/usr/bin/env bash
set -euo pipefail

echo "[1/2] MongoDB replica set status"
podman exec -i code-metrics-platform-mongodb-1 mongosh --quiet --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr, health: m.health}))"

echo

echo "[2/2] Cassandra ring status"
podman exec -i code-metrics-platform-cassandra-1 nodetool status

echo

echo "Cluster verification completed."
