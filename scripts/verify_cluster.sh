#!/usr/bin/env bash
set -euo pipefail

RETRIES="${VERIFY_RETRIES:-30}"
SLEEP_SEC="${VERIFY_SLEEP_SEC:-3}"

retry_until_ok() {
	local name="$1"
	local cmd="$2"
	local i
	for ((i=1; i<=RETRIES; i++)); do
		if eval "$cmd" >/dev/null 2>&1; then
			echo "[ok] ${name}"
			return 0
		fi
		echo "[wait ${i}/${RETRIES}] ${name} not ready yet"
		sleep "$SLEEP_SEC"
	done
	echo "[fail] ${name} not ready after ${RETRIES} attempts"
	return 1
}

echo "[1/4] Kafka broker readiness"
retry_until_ok \
	"Kafka (kafka:29092)" \
	"podman exec -i code-metrics-platform-kafka-1 kafka-topics --bootstrap-server kafka:29092 --list"

echo
echo "[2/4] MongoDB replica set readiness"
retry_until_ok \
	"Mongo replica set (PRIMARY + 2 SECONDARY)" \
	"podman exec -i code-metrics-platform-mongodb-1 mongosh --quiet --eval \"const m=rs.status().members; const p=m.filter(x=>x.stateStr==='PRIMARY'&&x.health===1).length; const s=m.filter(x=>x.stateStr==='SECONDARY'&&x.health===1).length; quit((p>=1&&s>=2)?0:1);\""

podman exec -i code-metrics-platform-mongodb-1 mongosh --quiet --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr, health: m.health}))"
echo

echo "[3/4] Cassandra CQL readiness"
retry_until_ok \
	"Cassandra CQL" \
	"podman exec -i code-metrics-platform-cassandra-1 cqlsh -e \"SELECT now() FROM system.local;\""

echo
echo "[4/4] Cassandra ring health"
retry_until_ok \
	"Cassandra ring has 3 UN nodes" \
	"[[ \$(podman exec -i code-metrics-platform-cassandra-1 nodetool status | grep -c '^UN') -ge 3 ]]"
echo "[info] Cassandra ring status"
podman exec -i code-metrics-platform-cassandra-1 nodetool status

echo

echo "Preflight verification completed. Infrastructure is ready."
