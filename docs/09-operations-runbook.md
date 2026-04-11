# 09 - Operations Runbook

## Standard Startup Sequence

1. `uv sync`
2. `podman compose up -d`
3. Verify Cassandra and Mongo health
4. Apply Cassandra schema
5. Seed Mongo
6. Run simulator
7. Run stream
8. Run dashboard

## Health Checks

Cassandra ring:

```bash
podman exec -i code-metrics-platform-cassandra-1 nodetool status
```

Mongo replica set:

```bash
podman exec -i code-metrics-platform-mongodb-1 mongosh --quiet --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr, health: m.health}))"
```

Kafka process:

```bash
podman ps --format "table {{.Names}}\t{{.Status}}" | grep kafka
```

## Runtime Commands

Simulator:

```bash
PYTHONPATH=src uv run python src/code_metrics/simulator/generate_logs.py
```

Stream:

```bash
PYTHONPATH=src STREAM_FAIL_ON_DATA_LOSS=false STREAM_STARTING_OFFSETS=latest STREAM_MAX_OFFSETS_PER_TRIGGER=300 CASSANDRA_HOST=127.0.0.1 CASSANDRA_CONTACT_POINTS=127.0.0.1 CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost CASSANDRA_CONNECT_TIMEOUT_SEC=20 CASSANDRA_REQUEST_TIMEOUT_SEC=25 CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

Dashboard:

```bash
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```

## Quick Data Verification

```bash
uv run python - <<'PY'
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
p = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']), request_timeout=10)
cluster = Cluster(['127.0.0.1'], port=9042, execution_profiles={EXEC_PROFILE_DEFAULT:p}, connect_timeout=10, control_connection_timeout=10)
s = cluster.connect('code_metrics')
print('leaderboard rows:', len(list(s.execute('SELECT user_id FROM gold_live_leaderboard LIMIT 10'))))
print('alerts rows:', len(list(s.execute('SELECT alert_id FROM gold_system_alerts LIMIT 10'))))
cluster.shutdown()
PY
```

## Shutdown

```bash
podman compose down
```
