# 01 - Getting Started

## Objective

Stand up a local distributed data platform for telemetry ingestion, real-time monitoring, and nightly analytics.

## Prerequisites

- Podman or Docker with Compose compatibility
- Python 3.10+
- uv package manager
- Java 17 for Spark

## One-Time Setup

```bash
uv sync
```

## Start Infrastructure

```bash
podman compose up -d
```

Validate health:

```bash
c
```

Optional detailed view:

```bash
podman ps --format "table {{.Names}}\t{{.Status}}"
podman exec -i code-metrics-platform-cassandra-1 nodetool status
```

## Initialize Databases

```bash
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
```

## Run the Platform (3 terminals)

Before starting app jobs, run one preflight check:

```bash
bash scripts/verify_cluster.sh
```

Terminal A (Simulator):

```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
```

Terminal B (Streaming):

```bash
PYTHONPATH=src STREAM_FAIL_ON_DATA_LOSS=false STREAM_STARTING_OFFSETS=latest STREAM_MAX_OFFSETS_PER_TRIGGER=300 CASSANDRA_HOST=127.0.0.1 CASSANDRA_CONTACT_POINTS=127.0.0.1 CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost CASSANDRA_CONNECT_TIMEOUT_SEC=20 CASSANDRA_REQUEST_TIMEOUT_SEC=25 CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

Terminal C (Dashboard):

```bash
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```

Recommended startup order for best stability:

1. Terminal B (Streaming)
2. Terminal A (Simulator)
3. Terminal C (Dashboard)

## Nightly Batch ETL

```bash
PYTHONPATH=src uv run python -m code_metrics.cli batch
```

Or trigger via Airflow DAG `nightly_code_metrics_etl`.
