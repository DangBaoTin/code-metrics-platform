# 14 - Objective 5.1-5.5 Mapping Matrix

This matrix maps assignment objectives to concrete implementation evidence.

## Legend

- Status: Done / Partial
- Evidence: code module, schema/table, dashboard view, runtime command

| Objective | Requirement Summary | Status | Implementation Evidence |
|---|---|---|---|
| 5.1 | Real-time telemetry ingestion and live monitoring | Done | Streaming consumer from Kafka in `src/code_metrics/processing/stream_leaderboard.py`; simulator producer in `src/code_metrics/simulator/generate_logs.py`; dashboard view `Live Leaderboard` in `src/code_metrics/dashboard/app.py` |
| 5.2 | Distributed storage design with medallion layering | Done | Cassandra schema in `storage/init_cassandra.cql` with Bronze/Silver/Gold tables; Silver build in `src/code_metrics/processing/batch_etl.py` |
| 5.3 | Operational alerts (latency + suspicious activity) | Done | `HIGH_LATENCY` and `CHEAT_DETECTED` logic in `src/code_metrics/processing/stream_leaderboard.py`; rendered in `System Alerts` view in `src/code_metrics/dashboard/app.py` |
| 5.4 | Batch analytics for BI stakeholders | Done | Gold BI outputs in `src/code_metrics/processing/batch_etl.py`: engagement, difficulty heatmap, subscription revenue, instructor report; displayed in corresponding dashboard tabs |
| 5.5 | Data Science / AI outputs and recommendations | Done | Logistic Regression dropout + ALS recommendation + adaptive profile in `src/code_metrics/processing/batch_etl.py`; shown in `Dropout Risk` and `AI Recommender` tabs |

## Architecture and HA Evidence

| Area | Evidence |
|---|---|
| Cassandra masterless ring | 3-node Cassandra services in `docker-compose.yml`; ring verification via `nodetool status` |
| MongoDB Primary-Secondary HA | `mongodb`, `mongodb2`, `mongodb3` + `mongodb-rs-init` in `docker-compose.yml` |
| Orchestration | Airflow DAG `airflow/dags/code_metrics_dag.py` calling batch ETL |

## Operational Validation Checklist

1. Simulator terminal continuously emits events.
2. Stream starts with reset mode and no fatal termination.
3. Dashboard `Live Leaderboard` shows rows (not waiting message).
4. Dashboard `System Alerts` shows HIGH_LATENCY/CHEAT_DETECTED records.
5. Batch run populates BI/AI tabs.

## Recommended Demonstration Commands

```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
PYTHONPATH=src STREAM_FAIL_ON_DATA_LOSS=false STREAM_STARTING_OFFSETS=latest STREAM_MAX_OFFSETS_PER_TRIGGER=300 CASSANDRA_HOST=127.0.0.1 CASSANDRA_CONTACT_POINTS=127.0.0.1 CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost CASSANDRA_CONNECT_TIMEOUT_SEC=20 CASSANDRA_REQUEST_TIMEOUT_SEC=25 CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
PYTHONPATH=src uv run python -m code_metrics.cli batch
```
