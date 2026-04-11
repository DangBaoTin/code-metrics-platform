# 15 - Final Submission Checklist

Use this checklist right before final demo/submission.

## A. Environment and Infrastructure

- [ ] `uv sync` completed without errors
- [ ] `podman compose up -d` completed
- [ ] Cassandra ring healthy (`UN` nodes)
- [ ] Mongo replica set healthy (PRIMARY + SECONDARY members)
- [ ] Kafka container is running

## B. Data Initialization

- [ ] Cassandra schema applied from `storage/init_cassandra.cql`
- [ ] Mongo seed completed successfully
- [ ] Optional large seed applied (if required by demo)

## C. Runtime Pipeline Validation

- [ ] Simulator runs continuously and prints event logs
- [ ] Streaming job starts and stays alive
- [ ] Dashboard opens without DB connection failures
- [ ] `Live Leaderboard` no longer shows waiting message
- [ ] `System Alerts` contains HIGH_LATENCY and/or CHEAT_DETECTED rows

## D. Batch Validation

- [ ] Batch ETL run completes successfully
- [ ] `Difficulty Heatmap` tab populated
- [ ] `Subscription Dashboard` tab populated
- [ ] `Instructor Report Card` tab populated
- [ ] `Dropout Risk` and `AI Recommender` tabs populated

## E. Objective Compliance (5.1-5.5)

- [ ] Real-time ingest + monitoring demonstrated
- [ ] Distributed medallion storage demonstrated
- [ ] Alerting logic demonstrated
- [ ] BI outputs demonstrated
- [ ] AI outputs demonstrated

Reference matrix: `docs/14-objective-mapping-matrix.md`

## F. Required Commands for Live Demo

1. Start simulator:

```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
```

2. Start stream:

```bash
PYTHONPATH=src STREAM_FAIL_ON_DATA_LOSS=false STREAM_STARTING_OFFSETS=latest STREAM_MAX_OFFSETS_PER_TRIGGER=300 CASSANDRA_HOST=127.0.0.1 CASSANDRA_CONTACT_POINTS=127.0.0.1 CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost CASSANDRA_CONNECT_TIMEOUT_SEC=20 CASSANDRA_REQUEST_TIMEOUT_SEC=25 CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

3. Start dashboard:

```bash
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```

4. Trigger batch (for BI/AI views):

```bash
PYTHONPATH=src uv run python -m code_metrics.cli batch
```

## G. Suggested Screenshot Set

- [ ] Architecture diagram
- [ ] Cassandra ring status
- [ ] Mongo replica set status
- [ ] Live leaderboard populated
- [ ] System alerts populated
- [ ] Heatmap
- [ ] Revenue dashboard
- [ ] Instructor report
- [ ] Dropout risk chart
- [ ] AI recommender table

## H. Packaging

- [ ] README updated
- [ ] docs/ index updated
- [ ] No sensitive credentials in files
- [ ] Final repository state verified
