# 📊 Code Metrics Data Platform

An enterprise-grade, distributed data engineering pipeline designed to ingest, process, and serve high-velocity telemetry logs for a simulated EdTech coding platform.

## 🏗️ Architecture
This platform utilizes a distributed, decoupled architecture to handle high-throughput event streams without bottlenecking the application layer.

* **Data Generation:** Python-based telemetry simulator generating concurrent student code submission logs and operational metrics.
* **Ingestion Layer:** **Apache Kafka** acts as a message broker to absorb traffic spikes and queue streams (`raw_submissions`, `system_metrics`).
* **Processing Engine:** **Apache Spark** running a Lambda architecture:
  * *Spark Streaming:* Real-time micro-batching for live leaderboards and latency monitoring.
  * *Spark SQL:* Nightly batch processing for heavy ETL and historical analytics.
* **Distributed Storage:**
  * **Apache Cassandra:** A Masterless Ring architecture utilizing a Medallion (Bronze/Silver/Gold) structure for time-series log storage.
  * **MongoDB:** A Primary-Secondary Replica Set for highly available operational metadata.
* **Downstream Serving:** **Streamlit** + **Plotly** + **Matplotlib** for interactive BI dashboards and operational alert reporting.
* **Orchestration:** **Apache Airflow** for scheduled nightly batch ETL.

Implemented objective coverage now includes:
* Real-time: Live leaderboard, latency monitor (`> 5s`), and cheat detection using 30-second windows + multi-IP anomaly.
* Batch BI: Engagement score, difficulty heatmap, subscription revenue dashboard, and instructor report card.
* Data Science outputs: Logistic Regression dropout prediction, ALS-based next-problem recommendation, and adaptive difficulty profiles.

## 🛠️ Technology Stack
* **Environment Manager:** `uv` (Lightning-fast Python package manager)
* **Containerization:** `podman` / `docker-compose`
* **Language:** Python 3.10+
* **Core Frameworks:** PySpark 3.5.0, Kafka-Python, Cassandra-Driver, Streamlit, Apache Airflow

## 📂 Project Structure
```text
code-metrics-platform/
├── docker-compose.yml        # Infrastructure setup (Kafka, Cassandra, Mongo, Airflow, Postgres)
├── airflow/
│   └── dags/                 # Airflow DAG definitions
├── src/
│   └── code_metrics/
│       ├── simulator/        # Log generation scripts
│       ├── processing/       # Spark Streaming and Batch ETL jobs
│       ├── storage/          # MongoDB seed scripts
│       └── dashboard/        # Streamlit BI application
├── storage/                  # Database schema initializations (CQL/JSON)
└── scripts/                  # Utility scripts (e.g., cluster verification)
```

## 📚 Full Documentation Suite
Detailed technical documentation is available in `docs/`:

- `docs/README.md` (index)
- `docs/01-getting-started.md`
- `docs/02-system-architecture.md`
- `docs/03-data-model-and-schema.md`
- `docs/04-distributed-and-ha-design.md`
- `docs/05-streaming-pipeline-walkthrough.md`
- `docs/06-batch-pipeline-walkthrough.md`
- `docs/07-dashboard-and-serving-layer.md`
- `docs/08-airflow-orchestration.md`
- `docs/09-operations-runbook.md`
- `docs/10-troubleshooting-knowledge-base.md`
- `docs/11-academic-report.md`
- `docs/14-objective-mapping-matrix.md`
- `docs/15-final-submission-checklist.md`

## ⚡ Common Daily Commands

Setup once per session:
```bash
source .venv/bin/activate
uv sync
```

Start/stop infrastructure:
```bash
podman compose up -d
podman compose down
```

Initialize data stores:
```bash
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
```

Run the platform (3 terminals):
```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```

Verify cluster health:
```bash
bash scripts/verify_cluster.sh
```

## 🚀 Quick Start Guide

### 0. Command Checklist (Recommended Order)
Run these in order for a clean local startup:
```bash
uv sync
podman compose up -d
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
uv run python src/code_metrics/storage/init_mongo.py
uv run python src/code_metrics/simulator/generate_logs.py
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py
uv run streamlit run src/code_metrics/dashboard/app.py
```

### 1. Prerequisites
Ensure you have the following installed on your local machine:
* [Podman](https://podman.io/) or Docker Desktop
* [uv](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
* Python 3.10+ (recommended: managed through `uv`)
* **Java 17** (Strictly required for Apache Spark execution)
  * *macOS Installation:*
    ```bash
    brew install openjdk@17
    sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
    ```

### 2. Initialize the Environment
Clone the repository and sync dependencies:
```bash
git clone [https://github.com/YOUR_USERNAME/code-metrics-platform.git](https://github.com/YOUR_USERNAME/code-metrics-platform.git)
cd code-metrics-platform
uv sync
```
Optional check:
```bash
uv run python -c "from airflow.providers.standard.operators.python import PythonOperator; print('airflow import ok')"
```

Optional CLI check:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli --help
```

Optional console-script check:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli --help
```

### 3. Start the Infrastructure
Start all infrastructure services (including Airflow + Postgres):
```bash
podman compose up -d
```
Wait until services are healthy. To monitor Airflow startup:
```bash
podman compose logs -f airflow
```

To monitor scheduler separately:
```bash
podman compose logs -f airflow-scheduler
```

Mongo replica set is auto-initialized by `mongodb-rs-init` service.

### 4. Initialize Storage Schemas
Create Cassandra keyspace/tables:
```bash
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
```
If you see `ConnectionRefused` right after startup, wait until Cassandra is ready and retry:
```bash
until podman exec -i code-metrics-platform-cassandra-1 cqlsh -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; do
  echo "Waiting for Cassandra..."
  sleep 3
done
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
```
The CQL file is located at `storage/init_cassandra.cql`.

Seed Mongo metadata:
```bash
uv run python src/code_metrics/storage/init_mongo.py
```
Equivalent CLI command:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
```

Optional: seed larger Mongo datasets for demo/testing:
```bash
MONGO_SEED_USERS=300 MONGO_SEED_PROBLEMS=180 MONGO_SEED_TXNS=500 MONGO_SEED_RATINGS=900 uv run python src/code_metrics/storage/init_mongo.py
```

### 5. Run the Real-Time Pipeline (Multi-Terminal Setup)
Open three terminals in the project root:

**Terminal 1: Start the Log Simulator**
```bash
uv run python src/code_metrics/simulator/generate_logs.py
```
Equivalent CLI command:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
```
The simulator now auto-loads `user_id` and `problem_id` from MongoDB collections (`users`, `problems`).
It emits to Kafka topics `raw_submissions` and `system_metrics`.
Optional custom Mongo URI:
```bash
MONGO_URI=mongodb://localhost:27017/ uv run python src/code_metrics/simulator/generate_logs.py
```
Optional periodic entity refresh (seconds, default `60`; set `0` to disable):
```bash
ENTITY_REFRESH_SECONDS=30 uv run python src/code_metrics/simulator/generate_logs.py
```

**Terminal 2: Start the Spark Streaming Job**
```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py
```
This single streaming job consumes both topics, writes Bronze raw telemetry, updates live leaderboard, and generates `gold_system_alerts`.

Optional clean reset before streaming (recommended after reseeding Mongo):
```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

Optional tuning:
```bash
STREAM_STARTING_OFFSETS=earliest STREAM_CHECKPOINT_BASE=./storage/checkpoints/stream_leaderboard uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py
```

**Terminal 3: Launch the BI Dashboard**
```bash
uv run streamlit run src/code_metrics/dashboard/app.py
```
Equivalent CLI command:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```
Use the **System Alerts** view to monitor `HIGH_LATENCY` and `CHEAT_DETECTED` events.
Use **Subscription Dashboard**, **Instructor Report Card**, **Dropout Risk**, and **AI Recommender** views for batch BI/AI outputs.

### 6. Use Airflow for Nightly Batch ETL
1. Open Airflow UI at `http://localhost:8080`
2. Login with:
  * Username: `admin`
  * Password: `admin`
3. Unpause DAG `nightly_code_metrics_etl`
4. Trigger DAG manually (or wait for schedule)

The DAG uses `PythonOperator` to run the batch ETL entrypoint (now canonical at `src/code_metrics/processing/batch_etl.py`).

By default, batch ETL now refreshes Gold snapshot tables (`gold_engagement_scores`, `gold_difficulty_heatmap`) by truncating them before writing. This prevents stale rows from previous runs.

Batch ETL now also materializes:
- `gold_subscription_revenue`
- `gold_instructor_report_card`
- `gold_dropout_predictions`
- `gold_next_problem_recommendations`
- `gold_adaptive_difficulty_profiles`

To disable this behavior (incremental-like behavior), run with:
```bash
BATCH_RESET_GOLD=0 uv run python src/code_metrics/processing/batch_etl.py
```

---

## 🛑 Troubleshooting Common Issues

### 1. Spark Error: `Unable to locate a Java Runtime` or `bad array subscript`
**Cause:** Spark requires the Java Virtual Machine (JVM) to execute, but macOS cannot find it in the system paths.
**Solution:** Ensure you installed `openjdk@17` via Homebrew and executed the `sudo ln -sfn ...` symlink command detailed in the Prerequisites section.

### 2. Spark Error: `Failed to find data source: kafka`
**Cause:** PySpark version mismatch. If you accidentally installed Spark 4.x, the `3.5.0` Kafka connector will fail to resolve.
**Solution:** Force `uv` to downgrade PySpark to the stable LTS version:
```bash
uv add pyspark==3.5.0
```
Then, always run your job with the packages flag:
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ...`

### 3. Dashboard Error: `background_gradient requires matplotlib`
**Cause:** Streamlit's Pandas styling engine requires Matplotlib under the hood to calculate the color gradients for the Difficulty Heatmap.
**Solution:** Add the missing dependency to your environment:
```bash
uv add matplotlib
```

### 4. Airflow Login `401 Unauthorized`
If login fails after restarts, fully recreate containers:
```bash
podman compose down
podman compose up -d postgres zookeeper kafka cassandra cassandra2 cassandra3 mongodb mongodb2 mongodb3 mongodb-rs-init airflow airflow-scheduler
```
Then wait for `airflow` logs to settle and login again with `admin/admin`.

### 5. Airflow import warning in editor
If VS Code shows unresolved import for Airflow operators, ensure the Python interpreter points to this project's `.venv` and run:
```bash
uv sync
```

### 5.1 Airflow webserver exits with Gunicorn timeout / stale PID
Symptoms:
- `No response from gunicorn master within 120 seconds`
- `Already running on PID ... airflow-webserver.pid is stale`

Recovery:
```bash
podman compose stop airflow airflow-scheduler
podman compose rm -f airflow airflow-scheduler
podman compose up -d airflow airflow-scheduler
podman compose logs -f airflow
```

If still failing, remove stale PID inside the container and restart:
```bash
podman exec -it code-metrics-platform-airflow-1 bash -lc "rm -f /opt/airflow/airflow-webserver.pid /opt/airflow/airflow-scheduler.pid"
podman compose restart airflow airflow-scheduler
```

### 5.2 Airflow DAG fails with Cassandra connection refused on 127.0.0.1:9042
Symptom in task logs:
- `NoHostAvailable ... Tried connecting to [('127.0.0.1', 9042)]`

Cause:
- Airflow container uses localhost default for Cassandra, but Cassandra runs in another service (`cassandra`).

Fix:
```bash
podman compose up -d --force-recreate airflow airflow-scheduler
podman compose logs -f airflow-scheduler
```

Expected environment inside Airflow services:
- `CASSANDRA_HOST=cassandra`
- `CASSANDRA_PORT=9042`

### 6. Verify Replica Set and Ring
Use these checks after `podman compose up -d` to confirm distributed topology is healthy.

MongoDB replica set status:
```bash
podman exec -i code-metrics-platform-mongodb-1 mongosh --quiet --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr, health: m.health}))"
```

Expected: one `PRIMARY`, two `SECONDARY`, all with `health: 1`.

Cassandra ring status:
```bash
podman exec -i code-metrics-platform-cassandra-1 nodetool status
```

Expected: three `UN` nodes in keyspace ring (node1, node2, node3).

Optional one-shot verifier script:
```bash
bash scripts/verify_cluster.sh
```

### 7. Dashboard shows no data / System Alerts Mongo DNS error
Symptoms:
- "Waiting for telemetry data from Kafka"
- "No batch data found"
- Mongo errors mentioning `mongodb:27017`, `mongodb2:27017`, `mongodb3:27017`

Common causes:
1. Streaming job is not actually running.
2. Simulator was stopped (`Exit Code 130` means interrupted with Ctrl+C).
3. Command was pasted with markdown link syntax (example: `[stream_leaderboard.py](...)`) which causes shell parse errors.

Recovery steps:
```bash
podman compose up -d
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
uv run python src/code_metrics/simulator/generate_logs.py
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
uv run streamlit run src/code_metrics/dashboard/app.py
```

Host-mode Mongo note:
- If your terminal app injects container hostnames into `MONGO_URI`, unset it before starting dashboard/simulator:
```bash
unset MONGO_URI
```

### 8. Cassandra write error: Not enough replicas for LOCAL_QUORUM
Symptom:
- `Not enough replicas available for query at consistency LOCAL_QUORUM (2 required but only 1 alive)`

Why it happens:
- In local runs, your Spark task may only see one Cassandra replica as available at write time.

Fix for local development:
```bash
CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run python src/code_metrics/processing/batch_etl.py
```

For Airflow, set the same env on `airflow` and `airflow-scheduler` services in `docker-compose.yml`:
- `CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE`
- `CASSANDRA_READ_CONSISTENCY=LOCAL_ONE`

Then recreate Airflow services:
```bash
podman compose up -d --force-recreate airflow airflow-scheduler
```

### 9. Cassandra ChannelPool warnings with `NotYetConnectedException`
Symptoms:
- Repeated warnings similar to:
  - `WARN ChannelPool ... failed to send request (java.nio.channels.NotYetConnectedException)`
  - peer addresses like `10.x.x.x:9042`

What it usually means in local host-mode runs:
- Spark is connected through `localhost:9042`, but the Cassandra ring advertises internal container peer IPs.
- The driver attempts peer connections and logs warnings for unreachable peers from host networking.
- If dashboard and writes continue updating, these warnings are mostly noise.

Treat as a real problem only if you also see:
- stream job exits with a fatal exception
- dashboard stops updating entirely
- repeated `NoHostAvailable` errors for your configured host

Stability tips:
```bash
export CASSANDRA_HOST=127.0.0.1
export CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost
export CASSANDRA_READ_CONSISTENCY=LOCAL_ONE
export CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE
```

And ensure Cassandra is healthy before starting stream:
```bash
podman exec -i code-metrics-platform-cassandra-1 nodetool status
```

### 10. After pulling latest code, BI/AI tables are missing
If dashboard sections for Subscription/Instructor/AI show no table errors, re-apply schema and re-seed metadata:
```bash
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
```
Then run batch ETL once:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli batch
```

### 11. Python 3.12 error: `ModuleNotFoundError: No module named 'distutils'`
Cause:
- PySpark 3.5 still imports `distutils.version` internally, but Python 3.12 removed `distutils` from stdlib.

Resolution in this repo:
- A compatibility shim is already included under `src/distutils/`.
- Run commands with `PYTHONPATH=src` so the shim is discoverable.

Example:
```bash
PYTHONPATH=src uv run python -m code_metrics.cli batch
```

### 12. Stream error: `There are [1] sources in the checkpoint offsets and now there are [2] sources`
Cause:
- The streaming query shape changed (for example, system alerts now combine multiple sources), but old checkpoint metadata still exists.

Fix:
```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

If you prefer manual cleanup, remove old stream checkpoints under:
- `storage/checkpoints/stream_leaderboard/`

### 13. Stream `--reset-state` fails with `Connection reset by peer` on Cassandra
Cause:
- Cassandra can still be warming up sockets right when reset starts, even if container status looks healthy.

Current behavior:
- The stream now performs readiness checks before reset and retries reset automatically.

If it still fails once, retry after a few seconds:
```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

### 14. Stream warns `OperationTimedOut` / `received only 0 responses` and dashboard shows no Kafka data
Symptoms:
- `WARN bronze metrics batch failed ... Unable to connect to any servers`
- `Coordinator node timed out waiting for replica nodes' responses`
- Dashboard keeps showing waiting state

Cause:
- The first micro-batches can overwhelm Cassandra or arrive before the ring is fully responsive.

Current behavior in this repo:
- Stream writes now retry automatically per row and per batch.
- Kafka ingest is throttled via `STREAM_MAX_OFFSETS_PER_TRIGGER` (default `1200`) to reduce startup pressure.

Recommended launch command:
```bash
PYTHONPATH=src STREAM_STARTING_OFFSETS=latest STREAM_MAX_OFFSETS_PER_TRIGGER=600 CASSANDRA_BATCH_MAX_ATTEMPTS=8 CASSANDRA_BATCH_RETRY_DELAY_SEC=2 CASSANDRA_ROW_MAX_ATTEMPTS=4 CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE CASSANDRA_READ_CONSISTENCY=LOCAL_ONE uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```

If warnings persist for more than ~1 minute, verify ring health before restarting stream:
```bash
podman exec -i code-metrics-platform-cassandra-1 nodetool status
```

### 15. Stream fails with `Partition ... offset was changed ... failOnDataLoss`
Symptom:
- `StreamingQueryException: Partition raw_submissions-0's offset was changed ...`

Cause:
- Kafka topic offsets were reset/aged out (topic recreation, retention, or checkpoint history mismatch).

Current behavior in this repo:
- Stream now sets Kafka `failOnDataLoss` from env and defaults it to `false`, so it continues instead of terminating.

Recommended recovery command:
```bash
PYTHONPATH=src STREAM_FAIL_ON_DATA_LOSS=false STREAM_STARTING_OFFSETS=latest uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 src/code_metrics/processing/stream_leaderboard.py --reset-state
```
