# Mac A-Z Setup Guide: Code Metrics Platform

This guide will walk you through setting up this complex, distributed data engineering project on a brand new MacBook from scratch.

## 1. System Prerequisites

You will need **Homebrew** (the package manager for Mac). If you don't have it, open your terminal and run:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## 2. Install Required Software

We need Podman (for running databases), Java 17 (required by Spark), and `uv` (a blazingly fast Python package manager).

```bash
# Install the core system dependencies
brew install podman podman-compose openjdk@17 uv
```

**Configure Java Environment Variables:**
Spark absolutely requires Java 17 to run. Add it to your shell profile (assuming you use `zsh`):
```bash
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@17"' >> ~/.zshrc
source ~/.zshrc
```

## 3. Configure Podman (CRITICAL STEP)

Because we are running Cassandra (x3), Kafka, Zookeeper, MongoDB, and Airflow locally, the default Podman virtual machine does not have enough resources and **will crash or timeout**. You must allocate at least 9-10GB of RAM.

```bash
# Initialize a Podman machine with 4 CPU cores, 10GB of RAM, and 50GB of disk space
podman machine init --cpus=4 --memory=10240 --disk-size=50

# Start the Podman engine
podman machine start
```

## 4. Setup the Project

Navigate to where you want the project to live, clone it, and enter the directory.

```bash
# (Assuming you clone your repo here, replace with your actual repo URL)
git clone <your-repo-url>
cd code-metrics-platform
```

Install the Python dependencies using `uv` (this will automatically create a `.venv` virtual environment for you):
```bash
uv sync
```

## 5. Boot Up the Infrastructure

Now it's time to start the 3-node Cassandra cluster, Kafka, MongoDB, and Airflow.

```bash
# Start all containers in the background (this may take a few minutes the first time to download images)
podman compose up -d

# Check if they are downloading/running
podman compose ps
```

**Wait for the databases to become healthy.** We have a script that checks this for you:
```bash
bash scripts/verify_cluster.sh
```
*(Do not proceed until the script says all Cassandra nodes and Kafka are healthy. If Cassandra is stuck, run `podman compose restart cassandra cassandra-node2 cassandra-node3` and wait 45 seconds).*

## 6. Initialize the Databases

Once the cluster is healthy, we need to create the Cassandra tables and insert seed data into MongoDB.

```bash
# 1. Create Cassandra Keyspace and Tables
podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql

# 2. Seed MongoDB with user data
PYTHONPATH=src uv run python -m code_metrics.cli seed-mongo
```

## 7. Running the Platform

To see the platform working, you need to open **3 separate terminal windows/tabs**. Make sure you are in the `code-metrics-platform` directory in all of them.

### Terminal 1: Real-Time Streaming
This PySpark job reads from Kafka and writes to Cassandra.
```bash
export CASSANDRA_HOST=127.0.0.1
export CASSANDRA_CONTACT_POINTS=127.0.0.1
export CASSANDRA_ALLOWED_HOSTS=127.0.0.1,localhost
export CASSANDRA_CONNECT_TIMEOUT_SEC=20
export CASSANDRA_REQUEST_TIMEOUT_SEC=25
export CASSANDRA_WRITE_CONSISTENCY=LOCAL_ONE
export CASSANDRA_READ_CONSISTENCY=LOCAL_ONE
export PYTHONPATH=src
export STREAM_FAIL_ON_DATA_LOSS=false
export STREAM_STARTING_OFFSETS=latest

uv run spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  src/code_metrics/processing/stream_leaderboard.py --reset-state
```

### Terminal 2: Data Simulator
This acts as your "users", generating fake code submissions and sending them to Kafka.
```bash
PYTHONPATH=src uv run python -m code_metrics.cli simulate
```

### Terminal 3: Streamlit Dashboard
The UI to view the real-time data.
```bash
PYTHONPATH=src uv run python -m code_metrics.cli dashboard
```

---

## 8. Running the Nightly Batch Job

The batch job crunches historical data to create ML-based predictions (like dropout probabilities). 

**Important:** Because running 3 active terminals + Cassandra + a Heavy Batch Job all on one laptop will overwhelm it, **stop the simulator (Terminal 2) before running the batch job.**

Run the batch job using the custom script:
```bash
bash scripts/run_batch.sh
```
*(Once completed, you can restart your simulator).*
