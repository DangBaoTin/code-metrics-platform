# 📊 Code Metrics Data Platform

An enterprise-grade, distributed data engineering pipeline designed to ingest, process, and serve high-velocity telemetry logs for a simulated EdTech coding platform.

## 🏗️ Architecture
This platform utilizes a distributed, decoupled architecture to handle high-throughput event streams without bottlenecking the application layer.

* **Data Generation:** Python-based telemetry simulator generating concurrent student code submission logs.
* **Ingestion Layer:** **Apache Kafka** acts as a message broker to absorb traffic spikes and queue streams.
* **Processing Engine:** **Apache Spark** running a Lambda architecture:
  * *Spark Streaming:* Real-time micro-batching for live leaderboards and latency monitoring.
  * *Spark SQL:* Nightly batch processing for heavy ETL and historical analytics.
* **Distributed Storage:** * **Apache Cassandra:** A Masterless Ring architecture utilizing a Medallion (Bronze/Silver/Gold) structure for time-series log storage.
  * **MongoDB:** A Primary-Secondary Replica Set for highly available operational metadata.
* **Downstream Serving:** **Streamlit** + **Plotly** + **Matplotlib** for interactive Business Intelligence dashboards.

## 🛠️ Technology Stack
* **Environment Manager:** `uv` (Lightning-fast Python package manager)
* **Containerization:** `podman` / `docker-compose`
* **Language:** Python 3.10+
* **Core Frameworks:** PySpark 3.5.0, Kafka-Python, Cassandra-Driver, Streamlit

## 📂 Project Structure
```text
code-metrics-platform/
├── docker-compose.yml        # Infrastructure setup (Kafka, Cassandra, Mongo)
├── simulator/                # Log generation scripts
├── processing/               # Spark Streaming and Batch ETL jobs
├── storage/                  # Database schema initializations (CQL/JSON)
└── dashboard/                # Streamlit BI applications
```

## 🚀 Quick Start Guide

### 1. Prerequisites
Ensure you have the following installed on your local machine:
* [Podman](https://podman.io/) or Docker Desktop
* [uv](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
* **Java 17** (Strictly required for Apache Spark execution)
  * *macOS Installation:*
    ```bash
    brew install openjdk@17
    sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
    ```

### 2. Initialize the Environment
Clone the repository and let `uv` sync the dependencies automatically:
```bash
git clone [https://github.com/YOUR_USERNAME/code-metrics-platform.git](https://github.com/YOUR_USERNAME/code-metrics-platform.git)
cd code-metrics-platform
uv sync
```
*(Ensure PySpark is specifically pinned to version `3.5.0` to maintain compatibility with the Kafka connectors).*

### 3. Start the Infrastructure
Spin up Zookeeper, Kafka, Cassandra, and MongoDB in the background:
```bash
podman compose up -d
```
*(Wait ~30 seconds for Cassandra and Kafka to fully initialize).*

### 4. Run the Pipeline (Multi-Terminal Setup)
To see the end-to-end data flow, open three separate terminal windows in the project root:

**Terminal 1: Start the Log Simulator**
```bash
uv run python simulator/generate_logs.py
```

**Terminal 2: Start the Spark Streaming Job**
```bash
uv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 processing/stream_leaderboard.py
```

**Terminal 3: Launch the BI Dashboard**
```bash
uv run streamlit run dashboard/app.py
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
Then, always run your job with the packages flag: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ...`

### 3. Dashboard Error: `background_gradient requires matplotlib`
**Cause:** Streamlit's Pandas styling engine requires Matplotlib under the hood to calculate the color gradients for the Difficulty Heatmap.
**Solution:** Add the missing dependency to your environment:
```bash
uv add matplotlib
```
