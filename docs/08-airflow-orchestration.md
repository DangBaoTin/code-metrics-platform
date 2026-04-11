# 08 - Airflow Orchestration

Source: `airflow/dags/code_metrics_dag.py`

## DAG

- DAG ID: `nightly_code_metrics_etl`
- Schedule: daily
- Task: `trigger_spark_cassandra_etl`
- Operator: PythonOperator

## Execution Model

The DAG calls `run_batch_job()` from the batch module at runtime. This keeps DAG parsing lightweight and centralizes ETL logic in one code path.

## Infrastructure Dependencies

Airflow containers in compose depend on:

- Postgres healthy
- Kafka started
- Cassandra healthy
- Mongo started and replica set initialized

## Runtime Configuration

Airflow services are provisioned with:

- Cassandra host and consistency environment variables
- Mongo replica-set URI
- Java and PySpark runtime dependencies

## Recovery Pattern

Scheduler service runs in a restart loop to recover from transient crashes.

## Validation

- UI endpoint: http://localhost:8080
- Default credentials: admin / admin
- Unpause `nightly_code_metrics_etl` and trigger manually for test runs
