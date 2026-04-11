# 10 - Troubleshooting Knowledge Base

This section summarizes known failure modes and practical fixes.

## 1. Dashboard says Waiting for telemetry data from Kafka

Likely causes:

- Simulator is not running or crashed
- Stream job is not running
- Stream cannot write to Cassandra

Checks:

1. Ensure simulator terminal is continuously printing Sent logs.
2. Ensure stream process is alive and not terminated.
3. Verify Cassandra ring is healthy.

## 2. Simulator fails with NoBrokersAvailable

Cause:

Kafka not yet ready at startup.

Resolution:

Simulator now includes producer connection retries and send-time reconnect logic.

## 3. Stream reset fails with Directory not empty during checkpoint cleanup

Cause:

Transient filesystem race on macOS while deleting checkpoint subfolders.

Resolution:

Streaming reset now retries directory deletion for transient ENOTEMPTY/EBUSY conditions.

## 4. Stream fails with failOnDataLoss offset mismatch

Cause:

Kafka offsets changed due to retention or topic recreation while old checkpoint metadata exists.

Resolution:

Use `STREAM_FAIL_ON_DATA_LOSS=false` and run with `--reset-state`.

## 5. Cassandra write timeouts in early batches

Cause:

Ring warm-up and connection churn.

Resolution:

- Wait for ring health before starting stream
- Use conservative `STREAM_MAX_OFFSETS_PER_TRIGGER`
- Keep `LOCAL_ONE` consistency for local development
- Reused Cassandra session in streaming writes reduces repeated reconnect pressure

## 6. No System Alerts even when stream is running

Cause:

If generated execution_time_ms values never exceed 5000, HIGH_LATENCY cannot trigger.

Resolution:

Simulator now injects latency spikes above threshold and hot-user bursts for alert visibility.

## 7. Airflow cannot connect to Cassandra at localhost

Cause:

Inside containers, Cassandra host should be service DNS, not localhost.

Resolution:

Set `CASSANDRA_HOST=cassandra` for airflow services in compose.

## 8. Python 3.12 distutils import issue with PySpark

Cause:

PySpark internal import expects distutils.

Resolution:

Project includes compatibility shim in `src/distutils`; run with `PYTHONPATH=src`.
