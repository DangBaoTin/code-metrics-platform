# 05 - Streaming Pipeline Walkthrough

Source: `src/code_metrics/processing/stream_leaderboard.py`

## Responsibilities

- Consume `raw_submissions` and `system_metrics` from Kafka
- Persist raw Bronze records to Cassandra
- Build and refresh live leaderboard in Gold
- Generate system alerts (latency and suspicious behavior)

## Startup and Safety Controls

- Cassandra readiness probe before stream starts
- Optional reset mode:
  - Deletes stream checkpoints with retry-safe filesystem cleanup
  - Truncates `gold_live_leaderboard` and `gold_system_alerts`
- Configurable Kafka behavior:
  - `STREAM_STARTING_OFFSETS`
  - `STREAM_FAIL_ON_DATA_LOSS`
  - `STREAM_MAX_OFFSETS_PER_TRIGGER`

## Cassandra Write Strategy

- Host-safe cluster configuration with allowed host filtering
- Reused cached session to reduce connection churn
- Batch-level and row-level retries for transient failures
- Failures in side outputs are logged without crashing the full stream

## Real-Time Outputs

### Bronze Outputs

- Raw submissions to `bronze_raw_submissions`
- Raw metrics to `bronze_raw_system_metrics`

### Gold Live Leaderboard

- Filters pass events
- Aggregates solved count by user
- Writes latest rank snapshot to `gold_live_leaderboard`

### Gold System Alerts

Two rules:

1. HIGH_LATENCY
- Trigger: `metric_type == execution_time_ms` and value > 5000

2. CHEAT_DETECTED
- Event-time windows of 30 seconds, sliding every 10 seconds
- Trigger if submissions in window are high or multiple client IPs appear for one user

## Stream Queries

The job runs multiple named streaming queries concurrently and waits on termination.

## Key Environment Variables

- `CASSANDRA_HOST`, `CASSANDRA_PORT`, `CASSANDRA_CONTACT_POINTS`
- `CASSANDRA_ALLOWED_HOSTS`
- `CASSANDRA_CONNECT_TIMEOUT_SEC`, `CASSANDRA_REQUEST_TIMEOUT_SEC`
- `CASSANDRA_WRITE_CONSISTENCY`, `CASSANDRA_READ_CONSISTENCY`
- `STREAM_CHECKPOINT_BASE`
- `STREAM_RESET`
