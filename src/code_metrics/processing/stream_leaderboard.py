import argparse
import errno
import os
import shutil
import time
import uuid
from datetime import datetime

from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


_CASS_CLUSTER = None
_CASS_SESSION = None


def remove_dir_with_retry(path: str):
    max_attempts = int(os.getenv("CHECKPOINT_DELETE_MAX_ATTEMPTS", "8"))
    delay_seconds = float(os.getenv("CHECKPOINT_DELETE_RETRY_DELAY_SEC", "0.5"))
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            last_error = exc
            # macOS can surface transient ENOTEMPTY while checkpoint files are being finalized.
            if exc.errno in {errno.ENOTEMPTY, errno.EBUSY} and attempt < max_attempts:
                time.sleep(delay_seconds)
                continue
            raise

    raise RuntimeError(f"Failed to delete checkpoint path {path}: {last_error}")


def parse_args():
    parser = argparse.ArgumentParser(description="Stream leaderboard pipeline")
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Clear stream checkpoints and truncate leaderboard table before starting.",
    )
    return parser.parse_args()


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def create_host_safe_cluster(cassandra_host: str, cassandra_port: str):
    allowed_hosts = [
        host.strip()
        for host in os.getenv("CASSANDRA_ALLOWED_HOSTS", cassandra_host).split(",")
        if host.strip()
    ]
    contact_points = [
        host.strip()
        for host in os.getenv("CASSANDRA_CONTACT_POINTS", cassandra_host).split(",")
        if host.strip()
    ]
    request_timeout = float(os.getenv("CASSANDRA_REQUEST_TIMEOUT_SEC", "20"))
    connect_timeout = float(os.getenv("CASSANDRA_CONNECT_TIMEOUT_SEC", "15"))
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(allowed_hosts),
        request_timeout=request_timeout,
    )
    return Cluster(
        contact_points,
        port=int(cassandra_port),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=connect_timeout,
        control_connection_timeout=connect_timeout,
    )


def close_cached_cassandra_session():
    global _CASS_CLUSTER, _CASS_SESSION
    if _CASS_CLUSTER is not None:
        try:
            _CASS_CLUSTER.shutdown()
        except Exception:
            pass
    _CASS_CLUSTER = None
    _CASS_SESSION = None


def get_cassandra_session(cassandra_host: str, cassandra_port: str, force_reconnect: bool = False):
    global _CASS_CLUSTER, _CASS_SESSION
    if force_reconnect:
        close_cached_cassandra_session()

    if _CASS_SESSION is not None:
        return _CASS_SESSION

    _CASS_CLUSTER = create_host_safe_cluster(cassandra_host, cassandra_port)
    _CASS_SESSION = _CASS_CLUSTER.connect()
    _CASS_SESSION.execute("SELECT release_version FROM system.local")
    return _CASS_SESSION


def reset_stream_state(cassandra_host: str, cassandra_port: str, checkpoint_base: str):
    bronze_cp = os.path.join(checkpoint_base, "bronze")
    gold_cp = os.path.join(checkpoint_base, "gold")
    system_cp = os.path.join(checkpoint_base, "system")

    for path in (bronze_cp, gold_cp, system_cp):
        if os.path.isdir(path):
            remove_dir_with_retry(path)

    max_attempts = int(os.getenv("RESET_MAX_ATTEMPTS", "8"))
    delay_seconds = float(os.getenv("RESET_RETRY_DELAY_SEC", "2"))
    last_error = None

    for attempt in range(1, max_attempts + 1):
        cluster = None
        try:
            cluster = create_host_safe_cluster(cassandra_host, cassandra_port)
            session = cluster.connect()
            session.execute("TRUNCATE code_metrics.gold_live_leaderboard")
            session.execute("TRUNCATE code_metrics.gold_system_alerts")
            return
        except Exception as exc:
            last_error = exc
            print(f"WARN reset attempt {attempt}/{max_attempts} failed: {exc}")
            if attempt < max_attempts:
                time.sleep(delay_seconds)
        finally:
            if cluster is not None:
                cluster.shutdown()

    raise RuntimeError(
        "Reset failed: Cassandra is not ready or keyspace is missing. "
        "Run `podman exec -i code-metrics-platform-cassandra-1 cqlsh < storage/init_cassandra.cql` "
        f"then retry. Last error: {last_error}"
    )


def wait_for_cassandra_ready(cassandra_host: str, cassandra_port: str):
    max_attempts = int(os.getenv("CASSANDRA_READY_MAX_ATTEMPTS", "20"))
    delay_seconds = float(os.getenv("CASSANDRA_READY_DELAY_SEC", "2"))
    last_error = None

    for attempt in range(1, max_attempts + 1):
        cluster = None
        try:
            cluster = create_host_safe_cluster(cassandra_host, cassandra_port)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            return
        except Exception as exc:
            last_error = exc
            print(f"WARN Cassandra not ready yet (attempt {attempt}/{max_attempts}): {exc}")
            if attempt < max_attempts:
                time.sleep(delay_seconds)
        finally:
            if cluster is not None:
                cluster.shutdown()

    raise RuntimeError(
        f"Cassandra not ready after {max_attempts} attempts: {last_error}"
    )


def write_rows_with_retry(
    rows,
    cassandra_host: str,
    cassandra_port: str,
    cql: str,
    params_fn,
    label: str,
):
    if not rows:
        return

    batch_max_attempts = int(os.getenv("CASSANDRA_BATCH_MAX_ATTEMPTS", "5"))
    batch_retry_delay = float(os.getenv("CASSANDRA_BATCH_RETRY_DELAY_SEC", "2"))
    row_max_attempts = int(os.getenv("CASSANDRA_ROW_MAX_ATTEMPTS", "3"))
    row_retry_delay = float(os.getenv("CASSANDRA_ROW_RETRY_DELAY_SEC", "0.4"))
    last_error = None

    for batch_attempt in range(1, batch_max_attempts + 1):
        try:
            session = get_cassandra_session(
                cassandra_host,
                cassandra_port,
                force_reconnect=(batch_attempt > 1),
            )
            prepared = session.prepare(cql)

            for idx, row in enumerate(rows):
                row_error = None
                for row_attempt in range(1, row_max_attempts + 1):
                    try:
                        session.execute(prepared, params_fn(row))
                        row_error = None
                        break
                    except Exception as exc:
                        row_error = exc
                        if row_attempt < row_max_attempts:
                            time.sleep(row_retry_delay)

                if row_error is not None:
                    raise RuntimeError(
                        f"{label} row write failed at index {idx} after {row_max_attempts} attempts: {row_error}"
                    )

            return
        except Exception as exc:
            last_error = exc
            close_cached_cassandra_session()
            if batch_attempt < batch_max_attempts:
                print(
                    f"WARN {label} write attempt {batch_attempt}/{batch_max_attempts} failed: {exc}; retrying..."
                )
                time.sleep(batch_retry_delay)

    raise RuntimeError(
        f"{label} write failed after {batch_max_attempts} attempts: {last_error}"
    )


def write_system_alerts(batch_df, batch_id, cassandra_host: str, cassandra_port: str):
    try:
        rows = batch_df.select("alert_type", "user_id", "description", "timestamp").collect()
        if not rows:
            return

        def _params(row):
            raw_ts = row.timestamp
            if raw_ts is None:
                ts = datetime.utcnow()
            elif isinstance(raw_ts, datetime):
                ts = raw_ts
            else:
                ts = datetime.utcfromtimestamp(float(raw_ts))
            return (
                uuid.uuid4(),
                row.alert_type,
                row.user_id,
                row.description,
                ts,
            )

        write_rows_with_retry(
            rows,
            cassandra_host,
            cassandra_port,
            "INSERT INTO code_metrics.gold_system_alerts (alert_id, alert_type, user_id, description, triggered_at) VALUES (?, ?, ?, ?, ?)",
            _params,
            "system-alerts",
        )
    except Exception as exc:
        # Keep stream alive even if a side alert write fails for a micro-batch.
        print(f"WARN system-alerts batch failed (batch_id={batch_id}): {exc}")


def write_bronze_submissions(batch_df, batch_id, cassandra_host: str, cassandra_port: str):
    try:
        rows = batch_df.select("submission_id", "ingest_timestamp", "raw_payload").collect()
        if not rows:
            return

        write_rows_with_retry(
            rows,
            cassandra_host,
            cassandra_port,
            "INSERT INTO code_metrics.bronze_raw_submissions (submission_id, ingest_timestamp, raw_payload) VALUES (?, ?, ?)",
            lambda row: (row.submission_id, row.ingest_timestamp, row.raw_payload),
            "bronze-submissions",
        )
    except Exception as exc:
        print(f"WARN bronze submissions batch failed (batch_id={batch_id}): {exc}")


def write_gold_leaderboard(batch_df, batch_id, cassandra_host: str, cassandra_port: str):
    try:
        rows = batch_df.select(
            "rank_tier", "user_id", "total_solved", "recent_status", "last_updated"
        ).collect()
        if not rows:
            return

        write_rows_with_retry(
            rows,
            cassandra_host,
            cassandra_port,
            "INSERT INTO code_metrics.gold_live_leaderboard (rank_tier, user_id, total_solved, recent_status, last_updated) VALUES (?, ?, ?, ?, ?)",
            lambda row: (
                row.rank_tier,
                row.user_id,
                int(row.total_solved) if row.total_solved is not None else 0,
                row.recent_status,
                row.last_updated,
            ),
            "gold-leaderboard",
        )
    except Exception as exc:
        print(f"WARN gold leaderboard batch failed (batch_id={batch_id}): {exc}")


def write_bronze_metrics(batch_df, batch_id, cassandra_host: str, cassandra_port: str):
    try:
        rows = batch_df.select("metric_id", "ingest_timestamp", "raw_payload").collect()
        if not rows:
            return

        write_rows_with_retry(
            rows,
            cassandra_host,
            cassandra_port,
            "INSERT INTO code_metrics.bronze_raw_system_metrics (metric_id, ingest_timestamp, raw_payload) VALUES (?, ?, ?)",
            lambda row: (row.metric_id, row.ingest_timestamp, row.raw_payload),
            "bronze-metrics",
        )
    except Exception as exc:
        print(f"WARN bronze metrics batch failed (batch_id={batch_id}): {exc}")


def main():
    args = parse_args()

    cassandra_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cassandra_port = os.getenv("CASSANDRA_PORT", "9042")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    checkpoint_base = os.getenv(
        "STREAM_CHECKPOINT_BASE", "./storage/checkpoints/stream_leaderboard"
    )
    starting_offsets = os.getenv("STREAM_STARTING_OFFSETS", "latest")
    fail_on_data_loss = os.getenv("STREAM_FAIL_ON_DATA_LOSS", "false")
    max_offsets_per_trigger = os.getenv("STREAM_MAX_OFFSETS_PER_TRIGGER", "1200")
    cass_read_consistency = os.getenv("CASSANDRA_READ_CONSISTENCY", "LOCAL_ONE")
    cass_write_consistency = os.getenv("CASSANDRA_WRITE_CONSISTENCY", "LOCAL_ONE")

    wait_for_cassandra_ready(cassandra_host, cassandra_port)

    if args.reset_state or env_bool("STREAM_RESET", False):
        reset_stream_state(cassandra_host, cassandra_port, checkpoint_base)

    spark = (
        SparkSession.builder.appName("CodeMetrics_LiveLeaderboard")
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.cassandra.connection.port", cassandra_port)
        .config("spark.cassandra.input.consistency.level", cass_read_consistency)
        .config("spark.cassandra.output.consistency.level", cass_write_consistency)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    submission_schema = StructType(
        [
            StructField("submission_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("problem_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("execution_time_ms", IntegerType(), True),
            StructField("memory_kb", IntegerType(), True),
            StructField("client_ip", StringType(), True),
            StructField("timestamp", DoubleType(), True),
        ]
    )

    metric_schema = StructType(
        [
            StructField("metric_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("metric_type", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("timestamp", DoubleType(), True),
        ]
    )

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "raw_submissions")
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )

    df_parsed = (
        df_kafka.selectExpr("CAST(value AS STRING) as raw_payload")
        .withColumn("data", F.from_json(F.col("raw_payload"), submission_schema))
        .select("raw_payload", "data.*")
    )

    # Bronze writes for raw submissions
    df_bronze = df_parsed.select(
        F.col("submission_id"), F.current_timestamp().alias("ingest_timestamp"), F.col("raw_payload")
    )

    def write_to_bronze(batch_df, batch_id):
        write_bronze_submissions(batch_df, batch_id, cassandra_host, cassandra_port)

    query_bronze = (
        df_bronze.writeStream.option(
            "checkpointLocation", os.path.join(checkpoint_base, "bronze")
        )
        .queryName("stream_bronze_raw_submissions")
        .foreachBatch(write_to_bronze)
        .outputMode("append")
        .start()
    )

    # Gold live leaderboard
    df_leaderboard = (
        df_parsed.filter(F.col("status") == "Pass")
        .groupBy("user_id")
        .agg(F.count("problem_id").alias("total_solved"))
        .withColumn("rank_tier", F.lit("Global"))
        .withColumn("recent_status", F.lit("Pass"))
        .withColumn("last_updated", F.current_timestamp())
    )

    def write_to_gold(batch_df, batch_id):
        write_gold_leaderboard(batch_df, batch_id, cassandra_host, cassandra_port)

    query_gold = (
        df_leaderboard.writeStream.option(
            "checkpointLocation", os.path.join(checkpoint_base, "gold")
        )
        .queryName("stream_gold_live_leaderboard")
        .foreachBatch(write_to_gold)
        .outputMode("update")
        .start()
    )

    # System metrics stream for latency monitoring
    df_metrics_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "system_metrics")
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", fail_on_data_loss)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )

    df_metrics = (
        df_metrics_kafka.selectExpr("CAST(value AS STRING) as raw_payload")
        .withColumn("data", F.from_json(F.col("raw_payload"), metric_schema))
        .select("raw_payload", "data.*")
    )

    df_metrics_bronze = df_metrics.select(
        F.col("metric_id"), F.current_timestamp().alias("ingest_timestamp"), F.col("raw_payload")
    )

    def write_metrics_bronze(batch_df, batch_id):
        write_bronze_metrics(batch_df, batch_id, cassandra_host, cassandra_port)

    query_metrics_bronze = (
        df_metrics_bronze.writeStream.option(
            "checkpointLocation", os.path.join(checkpoint_base, "system", "bronze_metrics")
        )
        .queryName("stream_bronze_raw_system_metrics")
        .foreachBatch(write_metrics_bronze)
        .outputMode("append")
        .start()
    )

    # Latency alert rule: execution exceeds 5 seconds.
    df_latency_alerts = (
        df_metrics.filter(
            (F.col("metric_type") == "execution_time_ms") & (F.col("value") > 5000)
        )
        .withColumn("alert_type", F.lit("HIGH_LATENCY"))
        .withColumn(
            "description",
            F.concat(
                F.lit("Execution latency exceeded 5s: "),
                F.col("value").cast("string"),
                F.lit(" ms"),
            ),
        )
        .select("alert_type", "user_id", "description", "timestamp")
    )

    # Cheat detection with 30-second time-window and multi-IP anomaly.
    df_events = (
        df_parsed.withColumn(
            "event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp")))
        )
        .filter(F.col("event_time").isNotNull() & F.col("user_id").isNotNull())
        .withColumn("client_ip", F.coalesce(F.col("client_ip"), F.lit("unknown")))
    )

    df_cheat_window = (
        df_events.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "30 seconds", "10 seconds"), F.col("user_id"))
        .agg(
            F.count("submission_id").alias("submission_count_30s"),
            F.approx_count_distinct("client_ip").alias("distinct_ip_count"),
            F.max("event_time").alias("window_last_event"),
        )
    )

    df_cheat_alerts = (
        df_cheat_window.filter(
            (F.col("submission_count_30s") >= 2) | (F.col("distinct_ip_count") > 1)
        )
        .withColumn("alert_type", F.lit("CHEAT_DETECTED"))
        .withColumn(
            "description",
            F.concat(
                F.lit("Window anomaly: submissions_30s="),
                F.col("submission_count_30s").cast("string"),
                F.lit(", distinct_ips="),
                F.col("distinct_ip_count").cast("string"),
            ),
        )
        .withColumn("timestamp", F.unix_timestamp(F.col("window_last_event")).cast("double"))
        .select("alert_type", "user_id", "description", "timestamp")
    )

    df_all_alerts = df_latency_alerts.unionByName(df_cheat_alerts)

    query_system_alerts = (
        df_all_alerts.writeStream.option(
            "checkpointLocation", os.path.join(checkpoint_base, "system", "alerts_v2")
        )
        .queryName("stream_gold_system_alerts_v2")
        .foreachBatch(
            lambda batch_df, batch_id: write_system_alerts(
                batch_df, batch_id, cassandra_host, cassandra_port
            )
        )
        .outputMode("append")
        .start()
    )

    _ = (query_bronze, query_gold, query_metrics_bronze, query_system_alerts)
    try:
        spark.streams.awaitAnyTermination()
    finally:
        close_cached_cassandra_session()


if __name__ == "__main__":
    main()
