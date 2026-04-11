import os
import time

import pymongo
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def create_host_safe_cluster(cassandra_host: str, cassandra_port: int):
    allowed_hosts = [
        host.strip()
        for host in os.getenv("CASSANDRA_ALLOWED_HOSTS", cassandra_host).split(",")
        if host.strip()
    ]

    if cassandra_host in {"localhost", "127.0.0.1"}:
        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy([cassandra_host])
        )
        return Cluster(
            [cassandra_host],
            port=cassandra_port,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )

    contact_points = allowed_hosts if allowed_hosts else [cassandra_host]
    return Cluster(contact_points, port=cassandra_port)


def truncate_gold_tables_if_enabled():
    if not env_bool("BATCH_RESET_GOLD", True):
        return

    cassandra_host = os.getenv("CASSANDRA_HOST", "localhost")
    cassandra_port = int(os.getenv("CASSANDRA_PORT", "9042"))
    cluster = create_host_safe_cluster(cassandra_host, cassandra_port)

    tables = [
        "gold_engagement_scores",
        "gold_difficulty_heatmap",
        "gold_subscription_revenue",
        "gold_instructor_report_card",
        "gold_dropout_predictions",
        "gold_next_problem_recommendations",
        "gold_adaptive_difficulty_profiles",
    ]

    try:
        last_error = None
        for _ in range(6):
            try:
                session = cluster.connect()
                for table in tables:
                    session.execute(f"TRUNCATE code_metrics.{table}")
                return
            except Exception as exc:
                last_error = exc
                time.sleep(3)
        raise RuntimeError(f"Failed to truncate gold tables after retries: {last_error}")
    finally:
        cluster.shutdown()


def create_mongo_client() -> pymongo.MongoClient:
    mongo_uri = os.getenv(
        "MONGODB_URI",
        "mongodb://localhost:27017,localhost:27018,localhost:27019/code_metrics?replicaSet=rs0",
    )
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client
    except Exception:
        fallback_uri = "mongodb://localhost:27017/?directConnection=true"
        client = pymongo.MongoClient(fallback_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client


def fetch_mongo_collection(spark: SparkSession, collection: str, projection: dict):
    client = create_mongo_client()
    try:
        rows = list(client["code_metrics"][collection].find({}, projection))
    finally:
        client.close()

    if not rows:
        return None
    return spark.createDataFrame(rows)


def build_dropout_predictions(df_user_features):
    training = df_user_features.withColumn(
        "label",
        F.when(
            (F.col("fail_rate_pct") >= 65)
            & (F.col("login_minutes") < 35)
            & (F.col("solved_count") < 12),
            F.lit(1.0),
        ).otherwise(F.lit(0.0)),
    )

    label_stats = training.groupBy("label").count().collect()
    if len(label_stats) < 2:
        return training.select(
            "user_id",
            F.when(F.col("label") == 1.0, F.lit(0.8)).otherwise(F.lit(0.2)).alias(
                "dropout_probability"
            ),
        )

    assembler = VectorAssembler(
        inputCols=[
            "total_submissions",
            "solved_count",
            "failed_count",
            "login_minutes",
            "avg_exec_ms",
            "fail_rate_pct",
        ],
        outputCol="features",
    )
    model_input = assembler.transform(training)

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=30,
        regParam=0.05,
        elasticNetParam=0.0,
    )
    model = lr.fit(model_input)

    scored = model.transform(model_input).withColumn(
        "probability_arr", vector_to_array("probability")
    )
    return scored.select(
        "user_id",
        F.col("probability_arr").getItem(1).alias("dropout_probability"),
    )


def build_recommendations(df_silver):
    interactions = (
        df_silver.filter(F.col("status") == "Pass")
        .groupBy("user_id", "problem_id")
        .agg(F.count("submission_id").cast("double").alias("rating"))
    )

    if interactions.rdd.isEmpty():
        return None

    user_indexer = StringIndexer(
        inputCol="user_id", outputCol="user_idx", handleInvalid="skip"
    )
    item_indexer = StringIndexer(
        inputCol="problem_id", outputCol="problem_idx", handleInvalid="skip"
    )

    indexed = user_indexer.fit(interactions).transform(interactions)
    indexed = item_indexer.fit(indexed).transform(indexed)

    als = ALS(
        userCol="user_idx",
        itemCol="problem_idx",
        ratingCol="rating",
        implicitPrefs=True,
        rank=8,
        maxIter=8,
        regParam=0.08,
        alpha=1.0,
        coldStartStrategy="drop",
        nonnegative=True,
    )
    model = als.fit(indexed)

    recs = model.recommendForAllUsers(1)
    exploded = recs.select(
        "user_idx", F.explode("recommendations").alias("rec")
    ).select(
        "user_idx",
        F.col("rec.problem_idx").alias("problem_idx"),
        F.col("rec.rating").alias("recommendation_score"),
    )

    user_map = indexed.select("user_id", "user_idx").dropDuplicates(["user_idx"])
    item_map = indexed.select("problem_id", "problem_idx").dropDuplicates(["problem_idx"])

    return (
        exploded.join(user_map, on="user_idx", how="left")
        .join(item_map, on="problem_idx", how="left")
        .select(
            "user_id",
            F.col("problem_id").alias("recommended_problem_id"),
            F.round("recommendation_score", 4).alias("recommendation_score"),
        )
    )


def run_batch_job():
    cass_read_consistency = os.getenv("CASSANDRA_READ_CONSISTENCY", "LOCAL_ONE")
    cass_write_consistency = os.getenv("CASSANDRA_WRITE_CONSISTENCY", "LOCAL_ONE")

    spark = (
        SparkSession.builder.appName("CodeMetricsBatchETL")
        .config(
            "spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "localhost")
        )
        .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
        .config("spark.cassandra.input.consistency.level", cass_read_consistency)
        .config("spark.cassandra.output.consistency.level", cass_write_consistency)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        )
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Starting Nightly Batch ETL Job...")

    truncate_gold_tables_if_enabled()

    # Bronze submissions extract
    df_bronze_sub = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="bronze_raw_submissions", keyspace="code_metrics")
        .load()
    )

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

    df_silver = (
        df_bronze_sub.withColumn("data", F.from_json(F.col("raw_payload"), submission_schema))
        .select("data.*")
        .withColumn("timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
        .withColumn("day", F.to_date(F.col("timestamp")))
    )

    # Silver persistence
    (
        df_silver.select(
            "problem_id",
            "user_id",
            "day",
            "timestamp",
            "submission_id",
            "status",
            "execution_time_ms",
            "memory_kb",
        )
        .write.format("org.apache.spark.sql.cassandra")
        .options(table="silver_submissions", keyspace="code_metrics")
        .mode("append")
        .save()
    )

    # Bronze system metrics for login-time proxy
    df_bronze_metrics = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="bronze_raw_system_metrics", keyspace="code_metrics")
        .load()
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

    df_metrics = (
        df_bronze_metrics.withColumn(
            "data", F.from_json(F.col("raw_payload"), metric_schema)
        )
        .select("data.*")
        .filter(F.col("user_id").isNotNull())
    )

    df_login_proxy = (
        df_metrics.filter(F.col("metric_type") == "submission_interval_sec")
        .groupBy("user_id")
        .agg((F.sum("value") / F.lit(60.0)).alias("login_minutes"))
    )

    # Feature aggregates
    df_user_features = (
        df_silver.groupBy("user_id")
        .agg(
            F.count("submission_id").alias("total_submissions"),
            F.sum(F.when(F.col("status") == "Pass", F.lit(1)).otherwise(F.lit(0))).alias(
                "solved_count"
            ),
            F.sum(
                F.when(F.col("status") != "Pass", F.lit(1)).otherwise(F.lit(0))
            ).alias("failed_count"),
            F.avg("execution_time_ms").alias("avg_exec_ms"),
        )
        .join(df_login_proxy, on="user_id", how="left")
        .fillna({"login_minutes": 0.0, "avg_exec_ms": 0.0})
        .withColumn(
            "fail_rate_pct",
            F.when(
                F.col("total_submissions") > 0,
                F.round((F.col("failed_count") / F.col("total_submissions")) * 100.0, 2),
            ).otherwise(F.lit(0.0)),
        )
    )

    # Gold A: Engagement score (Login time + solved)
    df_engagement = (
        df_user_features.withColumn(
            "engagement_score",
            F.round(F.col("login_minutes") * 4.0 + F.col("solved_count") * 18.0).cast("int"),
        )
        .withColumn("last_updated", F.current_timestamp())
        .select("user_id", "engagement_score", "last_updated")
    )

    (
        df_engagement.write.format("org.apache.spark.sql.cassandra")
        .options(table="gold_engagement_scores", keyspace="code_metrics")
        .mode("append")
        .save()
    )

    # Gold B: Difficulty heatmap
    df_problems_meta = fetch_mongo_collection(
        spark,
        "problems",
        {
            "_id": 1,
            "category": 1,
            "title": 1,
            "difficulty": 1,
            "instructor_id": 1,
            "instructor_name": 1,
        },
    )

    if df_problems_meta is not None:
        df_problems_meta = df_problems_meta.withColumnRenamed("_id", "problem_id")

        df_heatmap = (
            df_silver.groupBy("problem_id")
            .agg(
                F.count("submission_id").alias("total_attempts"),
                F.sum(
                    F.when(F.col("status") != "Pass", F.lit(1)).otherwise(F.lit(0))
                ).alias("failed_attempts"),
            )
            .withColumn(
                "fail_rate_pct",
                F.round((F.col("failed_attempts") / F.col("total_attempts")) * 100.0, 2),
            )
            .join(
                df_problems_meta.select("problem_id", "category"),
                on="problem_id",
                how="left",
            )
            .withColumn("last_updated", F.current_timestamp())
            .select(
                "problem_id",
                "total_attempts",
                "failed_attempts",
                "fail_rate_pct",
                "category",
                "last_updated",
            )
        )

        (
            df_heatmap.write.format("org.apache.spark.sql.cassandra")
            .options(table="gold_difficulty_heatmap", keyspace="code_metrics")
            .mode("append")
            .save()
        )

    # Gold C: Subscription revenue from Mongo transactions
    df_transactions = fetch_mongo_collection(
        spark,
        "transactions",
        {"_id": 0, "plan_type": 1, "amount_usd": 1, "date": 1},
    )

    if df_transactions is not None:
        df_subscription = (
            df_transactions.groupBy("plan_type")
            .agg(
                F.count("plan_type").alias("total_subscriptions"),
                F.round(F.sum("amount_usd"), 2).alias("total_revenue_usd"),
            )
            .withColumn("last_updated", F.current_timestamp())
        )

        (
            df_subscription.write.format("org.apache.spark.sql.cassandra")
            .options(table="gold_subscription_revenue", keyspace="code_metrics")
            .mode("append")
            .save()
        )

    # Gold D: Instructor report card from ratings + problem metadata
    df_ratings = fetch_mongo_collection(
        spark,
        "ratings",
        {"_id": 0, "problem_id": 1, "rating_score": 1},
    )

    if df_ratings is not None and df_problems_meta is not None:
        df_instructor = (
            df_ratings.join(
                df_problems_meta.select("problem_id", "instructor_id", "instructor_name"),
                on="problem_id",
                how="left",
            )
            .filter(F.col("instructor_id").isNotNull())
            .groupBy("instructor_id", "instructor_name")
            .agg(
                F.round(F.avg("rating_score"), 2).alias("avg_rating"),
                F.count("rating_score").alias("total_ratings"),
            )
            .withColumn("last_updated", F.current_timestamp())
        )

        (
            df_instructor.write.format("org.apache.spark.sql.cassandra")
            .options(table="gold_instructor_report_card", keyspace="code_metrics")
            .mode("append")
            .save()
        )

    # Gold E: Dropout prediction (Logistic Regression)
    df_dropout = build_dropout_predictions(df_user_features).withColumn(
        "risk_label",
        F.when(F.col("dropout_probability") >= 0.7, F.lit("HIGH"))
        .when(F.col("dropout_probability") >= 0.45, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )
    df_dropout = df_dropout.withColumn("last_updated", F.current_timestamp())

    (
        df_dropout.select("user_id", "dropout_probability", "risk_label", "last_updated")
        .write.format("org.apache.spark.sql.cassandra")
        .options(table="gold_dropout_predictions", keyspace="code_metrics")
        .mode("append")
        .save()
    )

    # Gold F: Next-problem recommender (Collaborative Filtering via ALS)
    df_recs = build_recommendations(df_silver)
    if df_recs is not None:
        df_recs = df_recs.withColumn("last_updated", F.current_timestamp())
        (
            df_recs.write.format("org.apache.spark.sql.cassandra")
            .options(table="gold_next_problem_recommendations", keyspace="code_metrics")
            .mode("append")
            .save()
        )

    # Gold G: Adaptive difficulty profiles
    df_adaptive = (
        df_user_features.withColumn(
            "performance_tier",
            F.when((F.col("solved_count") < 12) | (F.col("fail_rate_pct") >= 65), F.lit("NOVICE"))
            .when(
                (F.col("solved_count") < 35) | (F.col("fail_rate_pct") >= 35),
                F.lit("INTERMEDIATE"),
            )
            .otherwise(F.lit("EXPERT")),
        )
        .withColumn(
            "recommended_difficulty",
            F.when(F.col("performance_tier") == "NOVICE", F.lit("Easy"))
            .when(F.col("performance_tier") == "INTERMEDIATE", F.lit("Medium"))
            .otherwise(F.lit("Hard")),
        )
        .withColumn("last_updated", F.current_timestamp())
        .select("user_id", "performance_tier", "recommended_difficulty", "last_updated")
    )

    (
        df_adaptive.write.format("org.apache.spark.sql.cassandra")
        .options(table="gold_adaptive_difficulty_profiles", keyspace="code_metrics")
        .mode("append")
        .save()
    )

    print("Batch ETL complete: Gold BI + AI tables updated.")
    spark.stop()


if __name__ == "__main__":
    run_batch_job()
