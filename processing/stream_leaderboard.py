from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Initialize Spark Session with Kafka packages
spark = SparkSession.builder \
    .appName("CodeMetrics_LiveLeaderboard") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the Schema that matches our Simulator
schema = StructType([
    StructField("submission_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("problem_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("execution_time_ms", IntegerType(), True),
    StructField("memory_kb", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Read Stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_submissions") \
    .load()

# 4. Parse the JSON payload
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Output to Console (For testing before writing to Cassandra)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()