import json
import os
import time
import random
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import pymongo

fake = Faker()
producer = None

TOPIC_NAME = 'raw_submissions'
SYSTEM_METRICS_TOPIC = 'system_metrics'
STATUSES = ['Pass', 'Time Limit Exceeded', 'Runtime Error', 'Wrong Answer']
ENTITY_REFRESH_SECONDS = int(os.getenv("ENTITY_REFRESH_SECONDS", "60"))

FALLBACK_USERS = ["user_001", "user_002", "user_003", "user_004", "user_005"]
FALLBACK_PROBLEMS = ["prob_1", "prob_2", "prob_3", "prob_4", "prob_5"]
MIN_SIM_USERS = int(os.getenv("MIN_SIM_USERS", "5"))
MIN_SIM_PROBLEMS = int(os.getenv("MIN_SIM_PROBLEMS", "5"))


def create_kafka_producer_with_retry():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    max_attempts = int(os.getenv("KAFKA_CONNECT_MAX_ATTEMPTS", "30"))
    delay_seconds = float(os.getenv("KAFKA_CONNECT_RETRY_DELAY_SEC", "2"))
    request_timeout_ms = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "20000"))

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",") if s.strip()],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=request_timeout_ms,
            )
            print(f"Kafka producer connected to {bootstrap_servers}")
            return p
        except Exception as exc:
            last_error = exc
            print(f"WARN Kafka not ready (attempt {attempt}/{max_attempts}): {exc}")
            if attempt < max_attempts:
                time.sleep(delay_seconds)

    raise RuntimeError(
        f"Kafka producer failed to connect after {max_attempts} attempts: {last_error}"
    )


def safe_send(topic, value):
    global producer
    send_attempts = int(os.getenv("KAFKA_SEND_MAX_ATTEMPTS", "3"))
    send_delay = float(os.getenv("KAFKA_SEND_RETRY_DELAY_SEC", "0.5"))

    # CLI entrypoints call generate_telemetry() directly, so initialize lazily.
    if producer is None:
        producer = create_kafka_producer_with_retry()

    for attempt in range(1, send_attempts + 1):
        try:
            producer.send(topic, value=value)
            return
        except (KafkaError, NoBrokersAvailable, OSError) as exc:
            if attempt >= send_attempts:
                raise
            print(f"WARN Kafka send failed (attempt {attempt}/{send_attempts}): {exc}; reconnecting producer...")
            try:
                producer.close(timeout=2)
            except Exception:
                pass
            producer = create_kafka_producer_with_retry()
            time.sleep(send_delay)


def load_entities_from_mongo():
    """Load user/problem IDs from MongoDB so simulator follows seeded metadata."""
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017,localhost:27018,localhost:27019/code_metrics?replicaSet=rs0",
    )
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        db = client["code_metrics"]
        users = [doc["_id"] for doc in db["users"].find({}, {"_id": 1})]
        problems = [doc["_id"] for doc in db["problems"].find({}, {"_id": 1})]

        if len(users) < MIN_SIM_USERS:
            users = FALLBACK_USERS
        if len(problems) < MIN_SIM_PROBLEMS:
            problems = FALLBACK_PROBLEMS

        print(f"Loaded {len(users)} users and {len(problems)} problems from MongoDB")
        return users, problems
    except Exception as e:
        try:
            fallback_uri = "mongodb://localhost:27017/?directConnection=true"
            client = pymongo.MongoClient(fallback_uri, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            db = client["code_metrics"]
            users = [doc["_id"] for doc in db["users"].find({}, {"_id": 1})]
            problems = [doc["_id"] for doc in db["problems"].find({}, {"_id": 1})]
            if len(users) < MIN_SIM_USERS:
                users = FALLBACK_USERS
            if len(problems) < MIN_SIM_PROBLEMS:
                problems = FALLBACK_PROBLEMS
            print(f"Mongo fallback succeeded: loaded {len(users)} users and {len(problems)} problems")
            return users, problems
        except Exception as fallback_e:
            print(f"Mongo lookup failed ({e}); fallback failed ({fallback_e}). Using default IDs.")
            return FALLBACK_USERS, FALLBACK_PROBLEMS


def generate_telemetry(valid_users, valid_problems):
    last_reload = time.time()
    tick = 0
    hot_users = valid_users[: max(1, min(8, len(valid_users)))]
    ip_pool = [
        "10.10.0.11",
        "10.10.0.12",
        "10.10.0.13",
        "10.10.0.14",
        "10.10.0.15",
    ]
    while True:
        # Periodically reload user/problem IDs from Mongo so new seed data is picked up.
        if ENTITY_REFRESH_SECONDS > 0 and (time.time() - last_reload) >= ENTITY_REFRESH_SECONDS:
            valid_users, valid_problems = load_entities_from_mongo()
            hot_users = valid_users[: max(1, min(8, len(valid_users)))]
            last_reload = time.time()

        # 30% of events come from a small hot-user cohort to make short-window anomaly detection observable.
        chosen_user = random.choice(hot_users) if random.random() < 0.30 else random.choice(valid_users)

        # 12% of events simulate heavy latency spikes above the 5s alert threshold.
        if random.random() < 0.12:
            execution_time_ms = random.randint(5500, 12000)
        else:
            execution_time_ms = random.randint(10, 5000)

        log = {
            "submission_id": fake.uuid4(),
            "user_id": chosen_user,
            "problem_id": random.choice(valid_problems),
            "status": random.choice(STATUSES),
            "execution_time_ms": execution_time_ms,
            "memory_kb": random.randint(1024, 64000),
            "client_ip": random.choice(ip_pool),
            "timestamp": time.time()
        }
        
        safe_send(TOPIC_NAME, value=log)
        print(f"Sent: {log['user_id']} | {log['status']} | {log['execution_time_ms']}ms")

        # Emit operational telemetry every few events to feed system_metrics stream.
        if tick % 3 == 0:
            interval_metric = {
                "metric_id": fake.uuid4(),
                "user_id": log["user_id"],
                "metric_type": "submission_interval_sec",
                "value": random.randint(5, 120),
                "timestamp": time.time(),
            }
            safe_send(SYSTEM_METRICS_TOPIC, value=interval_metric)

        if tick % 2 == 0:
            metric_type = random.choice(["execution_time_ms", "memory_kb"])
            perf_metric = {
                "metric_id": fake.uuid4(),
                "user_id": log["user_id"],
                "metric_type": metric_type,
                "value": float(log[metric_type]),
                "timestamp": time.time(),
            }
            safe_send(SYSTEM_METRICS_TOPIC, value=perf_metric)

        tick += 1
        
        # Simulate traffic (e.g., 2 submissions per second)
        time.sleep(0.5) 

if __name__ == "__main__":
    print("Starting Code Metrics Telemetry Simulator...")
    producer = create_kafka_producer_with_retry()
    valid_users, valid_problems = load_entities_from_mongo()
    generate_telemetry(valid_users, valid_problems)