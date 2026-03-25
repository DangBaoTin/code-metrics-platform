import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw_submissions'
STATUSES = ['Pass', 'Time Limit Exceeded', 'Runtime Error', 'Wrong Answer']

def generate_telemetry():
    while True:
        log = {
            "submission_id": fake.uuid4(),
            "user_id": f"u_{random.randint(1, 1000)}",
            "problem_id": f"p_{random.randint(1, 50)}",
            "status": random.choice(STATUSES),
            "execution_time_ms": random.randint(10, 5000),
            "memory_kb": random.randint(1024, 64000),
            "timestamp": time.time()
        }
        
        producer.send(TOPIC_NAME, value=log)
        print(f"Sent: {log['user_id']} | {log['status']} | {log['execution_time_ms']}ms")
        
        # Simulate traffic (e.g., 2 submissions per second)
        time.sleep(0.5) 

if __name__ == "__main__":
    print("Starting Code Metrics Telemetry Simulator...")
    generate_telemetry()