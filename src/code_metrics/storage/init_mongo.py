import os
import random
from datetime import datetime, timezone

import pymongo
from faker import Faker


fake = Faker()


def _build_users(total_users: int):
    base_users = [
        {"_id": "user_001", "full_name": "Nguyen Van A", "class_cohort": "K22", "major": "Computer Science"},
        {"_id": "user_002", "full_name": "Tran Thi B", "class_cohort": "K22", "major": "Data Science"},
        {"_id": "user_003", "full_name": "Le Van C", "class_cohort": "K23", "major": "Computer Engineering"},
        {"_id": "user_004", "full_name": "Pham Thi D", "class_cohort": "K21", "major": "Computer Science"},
        {"_id": "user_005", "full_name": "Bao Tin", "class_cohort": "K22", "major": "Computer Science"},
    ]

    majors = [
        "Computer Science",
        "Data Science",
        "Computer Engineering",
        "Information Systems",
        "Artificial Intelligence",
    ]
    cohorts = ["K21", "K22", "K23", "K24"]

    users = list(base_users)
    for idx in range(6, total_users + 1):
        users.append(
            {
                "_id": f"user_{idx:03d}",
                "full_name": fake.name(),
                "class_cohort": random.choice(cohorts),
                "major": random.choice(majors),
            }
        )
    return users


def _build_problems(total_problems: int):
    instructors = [
        ("inst_001", "Dr. Tran Minh"),
        ("inst_002", "Dr. Pham An"),
        ("inst_003", "Prof. Le Quang"),
        ("inst_004", "Prof. Nguyen Hoa"),
    ]
    base_problems = [
        {
            "_id": "prob_1",
            "title": "Two Sum",
            "category": "Arrays",
            "difficulty": "Easy",
            "time_limit_ms": 1000,
            "instructor_id": "inst_001",
            "instructor_name": "Dr. Tran Minh",
        },
        {
            "_id": "prob_2",
            "title": "Reverse Linked List",
            "category": "Linked Lists",
            "difficulty": "Medium",
            "time_limit_ms": 2000,
            "instructor_id": "inst_002",
            "instructor_name": "Dr. Pham An",
        },
        {
            "_id": "prob_3",
            "title": "Dijkstra's Algorithm",
            "category": "Graphs",
            "difficulty": "Hard",
            "time_limit_ms": 5000,
            "instructor_id": "inst_003",
            "instructor_name": "Prof. Le Quang",
        },
        {
            "_id": "prob_4",
            "title": "Valid Parentheses",
            "category": "Stacks",
            "difficulty": "Easy",
            "time_limit_ms": 1000,
            "instructor_id": "inst_004",
            "instructor_name": "Prof. Nguyen Hoa",
        },
        {
            "_id": "prob_5",
            "title": "Binary Tree Traversal",
            "category": "Trees",
            "difficulty": "Medium",
            "time_limit_ms": 2000,
            "instructor_id": "inst_001",
            "instructor_name": "Dr. Tran Minh",
        },
    ]

    categories = ["Arrays", "Linked Lists", "Graphs", "Stacks", "Trees", "DP", "Greedy", "Strings"]
    difficulty_weights = ["Easy", "Easy", "Medium", "Medium", "Hard"]

    problems = list(base_problems)
    for idx in range(6, total_problems + 1):
        difficulty = random.choice(difficulty_weights)
        base_limit = {"Easy": 1000, "Medium": 2000, "Hard": 5000}[difficulty]
        instructor_id, instructor_name = random.choice(instructors)
        problems.append(
            {
                "_id": f"prob_{idx}",
                "title": f"{random.choice(categories)} Challenge {idx}",
                "category": random.choice(categories),
                "difficulty": difficulty,
                "time_limit_ms": base_limit,
                "instructor_id": instructor_id,
                "instructor_name": instructor_name,
            }
        )
    return problems


def _build_transactions(users, total_txns: int):
    plan_prices = {"Basic": 5.0, "Pro": 15.0, "Enterprise": 29.0}
    user_ids = [u["_id"] for u in users]
    txns = []
    for idx in range(1, total_txns + 1):
        plan = random.choice(list(plan_prices.keys()))
        txns.append(
            {
                "_id": f"txn_{idx:04d}",
                "user_id": random.choice(user_ids),
                "plan_type": plan,
                "amount_usd": plan_prices[plan],
                "date": datetime.now(timezone.utc),
            }
        )
    return txns


def _build_ratings(users, problems, total_ratings: int):
    user_ids = [u["_id"] for u in users]
    problem_ids = [p["_id"] for p in problems]
    feedback_pool = [
        "Helpful explanation.",
        "Great challenge!",
        "Could use more examples.",
        "Difficulty feels accurate.",
        "Nice problem statement.",
    ]
    ratings = []
    for idx in range(1, total_ratings + 1):
        ratings.append(
            {
                "_id": f"rat_{idx:04d}",
                "problem_id": random.choice(problem_ids),
                "user_id": random.choice(user_ids),
                "rating_score": random.randint(1, 5),
                "feedback": random.choice(feedback_pool),
            }
        )
    return ratings

def init_mongodb():
    print("Connecting to MongoDB...")
    # Connect to the local MongoDB container
    try:
        client = pymongo.MongoClient(
            "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
            serverSelectionTimeoutMS=3000,
        )
        client.admin.command("ping")
    except Exception:
        client = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true", serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    
    # Create/Connect to the code_metrics database
    db = client["code_metrics"]
    
    total_users = int(os.getenv("MONGO_SEED_USERS", "120"))
    total_problems = int(os.getenv("MONGO_SEED_PROBLEMS", "80"))
    total_txns = int(os.getenv("MONGO_SEED_TXNS", "180"))
    total_ratings = int(os.getenv("MONGO_SEED_RATINGS", "320"))

    # ==========================================
    # 1. USERS COLLECTION
    # ==========================================
    users = db["users"]
    users.drop()  # Clear old data for a fresh start

    user_data = _build_users(total_users)
    users.insert_many(user_data)
    print(f"✅ Inserted {len(user_data)} Users")

    # ==========================================
    # 2. PROBLEMS COLLECTION
    # ==========================================
    problems = db["problems"]
    problems.drop()

    problem_data = _build_problems(total_problems)
    problems.insert_many(problem_data)
    print(f"✅ Inserted {len(problem_data)} Problems")

    # ==========================================
    # 3. TRANSACTIONS COLLECTION (Subscriptions)
    # ==========================================
    transactions = db["transactions"]
    transactions.drop()

    txn_data = _build_transactions(user_data, total_txns)
    transactions.insert_many(txn_data)
    print(f"✅ Inserted {len(txn_data)} Transactions")

    # ==========================================
    # 4. RATINGS COLLECTION (Instructor Report Card)
    # ==========================================
    ratings = db["ratings"]
    ratings.drop()

    rating_data = _build_ratings(user_data, problem_data, total_ratings)
    ratings.insert_many(rating_data)
    print(f"✅ Inserted {len(rating_data)} Ratings")
    
    print("\n🎉 MongoDB Initialization Complete!")

if __name__ == "__main__":
    init_mongodb()