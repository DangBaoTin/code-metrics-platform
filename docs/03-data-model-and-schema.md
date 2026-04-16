# 03 - Data Model and Schema

## Cassandra Keyspace

Keyspace: `code_metrics`
Replication config in schema file uses `SimpleStrategy` with replication factor 3 for local distributed testing.

## Cassandra Tables

### Bronze

- `bronze_raw_submissions`
  - Primary key: `submission_id`
  - Stores original JSON payload and ingest timestamp
- `bronze_raw_system_metrics`
  - Primary key: `metric_id`
  - Stores original metric payload and ingest timestamp

### Silver

- `silver_submissions`
  - Partition key: `(problem_id, user_id, day)`
  - Clustering key: `timestamp DESC`
  - Structured submission telemetry for analytics

### Gold (Operational + BI + AI)

- `gold_live_leaderboard`
- `gold_system_alerts`
- `gold_engagement_scores`
- `gold_difficulty_heatmap`
- `gold_subscription_revenue`
- `gold_instructor_report_card`
- `gold_dropout_predictions`
- `gold_next_problem_recommendations`
- `gold_adaptive_difficulty_profiles`

## MongoDB Collections

Database: `code_metrics`

- `users`
- `problems` (includes instructor metadata)
- `transactions`
- `ratings`

These collections supply metadata enrichment and business dimensions for Spark jobs and dashboard views.

## Schema Files and Owners

- Cassandra schema definition: `storage/init_cassandra.cql`
- Mongo seed and synthetic data model: `src/code_metrics/storage/init_mongo.py`

## Notes on Query Design

- Bronze favors write durability and ingestion simplicity.
- Silver and Gold are shaped for query patterns used by dashboard charts and model outputs.
- Gold tables are snapshot-like and refreshed by batch/stream workflows.

## dbdiagram.io Production-Ready Reference (NoSQL-Aware)

Use this as a presentation-friendly reference for MongoDB + Cassandra. The `Ref` lines below are **logical application joins** (not FK constraints enforced by either database).

```dbml
// ==========================================
// 1) MONGODB (OPERATIONAL METADATA)
// ==========================================
TableGroup MongoDB {
  Users
  Problems
  Transactions
  Ratings
}

Table Users {
  _id varchar [primary key, note: 'Canonical user_id shared with Cassandra (string ID strategy)']
  full_name varchar
  email varchar [note: 'Recommend unique index in MongoDB']
  major varchar
  class_cohort varchar
  account_status varchar
  created_at timestamp

  indexes {
    email [unique]
    class_cohort
    major
  }
}

Table Problems {
  _id varchar [primary key, note: 'Canonical problem_id shared with Cassandra']
  title varchar
  category varchar
  difficulty varchar
  time_limit_ms int
  instructor_id varchar
  instructor_name varchar

  indexes {
    category
    difficulty
    instructor_id
  }
}

Table Transactions {
  _id varchar [primary key]
  user_id varchar
  plan_type varchar
  amount_usd float
  date timestamp

  indexes {
    user_id
    date
    plan_type
  }
}

Table Ratings {
  _id varchar [primary key]
  problem_id varchar
  user_id varchar
  rating_score int
  feedback text

  indexes {
    problem_id
    user_id
    rating_score
  }
}

// ==========================================
// 2) CASSANDRA (BIG DATA TELEMETRY)
// Query-first denormalization, no joins.
// ==========================================
TableGroup Cassandra_Cluster {
  Bronze_Raw_Submissions_V2
  Silver_Submissions_By_User_Day
  Silver_Submissions_By_Problem_Day
  Gold_Live_Leaderboard
  Gold_Difficulty_Heatmap
  Gold_System_Alerts
}

Table Bronze_Raw_Submissions_V2 {
  ingest_day date [primary key]
  ingest_timestamp timestamp [primary key]
  submission_id text [primary key]
  raw_payload text [note: 'Raw JSON from Kafka']
  note: 'Partition Key: ingest_day; Clustering: ingest_timestamp DESC, submission_id ASC'
}

Table Silver_Submissions_By_User_Day {
  day date [primary key]
  user_id text [primary key]
  timestamp timestamp [primary key]
  submission_id text
  problem_id text
  status text
  execution_time_ms int
  memory_kb int
  note: 'Primary read path for dashboard/user timeline. Partition Key: (day, user_id); Clustering: timestamp DESC'
}

Table Silver_Submissions_By_Problem_Day {
  day date [primary key]
  problem_id text [primary key]
  timestamp timestamp [primary key]
  submission_id text
  user_id text
  status text
  execution_time_ms int
  memory_kb int
  note: 'Read path for problem analytics/heatmaps. Partition Key: (day, problem_id); Clustering: timestamp DESC'
}

Table Gold_Live_Leaderboard {
  rank_tier text [primary key]
  total_solved bigint [primary key]
  user_id text [primary key]
  recent_status text
  last_updated timestamp
  note: 'Partition Key: rank_tier; Clustering: total_solved DESC, user_id ASC'
}

Table Gold_Difficulty_Heatmap {
  problem_id text [primary key]
  total_attempts int
  failed_attempts int
  fail_rate_pct float
  category text
  last_updated timestamp
}

Table Gold_System_Alerts {
  alert_id uuid [primary key]
  alert_type text
  user_id text
  description text
  triggered_at timestamp
}

// ==========================================
// 3) LOGICAL RELATIONSHIPS (APP-LAYER ONLY)
// ==========================================
Ref: Users._id < Silver_Submissions_By_User_Day.user_id
Ref: Users._id < Silver_Submissions_By_Problem_Day.user_id
Ref: Problems._id < Silver_Submissions_By_Problem_Day.problem_id

Ref: Users._id < Gold_Live_Leaderboard.user_id
Ref: Problems._id < Gold_Difficulty_Heatmap.problem_id

Ref: Users._id < Transactions.user_id
Ref: Users._id < Ratings.user_id
Ref: Problems._id < Ratings.problem_id
```

### Why this is better for NoSQL

- MongoDB and Cassandra do not enforce cross-database foreign keys, so references are documented at the architecture level.
- Cassandra is modeled by query path, so separate Silver tables are intentional (duplicate-by-design) for fast reads.
- Bronze is bucketed by day so replay/backfill and time-range scans are practical at scale.
