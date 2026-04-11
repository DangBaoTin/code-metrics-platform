-- =========================================================
-- CODE METRICS DATA PLATFORM - CASSANDRA SCHEMA
-- =========================================================

-- Create the Keyspace (Database)
-- Using SimpleStrategy with RF=1 for local Podman development. 
-- In production, this would be NetworkTopologyStrategy with RF=3.
CREATE KEYSPACE IF NOT EXISTS code_metrics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE code_metrics;

-- =========================================================
-- 🥉 BRONZE LAYER (Raw Ingestion)
-- =========================================================
-- Purpose: An append-only table to dump raw JSON directly 
-- from Kafka as fast as possible to prevent data loss.
CREATE TABLE IF NOT EXISTS bronze_raw_submissions (
    submission_id text,
    ingest_timestamp timestamp,
    raw_payload text,
    PRIMARY KEY (submission_id)
);

CREATE TABLE IF NOT EXISTS bronze_raw_system_metrics (
    metric_id text,
    ingest_timestamp timestamp,
    raw_payload text,
    PRIMARY KEY (metric_id)
);

-- =========================================================
-- 🥈 SILVER LAYER (Cleaned & Partitioned Telemetry)
-- =========================================================
-- Purpose: Cleaned, structured logs ready for time-series queries.
-- Note: This EXACTLY matches your design from Slide 14!
CREATE TABLE IF NOT EXISTS silver_submissions (
    problem_id text,
    user_id text,
    day date,
    timestamp timestamp,
    submission_id text,
    status text,
    execution_time_ms int,
    memory_kb int,
    PRIMARY KEY ((problem_id, user_id, day), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- =========================================================
-- 🥇 GOLD LAYER (Business-Ready Aggregates for Dashboard)
-- =========================================================

-- 1. Student Engagement Score (Consumed by Dashboard & ML)
CREATE TABLE IF NOT EXISTS gold_engagement_scores (
    user_id text,
    engagement_score int,
    last_updated timestamp,
    PRIMARY KEY (user_id)
);

-- 2. Difficulty Heatmap (Consumed by Instructors)
CREATE TABLE IF NOT EXISTS gold_difficulty_heatmap (
    problem_id text,
    total_attempts int,
    failed_attempts int,
    fail_rate_pct float,
    category text,
    last_updated timestamp,
    PRIMARY KEY (problem_id)
);

-- 3. Live Leaderboard (Updated via Spark Streaming)
CREATE TABLE IF NOT EXISTS gold_live_leaderboard (
    rank_tier text,     -- e.g., 'Global' or 'Class_A' to act as partition
    user_id text,
    total_solved bigint,
    recent_status text,
    last_updated timestamp,
    PRIMARY KEY ((rank_tier), user_id)
);

-- 🥇 GOLD LAYER: System & Security Alerts
CREATE TABLE IF NOT EXISTS gold_system_alerts (
    alert_id uuid PRIMARY KEY,
    alert_type text,      -- 'CHEAT_DETECTED' or 'HIGH_LATENCY'
    user_id text,         -- Who cheated
    description text,     -- e.g., "Submission < 30s" or "Execution > 5s"
    triggered_at timestamp
);

-- 4. Subscription Revenue Analytics
CREATE TABLE IF NOT EXISTS gold_subscription_revenue (
    plan_type text,
    total_subscriptions bigint,
    total_revenue_usd double,
    last_updated timestamp,
    PRIMARY KEY (plan_type)
);

-- 5. Instructor Report Card
CREATE TABLE IF NOT EXISTS gold_instructor_report_card (
    instructor_id text,
    instructor_name text,
    avg_rating double,
    total_ratings bigint,
    last_updated timestamp,
    PRIMARY KEY (instructor_id)
);

-- 6. Dropout Prediction (Logistic Regression output)
CREATE TABLE IF NOT EXISTS gold_dropout_predictions (
    user_id text,
    dropout_probability double,
    risk_label text,
    last_updated timestamp,
    PRIMARY KEY (user_id)
);

-- 7. Next Problem Recommendation (Collaborative Filtering)
CREATE TABLE IF NOT EXISTS gold_next_problem_recommendations (
    user_id text,
    recommended_problem_id text,
    recommendation_score double,
    last_updated timestamp,
    PRIMARY KEY (user_id)
);

-- 8. Adaptive Difficulty Profiles
CREATE TABLE IF NOT EXISTS gold_adaptive_difficulty_profiles (
    user_id text,
    performance_tier text,
    recommended_difficulty text,
    last_updated timestamp,
    PRIMARY KEY (user_id)
);