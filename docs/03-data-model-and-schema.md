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
