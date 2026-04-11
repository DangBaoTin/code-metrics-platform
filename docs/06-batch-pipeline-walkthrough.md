# 06 - Batch Pipeline Walkthrough

Source: `src/code_metrics/processing/batch_etl.py`

## Responsibilities

- Read Bronze tables and parse payloads
- Build Silver submission fact table
- Produce BI-ready and AI-ready Gold tables
- Enrich with Mongo metadata

## Batch Flow

1. Optional Gold table truncation (snapshot refresh behavior)
2. Load Bronze submissions and parse JSON into structured columns
3. Persist `silver_submissions`
4. Load Bronze metrics and derive login-time proxy features
5. Build user-level feature set
6. Compute and write Gold outputs

## BI Outputs

- `gold_engagement_scores`
  - Combines login proxy and solved count
- `gold_difficulty_heatmap`
  - Attempts, failures, fail rate, category
- `gold_subscription_revenue`
  - Aggregated from Mongo transactions
- `gold_instructor_report_card`
  - Aggregated from ratings and problem instructor metadata

## AI Outputs

- `gold_dropout_predictions`
  - Logistic regression on behavioral features
- `gold_next_problem_recommendations`
  - ALS collaborative filtering on pass interactions
- `gold_adaptive_difficulty_profiles`
  - Tiering and recommended next difficulty

## Data Enrichment

Batch job uses Mongo collections for business dimensions:

- users
- problems
- transactions
- ratings

## Important Config

- `BATCH_RESET_GOLD` (default refresh mode enabled)
- `CASSANDRA_WRITE_CONSISTENCY`
- `CASSANDRA_READ_CONSISTENCY`
- `CASSANDRA_ALLOWED_HOSTS`

## Failure Tolerance

- Retry logic for gold-table truncation
- Mongo direct-connection fallback when replica-set DNS is not resolvable from host
