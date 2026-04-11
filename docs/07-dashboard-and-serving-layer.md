# 07 - Dashboard and Serving Layer

Source: `src/code_metrics/dashboard/app.py`

## Purpose

Single UI for real-time operations and batch analytics.

## Data Access

- Cassandra for streaming and batch result tables
- Mongo for user/problem metadata enrichment
- Connection startup includes retries and host-safe settings

## Navigation Views

- Overview
- Live Leaderboard
- Difficulty Heatmap
- Subscription Dashboard
- Instructor Report Card
- Dropout Risk
- AI Recommender
- System Alerts

## Real-Time Views

- Live Leaderboard uses `gold_live_leaderboard`
- System Alerts uses `gold_system_alerts`

## Batch Views

- Heatmap uses `gold_difficulty_heatmap`
- Subscription uses `gold_subscription_revenue`
- Instructor uses `gold_instructor_report_card`
- Dropout Risk uses `gold_dropout_predictions` + adaptive profiles
- AI Recommender uses `gold_next_problem_recommendations` + adaptive profiles

## UX/Behavior

- Auto-refresh with configurable interval
- Status tape and KPI cards
- Sidebar navigation and layout mode
- Graceful user messaging when source tables are empty

## Operational Diagnostics in UI

Sidebar includes database status and refresh timestamp so operators can quickly see data-source availability.
