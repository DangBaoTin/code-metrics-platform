# 11 - Academic Report Version

## Title

Code Metrics Data Platform: A Distributed Lambda Architecture for Real-Time Educational Telemetry and Nightly Analytics

## Abstract

This project implements an end-to-end distributed data engineering platform for an EdTech coding environment. The system ingests high-velocity telemetry through Apache Kafka, processes data in both streaming and batch modes with Apache Spark, persists operational and analytical layers in Apache Cassandra, enriches context with MongoDB metadata, and serves insights through a Streamlit dashboard. The implementation adopts a Lambda architecture and a medallion data model (Bronze, Silver, Gold). High availability is achieved through a Cassandra masterless ring and MongoDB replica set. The platform supports operational monitoring (leaderboard and alerts), business intelligence (revenue and instructor scorecards), and applied machine learning outputs (dropout risk and recommendations).

## 1. Introduction

Modern educational systems generate large and continuous event streams (submissions, execution metrics, behavior traces). Conventional monolithic pipelines struggle with throughput bursts, late-arriving data, and mixed real-time plus historical requirements. This work addresses these challenges with a cloud-native-style distributed architecture that separates ingestion, processing, storage, and serving concerns.

Research/engineering goals:

1. Build a robust real-time pipeline for leaderboard and anomaly alerts.
2. Build a scheduled batch pipeline for business and ML outputs.
3. Demonstrate distributed storage patterns with fault tolerance.
4. Provide an interactive serving layer suitable for operations and teaching staff.

## 2. System Overview

### 2.1 Core Components

- Ingestion: Kafka topics `raw_submissions` and `system_metrics`
- Streaming compute: Spark Structured Streaming
- Batch compute: Spark SQL + Spark MLlib
- Storage:
  - Cassandra ring for medallion and analytics tables
  - MongoDB replica set for metadata and dimensions
- Orchestration: Airflow DAG for nightly ETL
- Serving: Streamlit dashboard with operational and BI/AI views

### 2.2 Architectural Pattern

- Lambda architecture:
  - Speed layer for low-latency operational monitoring
  - Batch layer for comprehensive historical analytics and model outputs
- Medallion storage pattern:
  - Bronze (raw durability)
  - Silver (clean structured facts)
  - Gold (consumer-ready KPIs and predictions)

## 3. Data Model and Storage Design

### 3.1 Cassandra

Keyspace `code_metrics` with replication factor 3 (local distributed deployment). Core tables:

- Bronze: raw submissions, raw system metrics
- Silver: partitioned and time-ordered submissions
- Gold: leaderboard, alerts, engagement, heatmap, revenue, instructor metrics, dropout predictions, recommendations, adaptive profiles

Design rationale:

- Write-heavy streams land in Bronze with minimal transform.
- Silver normalizes telemetry for efficient aggregations.
- Gold tables are shaped for direct dashboard and model consumption.

### 3.2 MongoDB

Collections: users, problems, transactions, ratings. Used as a metadata plane for enrichment and business dimensions.

HA approach:

- Replica set (`rs0`) with one primary and two secondaries.
- Automatic failover and high read availability.

## 4. Distributed and High-Availability Implementation

### 4.1 Cassandra Masterless Ring

- Three homogeneous Cassandra nodes participate equally in request coordination.
- No single master node; coordinator responsibility is dynamic.
- Replication ensures resilience under node faults.

### 4.2 MongoDB Primary-Secondary

- One primary for writes, secondaries for replication/read availability.
- Replica-set initialization is automated at deployment startup.

### 4.3 Orchestration Resilience

- Health-checked dependency startup in compose.
- Airflow scheduler restart loop to reduce operational downtime.

## 5. Streaming Pipeline Implementation

Main module: `src/code_metrics/processing/stream_leaderboard.py`

Key capabilities:

1. Startup readiness checks for Cassandra.
2. Optional stream reset mode (checkpoint cleanup + table truncate).
3. Kafka consumption from two topics with controlled offset behavior (`failOnDataLoss` support).
4. Retry-aware Cassandra writes with session reuse.
5. Real-time output materialization:
   - `gold_live_leaderboard`
   - `gold_system_alerts`

Alert rules:

- HIGH_LATENCY when execution metric > 5000ms
- CHEAT_DETECTED on short-window submission/IP anomalies

## 6. Batch Pipeline Implementation

Main module: `src/code_metrics/processing/batch_etl.py`

Stages:

1. Bronze load and JSON parsing
2. Silver table generation
3. Feature engineering using submission and system metrics
4. Gold BI output generation
5. Gold AI output generation

ML outputs:

- Logistic regression-based dropout probability
- ALS-based next-problem recommendations
- Adaptive difficulty profile derivation

## 7. Dashboard and Analytics Serving

Main module: `src/code_metrics/dashboard/app.py`

Views include:

- Real-time leaderboard and system alerts
- Difficulty heatmap
- Subscription revenue dashboard
- Instructor report card
- Dropout risk monitor
- AI recommendation panel

The dashboard applies live refresh and source-aware status messaging to support operational monitoring.

## 8. Reliability Engineering and Stabilization

Observed failure modes during integration included:

- Cassandra warm-up race conditions
- Kafka offset mismatch with checkpoint divergence
- Simulator broker unavailability at startup
- Checkpoint deletion race on macOS

Mitigations implemented:

- Connection retries and readiness probes
- Stream reset with retry-safe checkpoint deletion
- Session reuse for Cassandra writes
- Kafka producer retry and reconnect logic in simulator
- Conservative local consistency defaults (`LOCAL_ONE`)

## 9. Evaluation Summary

The platform demonstrates:

1. End-to-end distributed ingestion and processing.
2. Real-time operations insight under evolving data.
3. Nightly analytical refresh with BI/AI outputs.
4. Practical HA patterns suitable for local cluster simulation and extension to production-style deployment.

## 10. Limitations and Future Work

Current limitations:

- Local environment constraints (single-host networking and resource limits)
- Limited formal benchmarking and SLA measurement
- Heuristic thresholds for certain operational alerts

Future work:

1. Add formal load tests and throughput/latency benchmarks.
2. Introduce schema registry and stronger data contracts.
3. Improve model training workflows with experiment tracking.
4. Add deployment profiles for cloud-managed services.

## 11. Conclusion

This project provides a complete educational reference implementation of a modern distributed data platform. It integrates stream and batch processing, demonstrates practical HA patterns, and exposes analytical outcomes through an operator-friendly dashboard. The resulting system is both instructional and operationally meaningful for data engineering coursework and prototype deployments.
