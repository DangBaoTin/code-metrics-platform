# 12 - Slide-Ready Outline

Use this as a 10-12 slide presentation script.

## Slide 1 - Title

- Code Metrics Data Platform
- Distributed telemetry pipeline for EdTech
- Team, course, date

## Slide 2 - Problem Statement

- High-volume submission telemetry
- Need both real-time operations and nightly analytics
- Need resilient distributed storage and orchestration

## Slide 3 - Solution Overview

- Lambda architecture
- Kafka + Spark Streaming + Spark Batch
- Cassandra + MongoDB + Airflow + Streamlit

## Slide 4 - End-to-End Data Flow

- Simulator -> Kafka topics
- Streaming path -> leaderboard + alerts
- Batch path -> BI/AI gold tables
- Dashboard consumes both

## Slide 5 - Distributed Storage Design

- Cassandra 3-node masterless ring
- MongoDB replica set (Primary/Secondary)
- Why this combination (write throughput + metadata flexibility)

## Slide 6 - Medallion Data Model

- Bronze raw durability
- Silver normalized facts
- Gold consumer-ready analytics
- Example tables and consumers

## Slide 7 - Real-Time Features

- Live leaderboard
- HIGH_LATENCY alert (>5s)
- CHEAT_DETECTED (window + multi-IP anomaly)

## Slide 8 - Batch BI and AI Features

- Engagement scores
- Difficulty heatmap
- Subscription revenue
- Instructor report card
- Dropout risk + recommendation outputs

## Slide 9 - Reliability Engineering

- Warm-up race and readiness probes
- Retry strategies for Cassandra/Kafka
- Checkpoint reset and offset mismatch handling
- Session reuse improvements

## Slide 10 - Demo Screens

- Leaderboard view
- System alerts view
- BI dashboards and AI recommendation panel

## Slide 11 - Lessons Learned

- Distributed local environments require explicit health gating
- Stream + batch consistency demands operational discipline
- Clear observability and runbooks reduce debugging time

## Slide 12 - Future Work

- Load testing and SLA metrics
- Cloud-native deployment profiles
- Model monitoring and data contracts

## Presenter Notes

- Keep each slide to 3-5 bullets.
- For architecture slides, use diagrams from docs 02 and 04.
- For demo, show data moving live first, then batch outputs.
