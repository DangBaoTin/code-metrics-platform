# Code Metrics Platform Documentation Suite

This folder contains a complete technical documentation set for implementation, operations, and evaluation.

## Document Map

1. [01 - Getting Started](./01-getting-started.md)
2. [02 - System Architecture](./02-system-architecture.md)
3. [03 - Data Model and Schema](./03-data-model-and-schema.md)
4. [04 - Distributed and HA Design](./04-distributed-and-ha-design.md)
5. [05 - Streaming Pipeline Walkthrough](./05-streaming-pipeline-walkthrough.md)
6. [06 - Batch Pipeline Walkthrough](./06-batch-pipeline-walkthrough.md)
7. [07 - Dashboard and Serving Layer](./07-dashboard-and-serving-layer.md)
8. [08 - Airflow Orchestration](./08-airflow-orchestration.md)
9. [09 - Operations Runbook](./09-operations-runbook.md)
10. [10 - Troubleshooting Knowledge Base](./10-troubleshooting-knowledge-base.md)
11. [11 - Academic Report Version](./11-academic-report.md)
12. [12 - Slide-Ready Outline](./12-slide-ready-outline.md)
13. [13 - Bao Cao Hoc Thuat (Tieng Viet)](./13-bao-cao-hoc-thuat-tieng-viet.md)
14. [14 - Objective 5.1-5.5 Mapping Matrix](./14-objective-mapping-matrix.md)
15. [15 - Final Submission Checklist](./15-final-submission-checklist.md)

## Source of Truth in Code

- Runtime and service topology: `docker-compose.yml`
- Cassandra schema: `storage/init_cassandra.cql`
- Mongo seed model: `src/code_metrics/storage/init_mongo.py`
- Streaming implementation: `src/code_metrics/processing/stream_leaderboard.py`
- Batch implementation: `src/code_metrics/processing/batch_etl.py`
- Dashboard implementation: `src/code_metrics/dashboard/app.py`
- Airflow DAG: `airflow/dags/code_metrics_dag.py`
- CLI entrypoints: `src/code_metrics/cli.py`
