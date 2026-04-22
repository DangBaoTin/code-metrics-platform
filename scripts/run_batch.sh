#!/bin/bash
# scripts/run_batch.sh
# This script manually triggers the PySpark nightly batch ETL
# as a fallback if Airflow is unavailable or timing out.

set -e

echo "========================================="
echo "   Starting Manual Nightly Batch ETL     "
echo "========================================="

# Exporting host-safe Cassandra variables (similar to the streaming script)
# This prevents timeouts when running locally from the host machine
export CASSANDRA_HOST="127.0.0.1"
export CASSANDRA_CONTACT_POINTS="127.0.0.1"
export CASSANDRA_ALLOWED_HOSTS="127.0.0.1,localhost"
export CASSANDRA_CONNECT_TIMEOUT_SEC=20
export CASSANDRA_REQUEST_TIMEOUT_SEC=25
export CASSANDRA_WRITE_CONSISTENCY="LOCAL_ONE"
export CASSANDRA_READ_CONSISTENCY="LOCAL_ONE"
export PYTHONPATH=src

# Force a clean gold-layer truncation on each batch run
export BATCH_RESET_GOLD="true"

# Run the batch job via the CLI entrypoint
uv run python -m code_metrics.cli batch

echo ""
echo "========================================="
echo "         Batch ETL Completed!            "
echo "========================================="
