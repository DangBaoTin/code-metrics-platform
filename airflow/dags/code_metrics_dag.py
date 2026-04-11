from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def run_batch_job_task():
    # Import at runtime so DAG parsing stays lightweight.
    from code_metrics.processing.batch_etl import run_batch_job

    run_batch_job()

default_args = {
    'owner': 'Group_4',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 3), 
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nightly_code_metrics_etl',
    default_args=default_args,
    description='Runs the nightly Spark Batch ETL for Heatmap and Risk Monitor',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'cassandra', 'batch']
) as dag:

    run_spark_batch = PythonOperator(
        task_id='trigger_spark_cassandra_etl',
        python_callable=run_batch_job_task,
    )