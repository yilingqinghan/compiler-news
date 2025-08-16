from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, sys
sys.path.append(os.path.abspath("/opt/airflow/dags/.."))

from pipelines.ingest_rss import main as ingest_rss
from pipelines.extract import main as extract_main
from pipelines.dedupe_cluster import main as cluster_main
from pipelines.summarize import main as summarize_main
from pipelines.publish import main as publish_main

default_args = {"retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    "compiler_intel_daily",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 7 * * *",
    catchup=False,
    default_args=default_args,
    tags=["compiler","intel","daily"],
) as dag:
    t1 = PythonOperator(task_id="ingest_rss", python_callable=ingest_rss)
    t2 = PythonOperator(task_id="extract", python_callable=extract_main)
    t3 = PythonOperator(task_id="cluster", python_callable=cluster_main)
    t4 = PythonOperator(task_id="summarize", python_callable=summarize_main)
    t5 = PythonOperator(task_id="publish", python_callable=publish_main)
    t1 >> t2 >> t3 >> t4 >> t5
