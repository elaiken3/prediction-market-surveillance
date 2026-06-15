"""Airflow DAG: batch surveillance.

The ingestion + Spark stream run continuously; this DAG runs on a schedule to
refresh resolved-market ground truth, rebuild the dbt models (coherence, volume
anomalies, calibration), and reconcile against the real-time flags.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/market_surveillance")
REPO_DIR = os.environ.get("REPO_DIR", "/opt/airflow")
LAKE_PATH = os.environ.get("LAKE_PATH", "/opt/airflow/data/lake")

default_args = {"owner": "eaiken", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="market_surveillance_batch",
    description="Refresh resolutions, rebuild dbt, reconcile stream vs batch",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["prediction-markets", "dbt", "surveillance"],
) as dag:

    fetch_resolutions = BashOperator(
        task_id="fetch_resolutions",
        bash_command=f"cd {REPO_DIR} && python -m ingestion.fetch_resolutions --lake {LAKE_PATH} --limit 200",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT_DIR} && dbt deps && dbt build --vars '{{lake_path: {LAKE_PATH}}}'",
    )

    report = BashOperator(
        task_id="reconciliation_report",
        bash_command=(
            f"cd {PROJECT_DIR} && dbt show --inline "
            "\"select reconciliation_status, count(*) n from {{ ref('rec_stream_vs_batch') }} group by 1\" "
            "--vars '{lake_path: " + LAKE_PATH + "}'"
        ),
    )

    fetch_resolutions >> dbt_build >> report
