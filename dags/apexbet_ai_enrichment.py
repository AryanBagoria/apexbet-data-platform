"""Enrichment retry DAG.

Runs enrichment over Silver events; retries failed enrichment records. Keeps
raw input and enrichment output separate. Non-blocking with respect to
ingestion. Uses rules by default; ML/LLM only if explicitly enabled.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("APEXBET_HOME", "/opt/apexbet/src"))
sys.path.insert(0, "/opt/airflow/dags")

from common import default_args, task_enrichment_retry

with DAG(
    dag_id="apexbet_ai_enrichment",
    description="Periodic enrichment + retry of failed enrichment records.",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["apexbet", "enrichment"],
) as dag:
    enrich = PythonOperator(task_id="enrichment_retry", python_callable=task_enrichment_retry)
