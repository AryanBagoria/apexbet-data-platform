"""Data freshness + quality monitoring DAG.

Fails (raising) when freshness or quality SLA is breached so Airflow surfaces
the problem and can trigger alerts.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("APEXBET_HOME", "/opt/apexbet/src"))
sys.path.insert(0, "/opt/airflow/dags")

from common import default_args, task_data_quality, task_freshness


def _assert_fresh(**context) -> None:
    result = task_freshness()
    if not result.get("freshness_ok"):
        raise ValueError(f"freshness SLA breached: {result}")


with DAG(
    dag_id="apexbet_freshness_and_quality",
    description="Checks data freshness and data-quality SLA; fails loudly on breach.",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args(),
    tags=["apexbet", "monitoring", "quality"],
) as dag:
    fresh = PythonOperator(task_id="freshness_sla", python_callable=_assert_fresh)
    dq = PythonOperator(task_id="quality_checks", python_callable=task_data_quality)
    fresh >> dq
