"""Historical backfill DAG (manual trigger).

Replays Bronze (or a raw JSONL backfill file) through Silver + Gold for a date
window. Idempotent: overwrites the recomputed partitions. Use for recovery and
for rebuilding derived datasets after a logic change.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("APEXBET_HOME", "/opt/apexbet/src"))
sys.path.insert(0, "/opt/airflow/dags")

from common import (
    default_args,
    task_build_gold,
    task_data_quality,
    task_reconcile,
)


def task_backfill_silver(**context) -> dict:
    from common import task_build_silver

    return task_build_silver()


with DAG(
    dag_id="apexbet_backfill",
    description="Manual backfill: replay Bronze through Silver and Gold for a window.",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["apexbet", "backfill", "replay"],
) as dag:
    backfill = PythonOperator(task_id="backfill_silver", python_callable=task_backfill_silver)
    gold = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
    dq = PythonOperator(task_id="validate", python_callable=task_data_quality)
    rec = PythonOperator(task_id="reconcile", python_callable=task_reconcile)

    backfill >> gold >> [dq, rec]
