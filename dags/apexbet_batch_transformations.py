"""Batch transformation DAG: Bronze -> Silver -> Gold, with quality + reconcile.

Idempotent: each task recomputes deterministic outputs and overwrites the same
objects. Safe to re-run for backfills. Scheduled hourly; the streaming consumer
already handles near-real-time, so this DAG exists for batch rebuild, backfill,
and periodic integrity checks (no duplicate streaming work).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("APEXBET_HOME", "/opt/apexbet/src"))
sys.path.insert(0, "/opt/airflow/dags")  # dags/common.py

from common import (
    task_anomaly_eval,
    task_build_gold,
    task_build_silver,
    task_data_quality,
    task_freshness,
    task_reconcile,
)

try:  # reuse the shared default args when importable
    from common import default_args
except Exception:  # pragma: no cover
    from datetime import timedelta

    def default_args(owner: str = "apexbet") -> dict:
        return {"owner": owner, "retries": 2, "retry_delay": timedelta(minutes=1)}


with DAG(
    dag_id="apexbet_batch_transformations",
    description="Hourly Bronze->Silver->Gold rebuild with quality, reconciliation and anomaly eval.",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args(),
    tags=["apexbet", "batch", "medallion"],
) as dag:
    silver = PythonOperator(task_id="build_silver", python_callable=task_build_silver)
    gold = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
    dq = PythonOperator(task_id="data_quality", python_callable=task_data_quality)
    rec = PythonOperator(task_id="reconcile", python_callable=task_reconcile)
    fresh = PythonOperator(task_id="freshness", python_callable=task_freshness)
    anom = PythonOperator(task_id="anomaly_eval", python_callable=task_anomaly_eval)

    silver >> gold >> [dq, rec, fresh]
    gold >> anom
