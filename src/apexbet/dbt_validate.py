"""Validate the dbt Gold models without the dbt runtime.

dbt-core's DuckDB adapter (``dbt-duckdb``) is an optional extra that is not a
hard dependency of the platform (the always-on Gold path is
:mod:`apexbet.analytics.duckdb_engine`). To keep the dbt SQL honest and
testable anyway, this module:

  1. reads every model under ``sql/dbt/models`` in dependency order,
  2. rewrites ``{{ ref('x') }}`` / ``{{ source('apexbet_raw','y') }}`` into real
     relation names,
  3. compiles + executes the resulting SQL against the local DuckDB file, and
  4. reports row counts, or the SQL error for any model that fails.

It is used by the CLI (``apexbet validate-dbt``) and by a unit test, so a
broken dbt model is caught in CI even when ``dbt-duckdb`` is absent.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_MODELS_ROOT = Path("sql/dbt/models")
# Dependency order (staging -> intermediate -> marts).
_MODEL_ORDER = [
    "staging/stg_silver_events.sql",
    "staging/stg_dq_results.sql",
    "staging/stg_reconciliation_results.sql",
    "intermediate/int_betting_lifecycle.sql",
    "intermediate/int_daily_financials.sql",
    "marts/mart_betting_performance.sql",
    "marts/mart_financial_daily.sql",
    "marts/mart_casino_activity.sql",
    "marts/mart_data_quality.sql",
    "marts/mart_reconciliation.sql",
]

_SOURCE_MAP = {
    "silver_events": "silver_events",
    "dq_results": "dq_results",
    "reconciliation_results": "reconciliation_results",
    "anomaly_alerts": "anomaly_alerts",
}

_REF_RE = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
_SOURCE_RE = re.compile(r"\{\{\s*source\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)\s*\}\}")
_CONFIG_RE = re.compile(r"\{\{\s*config\([^)]*\)\s*\}\}")


def _rewrite(sql: str) -> str:
    sql = _CONFIG_RE.sub("", sql)
    sql = _REF_RE.sub(lambda m: m.group(1), sql)
    sql = _SOURCE_RE.sub(lambda m: _SOURCE_MAP.get(m.group(1), m.group(1)), sql)
    return sql.strip().rstrip(";")


def validate_models(duckdb_path: str, root: str = str(_MODELS_ROOT)) -> dict[str, Any]:
    """Compile + run every dbt model; return per-model status."""
    import duckdb

    root_path = Path(root)
    results: dict[str, Any] = {"models": {}, "ok": True}
    con = duckdb.connect(duckdb_path)
    try:
        for rel in _MODEL_ORDER:
            path = root_path / rel
            if not path.exists():
                results["models"][rel] = {"ok": False, "error": "model file missing"}
                results["ok"] = False
                continue
            sql = _rewrite(path.read_text(encoding="utf-8"))
            view_name = Path(rel).stem
            try:
                con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS {sql}")
                row = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
                n = int(row[0]) if row else 0
                results["models"][rel] = {"ok": True, "rows": n}
            except Exception as exc:
                results["models"][rel] = {"ok": False, "error": str(exc)}
                results["ok"] = False
    finally:
        con.close()
    return results


if __name__ == "__main__":  # pragma: no cover
    import json
    import os

    path = os.environ.get("APEXBET_DUCKDB_PATH", "./lake/analytics.duckdb")
    print(json.dumps(validate_models(path), indent=2))
