"""dbt model validation.

Proves the dbt SQL under ``sql/dbt/models`` actually compiles and runs against a
DuckDB built from the same schema the pipeline produces. This keeps the dbt path
honest without requiring the ``dbt-duckdb`` adapter in CI.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from apexbet.dbt_validate import _rewrite, validate_models

_MODELS = Path("sql/dbt/models")


def _build_minimal_duckdb(path: str) -> None:
    con = duckdb.connect(path)
    con.execute(
        """
        CREATE TABLE silver_events AS
        SELECT * FROM (VALUES
          ('e1','BetPlaced','1.2.0','2026-01-01T00:00:00+00:00','2026-01-01','BETTING',
           'acct_1',NULL,'SOCCER','APL', false, NULL, 12.0,
           {'bet_id':'b1','market_id':'m1','stake_minor':1000,'currency':'USD',
            'sport_code':'SOCCER'}),
          ('e2','BetAccepted','1.2.0','2026-01-01T00:00:01+00:00','2026-01-01','BETTING',
           'acct_1',NULL,'SOCCER','APL', false, NULL, 8.0,
           {'bet_id':'b1','accepted_stake_minor':1000,'currency':'USD'}),
          ('e3','BetSettled','1.2.0','2026-01-01T00:01:00+00:00','2026-01-01','SETTLEMENT',
           'acct_1',NULL,'SOCCER','APL', false, true, 9.0,
           {'bet_id':'b1','result':'WON','settled_stake_minor':1000,'payout_minor':2500,
            'currency':'USD'}),
          ('e4','DepositRecorded','1.2.0','2026-01-01T00:02:00+00:00','2026-01-01','PAYMENT',
           'acct_1',NULL,NULL,NULL, false, NULL, 7.0,
           {'amount_minor':5000,'currency':'USD'}),
          ('e5','GameSessionEnded','1.2.0','2026-01-01T00:03:00+00:00','2026-01-01','CASINO',
           'acct_1',NULL,NULL,NULL, false, NULL, 11.0,
           {'game_type':'SLOTS','total_wagered_minor':2000,'total_won_minor':1500,
            'currency':'USD'})
        ) AS t(event_id, event_type, schema_version, event_timestamp, event_date,
               event_category, account_id, session_id, sport_code, league_code, is_late,
               is_winning_settlement, processing_latency_ms, payload)
        """
    )
    con.execute(
        """
        CREATE TABLE dq_results AS
        SELECT * FROM (VALUES
          ('DQ001','bronze.events','critical', 5, 0, 'required fields', true)
        ) AS t(rule_id, dataset, severity, record_count, failure_count, description, passed)
        """
    )
    con.execute(
        """
        CREATE TABLE reconciliation_results AS
        SELECT * FROM (VALUES
          ('REC001','event count','info', 5, 5, 0, 'EXPLAINED_BY_DEDUP_OR_QUARANTINE')
        ) AS t(rule_id, description, severity, expected, actual, difference, resolution_status)
        """
    )
    con.execute("CREATE TABLE anomaly_alerts AS SELECT 1 AS x WHERE false")
    con.close()


class TestRewrite:
    def test_ref_and_source_rewritten(self):
        sql = "select * from {{ ref('foo') }} join {{ source('apexbet_raw','silver_events') }}"
        out = _rewrite(sql)
        assert "{{" not in out
        assert "from foo" in out
        assert "silver_events" in out

    def test_config_stripped(self):
        assert "config" not in _rewrite("{{ config(materialized='view') }} select 1")


class TestDbtModels:
    @pytest.fixture()
    def built_duckdb(self, tmp_path) -> str:
        path = str(tmp_path / "analytics.duckdb")
        _build_minimal_duckdb(path)
        return path

    def test_all_models_compile_and_run(self, built_duckdb):
        report = validate_models(built_duckdb, root=str(_MODELS))
        failures = {k: v for k, v in report["models"].items() if not v["ok"]}
        assert report["ok"], f"dbt models failed: {failures}"

    def test_betting_performance_has_valid_numbers(self, built_duckdb):
        report = validate_models(built_duckdb, root=str(_MODELS))
        assert report["models"]["marts/mart_betting_performance.sql"]["rows"] == 1

    def test_validator_reports_missing_model(self, built_duckdb, tmp_path):
        # A model dir with a missing required file must report ok=False.
        empty = tmp_path / "empty-models"
        empty.mkdir()
        report = validate_models(built_duckdb, root=str(empty))
        assert not report["ok"]
