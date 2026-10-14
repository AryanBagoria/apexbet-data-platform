"""DuckDB analytics engine.

Reads the Silver (and Gold) Parquet datasets from the object store into a local
DuckDB database, then builds/query the Gold marts. DuckDB is used as the local
analytical SQL engine: it can query Parquet directly and is fast on a
single node.

The engine exposes `platform_summary`, `data_quality_summary`,
`latency_summary`, `anomaly_summary`, and `reconciliation_summary` for the
serving API, plus a generic `query()` for ad-hoc SQL.
"""

from __future__ import annotations

import os
from typing import Any

from apexbet.common.logging import get_logger
from apexbet.common.storage import LocalObjectStore, ObjectStore

log = get_logger("apexbet.analytics")


class AnalyticsEngine:
    def __init__(self, duckdb_path: str, store: ObjectStore | None = None) -> None:
        self.path = duckdb_path
        self.store = store or LocalObjectStore(os.path.dirname(duckdb_path) or ".")
        self._conn: Any | None = None

    # -- connection ---------------------------------------------------------

    def connect(self) -> Any:
        import duckdb

        if self._conn is None:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            self._conn = duckdb.connect(self.path)
        return self._conn

    def available(self) -> bool:
        return os.path.exists(self.path)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- loading silver -----------------------------------------------------

    def silver_dir(self) -> str | None:
        """Return a local filesystem path to Silver parquet, if the store is local."""
        if isinstance(self.store, LocalObjectStore):
            p = os.path.join(self.store.root, "silver")
            return p if os.path.isdir(p) else None
        return None

    def load_silver(self, *, stage_table: str = "silver_events") -> int:
        """Load all silver parquet into a DuckDB table. Returns row count."""
        conn = self.connect()
        silver = self.silver_dir()
        rows = 0
        if silver:
            conn.execute(
                f"CREATE OR REPLACE TABLE {stage_table} AS "
                f"SELECT * FROM read_parquet('{silver}/**/*.parquet', "
                f"union_by_name=true)"
            )
            rows = conn.execute(f"SELECT COUNT(*) FROM {stage_table}").fetchone()[0]
        else:
            # Non-local store (S3/MinIO): copy objects to a temp dir, then glob.
            import shutil
            import tempfile

            files = self.store.list("silver/")
            if not files:
                return 0
            tmpdir = tempfile.mkdtemp(prefix="apexbet-silver-")
            try:
                for f in files:
                    local = os.path.join(tmpdir, f.key.replace("/", "__"))
                    with open(local, "wb") as fh:
                        fh.write(self.store.get_bytes(f.key))
                conn.execute(
                    f"CREATE OR REPLACE TABLE {stage_table} AS "
                    f"SELECT * FROM read_parquet('{tmpdir}/*.parquet', union_by_name=true)"
                )
                rows = int(conn.execute(f"SELECT COUNT(*) FROM {stage_table}").fetchone()[0])
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        return rows

    # -- gold build ---------------------------------------------------------

    def build_gold(self) -> dict[str, int]:
        """Build all Gold marts from the loaded Silver table."""
        conn = self.connect()
        counts: dict[str, int] = {}
        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_betting_activity AS
            SELECT
              event_date,
              sport_code AS sport_code,
              event_type,
              COUNT(*) AS event_count,
              SUM(CASE WHEN event_type='BetPlaced' THEN payload.stake_minor ELSE 0 END) AS staked_minor,
              SUM(CASE WHEN event_type='BetSettled' THEN payload.payout_minor ELSE 0 END) AS payout_minor
            FROM silver_events
            WHERE event_category='BETTING' OR event_category='SETTLEMENT'
            GROUP BY 1,2,3
            """
        )
        counts["gold_betting_activity"] = self._count("gold_betting_activity")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_bet_outcomes AS
            SELECT
              payload.result AS result,
              COUNT(*) AS bet_count,
              SUM(payload.settled_stake_minor) AS settled_stake_minor,
              SUM(payload.payout_minor) AS payout_minor,
              SUM(payload.payout_minor) - SUM(payload.settled_stake_minor) AS gross_margin_minor
            FROM silver_events
            WHERE event_type='BetSettled'
            GROUP BY 1
            """
        )
        counts["gold_bet_outcomes"] = self._count("gold_bet_outcomes")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_event_volume_by_minute AS
            SELECT
              substr(event_timestamp,1,16) AS event_minute,
              event_type,
              COUNT(*) AS event_count
            FROM silver_events
            GROUP BY 1,2
            ORDER BY 1
            """
        )
        counts["gold_event_volume_by_minute"] = self._count("gold_event_volume_by_minute")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_latency_distribution AS
            SELECT
              event_type,
              COUNT(*) AS n,
              quantile_cont(processing_latency_ms, 0.50) AS p50_ms,
              quantile_cont(processing_latency_ms, 0.95) AS p95_ms,
              quantile_cont(processing_latency_ms, 0.99) AS p99_ms,
              MAX(processing_latency_ms) AS max_ms
            FROM silver_events
            WHERE processing_latency_ms IS NOT NULL
            GROUP BY 1
            """
        )
        counts["gold_latency_distribution"] = self._count("gold_latency_distribution")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_financial_ledger AS
            SELECT
              event_type,
              payload.currency AS currency,
              COUNT(*) AS txn_count,
              SUM(COALESCE(payload.amount_minor, payload.payout_minor, payload.stake_minor)) AS total_minor
            FROM silver_events
            WHERE event_category IN ('PAYMENT','SETTLEMENT')
            GROUP BY 1,2
            """
        )
        counts["gold_financial_ledger"] = self._count("gold_financial_ledger")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_responsible_gambling AS
            SELECT
              event_date,
              COUNT(*) AS limit_change_events,
              COUNT(DISTINCT account_id) AS accounts_with_limits
            FROM silver_events
            WHERE event_type='ResponsibleGamblingLimitChanged'
            GROUP BY 1
            """
        )
        counts["gold_responsible_gambling"] = self._count("gold_responsible_gambling")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_casino_activity AS
            SELECT
              event_date,
              payload.game_type AS game_type,
              COUNT(*) AS session_events,
              SUM(CASE WHEN event_type='GameSessionEnded' THEN payload.total_wagered_minor ELSE 0 END) AS wagered_minor,
              SUM(CASE WHEN event_type='GameSessionEnded' THEN payload.total_won_minor ELSE 0 END) AS won_minor
            FROM silver_events
            WHERE event_category='CASINO'
            GROUP BY 1,2
            """
        )
        counts["gold_casino_activity"] = self._count("gold_casino_activity")

        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_data_completeness AS
            SELECT
              event_date,
              COUNT(*) AS events,
              COUNT(DISTINCT account_id) AS accounts,
              SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_events,
              SUM(COALESCE(processing_latency_ms,0)) AS total_latency_ms
            FROM silver_events
            GROUP BY 1
            """
        )
        counts["gold_data_completeness"] = self._count("gold_data_completeness")

        # A simple event-counts fact used by reconciliation and quality checks.
        conn.execute(
            """
            CREATE OR REPLACE TABLE gold_event_counts AS
            SELECT event_type, COUNT(*) AS event_count,
                   COUNT(DISTINCT event_id) AS distinct_event_ids
            FROM silver_events GROUP BY 1
            """
        )
        counts["gold_event_counts"] = self._count("gold_event_counts")

        log.info("gold.built", marts=len(counts))
        return counts

    def _count(self, table: str) -> int:
        conn = self.connect()
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def query(self, sql: str) -> list[dict[str, Any]]:
        conn = self.connect()
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]

    # -- summaries for the API ---------------------------------------------

    def platform_summary(self) -> dict[str, Any]:
        self.connect()
        out: dict[str, Any] = {}
        try:
            out["silver_events"] = self._count("silver_events")
        except Exception:
            out["silver_events"] = 0
        for table in ("gold_event_counts", "gold_bet_outcomes", "gold_data_completeness"):
            try:
                out[table] = self.query(f"SELECT * FROM {table} LIMIT 50")
            except Exception:
                out[table] = []
        return out

    def data_quality_summary(self) -> dict[str, Any]:
        try:
            return {
                "quality_results": self.query(
                    "SELECT * FROM dq_results ORDER BY failure_count DESC LIMIT 100"
                )
            }
        except Exception:
            return {"quality_results": [], "detail": "dq_results table not present"}

    def latency_summary(self) -> dict[str, Any]:
        try:
            return {"latency": self.query("SELECT * FROM gold_latency_distribution")}
        except Exception:
            return {"latency": []}

    def anomaly_summary(self) -> dict[str, Any]:
        try:
            return {
                "anomalies": self.query(
                    "SELECT * FROM anomaly_alerts ORDER BY detected_at DESC LIMIT 100"
                )
            }
        except Exception:
            return {"anomalies": [], "detail": "anomaly_alerts table not present"}

    def reconciliation_summary(self) -> dict[str, Any]:
        try:
            return {
                "reconciliation": self.query(
                    "SELECT * FROM reconciliation_results ORDER BY severity, rule_id"
                )
            }
        except Exception:
            return {"reconciliation": [], "detail": "reconciliation_results not present"}
