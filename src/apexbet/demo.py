"""Reproducible end-to-end acceptance demonstration.

Runs the full platform flow with only synthetic data and no external service:

  1. start (uses the configured transport; memory works offline)
  2. generate synthetic sportsbook + casino events
  3. publish into the streaming system
  4. validate + process (Bronze)
  5. normalize/dedupe/enrich (Silver)
  6. run enrichment (rules by default)
  7. build Gold analytics
  8. run SQL analytics + financial reconciliation
  9. inject a controlled anomaly
 10. detect the anomaly and print explainable alerts
 11. report counts and verified reconciliation results

Designed to run in ~5 minutes after infrastructure is up; the default
`count=8000` runs in seconds on a laptop.
"""

from __future__ import annotations

import json
from typing import Any

from apexbet.common.logging import configure_logging, get_logger
from apexbet.config import get_settings
from apexbet.generator import AnomalyInjection
from apexbet.orchestration import run_pipeline

log = get_logger("apexbet.demo")


def run_demo(*, count: int = 8000, seed: int = 1337, console: Any = None) -> dict[str, Any]:
    configure_logging()

    def out(msg: str = "") -> None:
        if console is not None:
            console.print(msg)
        else:
            print(msg)

    settings = get_settings()
    out("[bold cyan]ApexBet end-to-end demo[/bold cyan]" if console else "ApexBet end-to-end demo")
    out(
        f"transport={settings.transport}  storage={settings.storage_backend}  "
        f"enrichment={settings.enrichment_mode}"
    )

    # Steps 1-8: full pipeline with the dirty-by-design demo anomaly profile.
    out(
        "\n[bold]1-8. Generate -> stream -> Bronze -> Silver -> Gold -> quality -> reconcile[/bold]"
        if console
        else "\n1-8. Generate -> stream -> Bronze -> Silver -> Gold -> quality -> reconcile"
    )
    report = run_pipeline(
        count=count,
        seed=seed,
        anomalies=AnomalyInjection.demo(),
        settings=settings,
    )
    pm = report["pipeline_metrics"]
    out(f"   generated/published: {count}")
    out(f"   consumed:            {pm['consumed']}")
    out(f"   bronze written:      {pm['bronze_written']}")
    out(
        f"   silver written:      {pm['silver_written']}  "
        f"(duplicates dropped: {pm['duplicates']}, quarantined: {pm['quarantined']}, "
        f"late: {pm['late_events']})"
    )
    out(f"   gold tables:         {len(report['gold_tables'])}")
    out(f"   throughput:          {pm['throughput_eps']} events/s")

    q = report["quality"]
    out("\n[bold]Data quality[/bold]" if console else "\nData quality")
    out(
        f"   rules run: {q['total_rules']}  failed: {q['failed_rules']}  "
        f"failures: {q['total_failures']}  by severity: {q['failed_by_severity']}"
    )

    rec = report["reconciliation"]
    out("\n[bold]Financial reconciliation[/bold]" if console else "\nFinancial reconciliation")
    out(f"   findings: {rec['total_findings']}  by severity: {rec['by_severity']}")
    for f in rec["findings"]:
        tag = "OK " if f["severity"] == "info" else "!! "
        out(
            f"   {tag}{f['rule_id']}: expected={f['expected']} actual={f['actual']} "
            f"({f['resolution_status']})"
        )

    # Step 9-10: anomaly injection + detection.
    out(
        "\n[bold]9-10. Inject controlled anomaly and detect it[/bold]"
        if console
        else "\n9-10. Inject controlled anomaly and detect it"
    )
    from apexbet.orchestration import inject_anomaly

    anomaly_report = inject_anomaly(count=max(2000, count // 3), settings=settings)
    an = anomaly_report["anomalies"]
    out(f"   alerts: {an['total_alerts']}  by severity: {an['by_severity']}")
    for a in an["alerts"][:6]:
        out(f"   [{a['severity']}] {a['detector']}: {a['explanation']}")

    # Step 11: report verification.
    out("\n[bold]Verification[/bold]" if console else "\nVerification")
    verified = (
        pm["consumed"] == count
        and pm["bronze_written"] == count
        and report["gold_tables"]
        and report["enrichment"].get("succeeded", 0) > 0
    )
    out(f"   all events consumed: {pm['consumed'] == count}")
    out(f"   bronze == source:    {pm['bronze_written'] == count}")
    out(f"   gold marts built:    {len(report['gold_tables'])}")
    out(f"   enrichment succeeded:{report['enrichment'].get('succeeded', 0)}")
    out(f"   anomaly alerts:      {an['total_alerts']}")
    out(
        f"\n[bold green]DEMO {'PASSED' if verified else 'INCOMPLETE'}[/bold green]"
        if console
        else f"\nDEMO {'PASSED' if verified else 'INCOMPLETE'}"
    )

    return {"report": report, "anomaly_report": anomaly_report, "verified": verified}


if __name__ == "__main__":
    result = run_demo()
    print(json.dumps({"verified": result["verified"]}, indent=2))
