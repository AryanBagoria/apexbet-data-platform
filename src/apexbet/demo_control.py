"""Safe demo controls for the dashboard.

These endpoints let a reviewer drive the synthetic pipeline from the UI. They are
deliberately constrained:

* **Bounded workloads.** Every generation count is clamped to a hard maximum
  (``MAX_EVENTS``) so the UI cannot accidentally generate unbounded data.
* **No shell, no arbitrary code.** The only actions are the ones defined here;
  they call the same in-process pipeline functions the CLI uses.
* **Local, synthetic only.** Nothing here touches a real broker/account.
* **Non-blocking.** A demo job runs on a background thread; the UI polls its
  status. Jobs are serialised with a lock so two clicks cannot overlap.
"""

from __future__ import annotations

import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any

from apexbet.common.logging import get_logger
from apexbet.common.timeutil import to_iso, utcnow

log = get_logger("apexbet.demo_control")

MAX_EVENTS = 50_000
MIN_EVENTS = 100
ALLOWED_JOBS = {
    "generate_small",
    "generate_medium",
    "inject_duplicates",
    "inject_invalid",
    "traffic_spike",
    "settlement_discrepancy",
    "full_pipeline",
}


@dataclass
class DemoJob:
    job_id: str
    kind: str
    params: dict[str, Any]
    status: str = "queued"  # queued | running | succeeded | failed
    started_at: str | None = None
    finished_at: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    log: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "kind": self.kind,
            "params": self.params,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
            "error": self.error,
            "log": self.log[-50:],
        }


class DemoController:
    """Runs bounded synthetic demo jobs on a background thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, DemoJob] = {}
        self._current: str | None = None

    def get(self, job_id: str) -> DemoJob | None:
        return self._jobs.get(job_id)

    def latest(self) -> DemoJob | None:
        if not self._jobs:
            return None
        return max(self._jobs.values(), key=lambda j: j.started_at or "")

    def is_busy(self) -> bool:
        return self._current is not None

    def submit(self, kind: str, params: dict[str, Any] | None = None) -> DemoJob:
        if kind not in ALLOWED_JOBS:
            raise ValueError(f"unknown demo job '{kind}'")
        with self._lock:
            if self._current is not None:
                raise RuntimeError("a demo job is already running")
            job = DemoJob(
                job_id=f"job_{uuid.uuid4().hex[:10]}",
                kind=kind,
                params=self._clean_params(params or {}),
            )
            self._jobs[job.job_id] = job
            self._current = job.job_id
        thread = threading.Thread(target=self._run, args=(job,), daemon=True)
        thread.start()
        return job

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        count = params.get("count")
        if count is not None:
            try:
                c = int(count)
            except (TypeError, ValueError):
                c = 3000
            params["count"] = max(MIN_EVENTS, min(c, MAX_EVENTS))
        seed = params.get("seed")
        if seed is not None:
            try:
                params["seed"] = int(seed)
            except (TypeError, ValueError):
                params.pop("seed", None)
        return params

    def _run(self, job: DemoJob) -> None:
        job.status = "running"
        job.started_at = to_iso(utcnow())
        job.log.append(f"started {job.kind} with {job.params}")
        try:
            result = self._dispatch(job.kind, job.params, job)
            job.result = result
            job.status = "succeeded"
            job.log.append("completed successfully")
        except Exception as exc:  # pragma: no cover - surfaced to the UI
            job.status = "failed"
            job.error = str(exc)
            job.log.append(f"failed: {exc}")
            job.log.append(traceback.format_exc().splitlines()[-1])
            log.error("demo.job.failed", kind=job.kind, error=str(exc))
        finally:
            job.finished_at = to_iso(utcnow())
            with self._lock:
                self._current = None

    def _dispatch(self, kind: str, params: dict[str, Any], job: DemoJob) -> dict[str, Any]:
        from apexbet.generator import AnomalyInjection
        from apexbet.orchestration import run_pipeline

        count = int(params.get("count", 3000))
        seed = int(params.get("seed", 1337))

        if kind == "generate_small":
            return run_pipeline(
                count=min(count, 3000), seed=seed, anomalies=AnomalyInjection.clean(), enrich=True
            )
        if kind == "generate_medium":
            return run_pipeline(
                count=max(count, 10_000), seed=seed, anomalies=AnomalyInjection.demo(), enrich=True
            )
        if kind == "full_pipeline":
            return run_pipeline(
                count=count, seed=seed, anomalies=AnomalyInjection.demo(), enrich=True
            )
        if kind == "inject_duplicates":
            inj = AnomalyInjection(duplicate_rate=0.25)
            return run_pipeline(count=count, seed=seed, anomalies=inj, enrich=False)
        if kind == "inject_invalid":
            inj = AnomalyInjection(
                invalid_amount_rate=0.20, missing_reference_rate=0.20, schema_drift_rate=0.10
            )
            return run_pipeline(count=count, seed=seed, anomalies=inj, enrich=False)
        if kind == "traffic_spike":
            inj = AnomalyInjection(traffic_spike=True, spike_multiplier=12.0)
            return run_pipeline(count=count, seed=seed, anomalies=inj, enrich=False)
        if kind == "settlement_discrepancy":
            inj = AnomalyInjection(settlement_discrepancy_rate=0.20)
            return run_pipeline(count=count, seed=seed, anomalies=inj, enrich=False)
        raise ValueError(f"unhandled kind {kind}")
