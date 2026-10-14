"""Anomaly detection service.

Two layers:
  1. Deterministic statistical rules over time-series aggregates (traffic
     spikes/drops, rejection-rate spikes, latency spikes, dedup spikes,
     DQ degradation, reconciliation discrepancies, suspicious transaction
     patterns, enrichment failures, consumer-lag increases).
  2. An optional Isolation Forest baseline for multivariate point anomalies.

Every alert carries structured evidence. Scores are operational signals and are
NEVER presented as proof of fraud or used to make consequential decisions about
individual accounts.

Works with no LLM and no external service. The Isolation Forest is optional and
degrades to rules-only if scikit-learn is unavailable.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from typing import Any

from apexbet.common.logging import get_logger
from apexbet.common.timeutil import to_iso, utcnow

log = get_logger("apexbet.anomaly")


@dataclass
class AnomalyAlert:
    alert_id: str
    detector: str
    severity: str  # info | warning | error | critical
    metric: str
    observed: float
    expected: float
    threshold: float
    explanation: str
    evidence: dict[str, Any] = field(default_factory=dict)
    detected_at: str = field(default_factory=lambda: to_iso(utcnow()))
    method: str = "rule"  # rule | isolation_forest

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _zscore(value: float, series: list[float]) -> float:
    if len(series) < 3:
        return 0.0
    mu = statistics.fmean(series)
    sd = statistics.pstdev(series)
    # Guard against a degenerate (near-constant) baseline producing a
    # meaningless astronomically large z-score. Use a relative floor instead.
    floor = max(1e-9, abs(mu) * 0.01)
    sd = max(sd, floor)
    return (value - mu) / sd


def _severity_for(z: float) -> str:
    az = abs(z)
    if az >= 5:
        return "critical"
    if az >= 4:
        return "error"
    if az >= 3:
        return "warning"
    return "info"


class AnomalyDetector:
    def __init__(self, *, z_threshold: float = 3.0, min_history: int = 5) -> None:
        self.z_threshold = z_threshold
        self.min_history = min_history
        self.alerts: list[AnomalyAlert] = []

    # -- rules --------------------------------------------------------------

    def detect_volume(
        self,
        series: list[tuple[str, float]],
        *,
        metric: str = "event_volume",
        spike_ratio: float = 3.0,
        drop_ratio: float = 0.3,
    ) -> list[AnomalyAlert]:
        """Detect traffic spikes and unexpected reductions in an (bucket, value) series."""
        out: list[AnomalyAlert] = []
        values = [v for _, v in series]
        if len(values) < self.min_history:
            return out
        hist = values[: max(self.min_history, len(values) // 2)]
        baseline = statistics.fmean(hist) if hist else 0.0
        for i, (bucket, v) in enumerate(series):
            if i < self.min_history:
                continue
            z = _zscore(v, values[:i])
            if v >= baseline * spike_ratio and z >= self.z_threshold:
                out.append(
                    self._alert(
                        "volume_spike",
                        metric,
                        v,
                        baseline,
                        baseline * spike_ratio,
                        f"Event volume in {bucket} is {v / baseline:.1f}x the baseline "
                        f"(z={z:.1f}).",
                        {"bucket": bucket, "z": round(z, 2)},
                        _severity_for(z),
                    )
                )
            elif baseline > 0 and v <= baseline * drop_ratio and z <= -self.z_threshold:
                out.append(
                    self._alert(
                        "volume_drop",
                        metric,
                        v,
                        baseline,
                        baseline * drop_ratio,
                        f"Event volume in {bucket} dropped to {v / baseline:.2f}x baseline "
                        f"(z={z:.1f}).",
                        {"bucket": bucket, "z": round(z, 2)},
                        _severity_for(z),
                    )
                )
        self.alerts.extend(out)
        return out

    def detect_ratio_spike(
        self,
        series: list[tuple[str, float, int]],
        *,
        metric: str,
        ratio_threshold: float = 0.4,
        min_n: int = 10,
    ) -> list[AnomalyAlert]:
        """Detect a rate (numer/denom) spike, e.g. rejection rate, per bucket."""
        out: list[AnomalyAlert] = []
        ratios: list[float] = []
        for bucket, numer, denom in series:
            if denom < min_n:
                continue
            ratio = numer / denom
            if ratios:
                z = _zscore(ratio, ratios)
                if ratio >= ratio_threshold and z >= self.z_threshold:
                    out.append(
                        self._alert(
                            f"{metric}_spike",
                            metric,
                            round(ratio, 4),
                            round(statistics.fmean(ratios), 4),
                            ratio_threshold,
                            f"{metric} in {bucket} is {ratio:.0%} (baseline "
                            f"{statistics.fmean(ratios):.0%}, z={z:.1f}).",
                            {
                                "bucket": bucket,
                                "numerator": numer,
                                "denominator": denom,
                                "z": round(z, 2),
                            },
                            _severity_for(z),
                        )
                    )
            ratios.append(ratio)
        self.alerts.extend(out)
        return out

    def detect_threshold(
        self,
        value: float,
        *,
        metric: str,
        warn: float,
        crit: float,
        explanation: str,
        evidence: dict[str, Any] | None = None,
    ) -> AnomalyAlert | None:
        if value >= crit:
            sev = "critical"
        elif value >= warn:
            sev = "warning"
        else:
            return None
        a = self._alert(metric, metric, value, warn, crit, explanation, evidence or {}, sev)
        self.alerts.append(a)
        return a

    def detect_latency_spike(
        self,
        series: list[tuple[str, float]],
        *,
        metric: str = "p95_latency_ms",
        spike_ratio: float = 2.5,
    ) -> list[AnomalyAlert]:
        return self.detect_volume(series, metric=metric, spike_ratio=spike_ratio, drop_ratio=0.0)

    def detect_duplicate_spike(
        self, dup_ratio: float, *, warn: float = 0.02, crit: float = 0.10
    ) -> AnomalyAlert | None:
        return self.detect_threshold(
            dup_ratio,
            metric="duplicate_rate",
            warn=warn,
            crit=crit,
            explanation=f"Duplicate event ratio {dup_ratio:.2%} exceeds threshold; "
            f"indicates broker redelivery or producer retries.",
            evidence={"duplicate_rate": dup_ratio},
        )

    def detect_suspicious_transactions(
        self, deposits: list[dict[str, Any]], *, threshold_minor: int = 49_000, min_count: int = 5
    ) -> list[AnomalyAlert]:
        """Flag clusters of round 'just-under-limit' deposits (structuring pattern)."""
        near_limit = [
            d
            for d in deposits
            if int((d.get("payload") or {}).get("amount_minor", 0)) >= threshold_minor
        ]
        if len(near_limit) < min_count:
            return []
        by_account: dict[str, int] = {}
        for d in near_limit:
            aid = d.get("account_id") or "?"
            by_account[aid] = by_account.get(aid, 0) + 1
        out = []
        for aid, cnt in by_account.items():
            if cnt >= min_count:
                a = self._alert(
                    "suspicious_transaction_pattern",
                    "near_limit_deposits",
                    float(cnt),
                    float(min_count),
                    float(min_count),
                    f"Account {aid} made {cnt} deposits just under the reporting limit "
                    f"(>= {threshold_minor / 100:.2f}).",
                    {"account_id": aid, "count": cnt, "threshold_minor": threshold_minor},
                    "warning",
                )
                out.append(a)
        self.alerts.extend(out)
        return out

    def _alert(
        self,
        detector: str,
        metric: str,
        observed: float,
        expected: float,
        threshold: float,
        explanation: str,
        evidence: dict[str, Any],
        severity: str,
    ) -> AnomalyAlert:
        import uuid

        return AnomalyAlert(
            alert_id=f"alert_{uuid.uuid4().hex[:12]}",
            detector=detector,
            severity=severity,
            metric=metric,
            observed=observed,
            expected=expected,
            threshold=threshold,
            explanation=explanation,
            evidence=evidence,
            method="rule",
        )

    # -- isolation forest ---------------------------------------------------

    def isolation_forest(
        self, features: list[list[float]], labels: list[str], *, contamination: float = 0.05
    ) -> list[AnomalyAlert]:
        """Multivariate baseline. Returns alerts for points flagged as anomalies."""
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:  # pragma: no cover
            log.warning("isolation_forest.unavailable")
            return []
        if len(features) < 10:
            return []
        model = IsolationForest(contamination=contamination, random_state=1337)
        preds = model.fit_predict(features)
        scores = model.score_samples(features)
        out: list[AnomalyAlert] = []
        for label, pred, score in zip(labels, preds, scores, strict=False):
            if pred == -1:
                a = AnomalyAlert(
                    alert_id=f"alert_if_{abs(hash(label)) & 0xFFFFFFFF:08x}",
                    detector="isolation_forest",
                    severity="warning",
                    metric="multivariate_score",
                    observed=float(score),
                    expected=0.0,
                    threshold=float(statistics.fmean(scores)),
                    method="isolation_forest",
                    explanation=f"Point {label} flagged by Isolation Forest (score {score:.4f}).",
                    evidence={"label": label, "score": float(score)},
                )
                out.append(a)
        self.alerts.extend(out)
        return out

    # -- evaluation ---------------------------------------------------------

    def evaluate(self, detected_ids: set[str], ground_truth_ids: set[str]) -> dict[str, float]:
        """Precision/recall/F1 for reproducible synthetic scenarios.

        ``detected_ids`` and ``ground_truth_ids`` are opaque identifiers (e.g.
        bucket names) sharing one namespace.
        """
        tp = len(detected_ids & ground_truth_ids)
        fp = len(detected_ids - ground_truth_ids)
        fn = len(ground_truth_ids - detected_ids)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        return {
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
        }

    def summary(self) -> dict[str, Any]:
        by_sev: dict[str, int] = {}
        for a in self.alerts:
            by_sev[a.severity] = by_sev.get(a.severity, 0) + 1
        return {
            "total_alerts": len(self.alerts),
            "by_severity": by_sev,
            "alerts": [a.to_dict() for a in self.alerts],
        }
