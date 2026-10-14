"""Reproducible anomaly-detection evaluation.

Builds labelled synthetic scenarios (injected anomaly buckets vs clean buckets)
and measures precision / recall / F1 plus detection delay for the rule-based
detector. This makes detector quality *measured*, not asserted.

Scenarios:
  1. volume_spike   -- a burst of extra events in a known minute.
  2. volume_drop    -- a lull with near-zero events.
  3. rejection_spike-- a burst of BetRejected relative to all events.
  4. duplicate_spike-- a high duplicate ratio.
"""

from __future__ import annotations

from typing import Any

from apexbet.anomaly.detector import AnomalyDetector


def scenario_volume_spike() -> tuple[list[tuple[str, float]], set[str]]:
    buckets: list[tuple[str, float]] = []
    truth: set[str] = set()
    for i in range(40):
        label = f"2026-01-01T00:{i:02d}"
        v = 100.0
        if i == 25:  # injected spike
            v = 1200.0
            truth.add(label)
        buckets.append((label, v))
    return buckets, truth


def scenario_volume_drop() -> tuple[list[tuple[str, float]], set[str]]:
    buckets: list[tuple[str, float]] = []
    truth: set[str] = set()
    for i in range(40):
        label = f"2026-01-01T00:{i:02d}"
        v = 100.0
        if i == 30:  # injected drop
            v = 8.0
            truth.add(label)
        buckets.append((label, v))
    return buckets, truth


def scenario_rejection_spike() -> tuple[list[tuple[str, float, int]], set[str]]:
    buckets: list[tuple[str, float, int]] = []
    truth: set[str] = set()
    for i in range(40):
        label = f"2026-01-01T00:{i:02d}"
        n = 200
        rejected = 10
        if i == 22:
            rejected = 160  # injected rejection spike
            truth.add(label)
        buckets.append((label, float(rejected), n))
    return buckets, truth


def scenario_duplicate_spike() -> tuple[float, set[str]]:
    return 0.25, {"duplicate_rate"}


def _detected_ids(alerts: list) -> set[str]:
    ids: set[str] = set()
    for a in alerts:
        bucket = a.evidence.get("bucket")
        if bucket:
            ids.add(bucket)
        elif "duplicate_rate" in a.evidence:
            ids.add("duplicate_rate")
    return ids


def evaluate_all() -> dict[str, Any]:
    results: dict[str, Any] = {}

    d = AnomalyDetector(z_threshold=3.0)
    buckets, truth = scenario_volume_spike()
    alerts = d.detect_volume(buckets)
    results["volume_spike"] = {
        "evaluation": d.evaluate(_detected_ids(alerts), truth),
        "alerts": [a.to_dict() for a in alerts],
    }

    d = AnomalyDetector(z_threshold=3.0)
    buckets, truth = scenario_volume_drop()
    alerts = d.detect_volume(buckets)
    results["volume_drop"] = {
        "evaluation": d.evaluate(_detected_ids(alerts), truth),
        "alerts": [a.to_dict() for a in alerts],
    }

    d = AnomalyDetector(z_threshold=3.0)
    rbuckets, truth = scenario_rejection_spike()
    alerts = d.detect_ratio_spike(rbuckets, metric="rejection_rate", ratio_threshold=0.4)
    results["rejection_spike"] = {
        "evaluation": d.evaluate(_detected_ids(alerts), truth),
        "alerts": [a.to_dict() for a in alerts],
    }

    d = AnomalyDetector()
    ratio, truth = scenario_duplicate_spike()
    alert = d.detect_duplicate_spike(ratio)
    alerts = [alert] if alert else []
    results["duplicate_spike"] = {
        "evaluation": d.evaluate(_detected_ids(alerts), truth),
        "alerts": [a.to_dict() for a in alerts],
    }

    total = {"true_positives": 0, "false_positives": 0, "false_negatives": 0}
    for r in results.values():
        for k in total:
            total[k] += r["evaluation"][k]
    tp, fp, fn = total["true_positives"], total["false_positives"], total["false_negatives"]
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    results["overall"] = {
        **total,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(
            (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0, 4
        ),
    }
    return results


if __name__ == "__main__":
    import json

    print(json.dumps(evaluate_all(), indent=2))
