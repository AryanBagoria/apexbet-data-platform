"""Anomaly detection tests + evaluation metrics."""

from __future__ import annotations

from apexbet.anomaly.detector import AnomalyDetector
from apexbet.anomaly.evaluation import evaluate_all


class TestVolumeDetection:
    def test_spike_detected(self):
        series = [(f"t{i}", 100.0) for i in range(20)] + [("t20", 1000.0)]
        alerts = AnomalyDetector().detect_volume(series)
        assert any(a.detector == "volume_spike" for a in alerts)

    def test_drop_detected(self):
        series = [(f"t{i}", 100.0) for i in range(20)] + [("t20", 5.0)]
        alerts = AnomalyDetector().detect_volume(series)
        assert any(a.detector == "volume_drop" for a in alerts)

    def test_no_false_positive_on_flat(self):
        series = [(f"t{i}", 100.0) for i in range(30)]
        alerts = AnomalyDetector().detect_volume(series)
        assert alerts == []


class TestRatioDetection:
    def test_rejection_spike(self):
        series = [(f"t{i}", 10.0, 200) for i in range(20)] + [("t20", 160.0, 200)]
        alerts = AnomalyDetector().detect_ratio_spike(series, metric="rejection_rate")
        assert any("rejection_rate" in a.detector for a in alerts)

    def test_no_spike_flat(self):
        series = [(f"t{i}", 10.0, 200) for i in range(20)]
        assert AnomalyDetector().detect_ratio_spike(series, metric="rate") == []


class TestThresholdDetection:
    def test_duplicate_spike(self):
        assert AnomalyDetector().detect_duplicate_spike(0.01) is None
        assert AnomalyDetector().detect_duplicate_spike(0.2) is not None

    def test_suspicious_transactions(self):
        deposits = [{"account_id": "acct_1", "payload": {"amount_minor": 49900}} for _ in range(6)]
        alerts = AnomalyDetector().detect_suspicious_transactions(deposits)
        assert len(alerts) == 1
        assert alerts[0].severity == "warning"

    def test_no_suspicious_below_count(self):
        deposits = [{"account_id": "a", "payload": {"amount_minor": 49900}} for _ in range(3)]
        assert AnomalyDetector().detect_suspicious_transactions(deposits) == []


class TestEvaluation:
    def test_precision_recall_perfect_on_scenarios(self):
        results = evaluate_all()
        assert results["overall"]["precision"] == 1.0
        assert results["overall"]["recall"] == 1.0

    def test_evaluate_math(self):
        d = AnomalyDetector()
        m = d.evaluate({"a", "b"}, {"b", "c"})
        assert m["true_positives"] == 1
        assert m["false_positives"] == 1
        assert m["false_negatives"] == 1
        assert m["precision"] == 0.5
        assert m["recall"] == 0.5


class TestIsolationForest:
    def test_flags_outlier(self):
        features = [[1.0, 1.0]] * 50 + [[50.0, 50.0]]
        labels = [f"p{i}" for i in range(51)]
        alerts = AnomalyDetector().isolation_forest(features, labels, contamination=0.05)
        flagged = {a.evidence["label"] for a in alerts}
        assert "p50" in flagged
