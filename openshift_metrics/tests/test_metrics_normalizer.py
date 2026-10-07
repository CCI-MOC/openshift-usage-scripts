"""Tests for metrics_normalizer module."""

import pytest

from openshift_metrics.metrics_normalizer import MetricsNormalizer


class TestMetricsNormalizer:
    def test_normalize_standalone_unchanged(self):
        metrics = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 10]],
            }
        ]
        result = MetricsNormalizer.normalize(metrics, "standalone")
        assert result == metrics
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[0]["metric"]["namespace"] == "ns1"

    def test_normalize_hypershift_renames_keys(self):
        metrics = [
            {
                "metric": {"exported_pod": "pod1", "exported_namespace": "ns1"},
                "values": [[0, 10]],
            }
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[0]["metric"]["namespace"] == "ns1"
        assert "exported_pod" not in result[0]["metric"]
        assert "exported_namespace" not in result[0]["metric"]

    def test_normalize_hypershift_with_mixed_keys(self):
        metrics = [
            {
                "metric": {
                    "exported_pod": "pod1",
                    "exported_namespace": "ns1",
                    "node": "node1",
                    "resource": "cpu",
                },
                "values": [[0, 10]],
            }
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[0]["metric"]["namespace"] == "ns1"
        assert result[0]["metric"]["node"] == "node1"
        assert result[0]["metric"]["resource"] == "cpu"

    def test_normalize_hypershift_preserves_values(self):
        metrics = [
            {
                "metric": {"exported_pod": "pod1", "exported_namespace": "ns1"},
                "values": [[0, 10], [60, 20], [120, 30]],
            }
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["values"] == [[0, 10], [60, 20], [120, 30]]

    def test_normalize_multiple_metrics(self):
        metrics = [
            {
                "metric": {"exported_pod": "pod1", "exported_namespace": "ns1"},
                "values": [[0, 10]],
            },
            {
                "metric": {"exported_pod": "pod2", "exported_namespace": "ns1"},
                "values": [[0, 20]],
            },
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert len(result) == 2
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[1]["metric"]["pod"] == "pod2"

    def test_normalize_hypershift_with_missing_keys(self):
        metrics = [
            {
                "metric": {"exported_pod": "pod1", "node": "node1"},
                "values": [[0, 10]],
            }
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["metric"]["pod"] == "pod1"
        assert "exported_pod" not in result[0]["metric"]
        assert result[0]["metric"]["node"] == "node1"
        assert "namespace" not in result[0]["metric"]

    def test_normalize_unknown_cluster_type_raises(self):
        metrics = [
            {
                "metric": {"exported_pod": "pod1", "exported_namespace": "ns1"},
                "values": [[0, 10]],
            }
        ]
        with pytest.raises(ValueError):
            MetricsNormalizer.normalize(metrics, "unknown_type")
