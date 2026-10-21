"""Tests for metrics_normalizer module."""

import pytest

from openshift_metrics.metrics_normalizer import MetricsNormalizer


def hypershift_metric(exported_pod="pod1", exported_namespace="ns1", **overrides):
    """
    A kube_pod_resource_request series as returned by the management cluster's thanos.

    The labels mirror a real series from a hosted cluster: pod/namespace belong to the
    hosted control plane's kube-scheduler that exported the metric, while
    exported_pod/exported_namespace identify the workload we actually bill for.
    """
    labels = {
        "__name__": "kube_pod_resource_request",
        "_id": "cac066c0-9e64-4421-928b-93b75c4c4d41",
        "container": "kube-scheduler",
        "endpoint": "client",
        "exported_namespace": exported_namespace,
        "exported_pod": exported_pod,
        "instance": "10.129.2.63:10259",
        "job": "kube-scheduler",
        "namespace": "clusters-oac-prod-workload0",
        "node": "node1",
        "pod": "kube-scheduler-b8ff6f5bd-xnndg",
        "priority": "0",
        "prometheus": "openshift-user-workload-monitoring/user-workload",
        "resource": "cpu",
        "scheduler": "default-scheduler",
        "service": "kube-scheduler",
        "unit": "cores",
    }
    labels.update(overrides)
    return {"metric": labels, "values": [[1792605600, "0.01"]]}


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

    def test_normalize_hypershift_replaces_scraper_pod_and_namespace(self):
        metrics = [hypershift_metric(exported_pod="pod1", exported_namespace="ns1")]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[0]["metric"]["namespace"] == "ns1"
        assert "exported_pod" not in result[0]["metric"]
        assert "exported_namespace" not in result[0]["metric"]

    def test_normalize_hypershift_preserves_other_labels(self):
        metrics = [hypershift_metric(node="node1", resource="memory")]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert result[0]["metric"]["node"] == "node1"
        assert result[0]["metric"]["resource"] == "memory"
        assert result[0]["metric"]["_id"] == "cac066c0-9e64-4421-928b-93b75c4c4d41"

    def test_normalize_hypershift_preserves_values(self):
        metric = hypershift_metric()
        metric["values"] = [[0, "10"], [60, "20"], [120, "30"]]
        result = MetricsNormalizer.normalize([metric], "hypershift")
        assert result[0]["values"] == [[0, "10"], [60, "20"], [120, "30"]]

    def test_normalize_multiple_metrics(self):
        # Every series is exported by the same scheduler pod, so they only differ
        # by exported_pod/exported_namespace.
        metrics = [
            hypershift_metric(exported_pod="pod1", exported_namespace="ns1"),
            hypershift_metric(exported_pod="pod2", exported_namespace="ns2"),
        ]
        result = MetricsNormalizer.normalize(metrics, "hypershift")
        assert len(result) == 2
        assert result[0]["metric"]["pod"] == "pod1"
        assert result[0]["metric"]["namespace"] == "ns1"
        assert result[1]["metric"]["pod"] == "pod2"
        assert result[1]["metric"]["namespace"] == "ns2"

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
        metrics = [hypershift_metric()]
        with pytest.raises(ValueError):
            MetricsNormalizer.normalize(metrics, "unknown_type")
