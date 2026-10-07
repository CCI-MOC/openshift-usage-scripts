"""Tests for openshift_prometheus_metrics module."""

import json
import sys

import pytest

from openshift_metrics import openshift_prometheus_metrics as opm


CPU_METRICS = [
    {
        "metric": {"pod": "pod1", "namespace": "ns1", "node": "node1"},
        "values": [[0, 1]],
    }
]
HYPERSHIFT_CPU_METRICS = [
    {
        "metric": {
            "exported_pod": "pod1",
            "exported_namespace": "ns1",
            "node": "node1",
        },
        "values": [[0, 1]],
    }
]
POD_LABELS = [
    {
        "metric": {
            "pod": "pod1",
            "namespace": "ns1",
            "label_nerc_mghpcc_org_class": "cls",
        },
        "values": [[0, 1]],
    }
]
NODE_LABELS = [
    {
        "metric": {"node": "node1", "label_nvidia_com_gpu_product": "gpu-a"},
        "values": [[0, 1]],
    }
]


class TestValidateConfig:
    @pytest.fixture(autouse=True)
    def valid_config(self, monkeypatch):
        monkeypatch.setattr(opm, "CLUSTER_TYPE", "standalone")
        monkeypatch.setattr(opm, "OPENSHIFT_CLUSTER_ID", "abc-123")
        monkeypatch.setattr(opm, "OPENSHIFT_TOKEN", "token")

    def test_valid_config(self):
        opm.validate_config()

    def test_invalid_cluster_type(self, monkeypatch):
        monkeypatch.setattr(opm, "CLUSTER_TYPE", "bogus")
        with pytest.raises(SystemExit):
            opm.validate_config()

    def test_missing_cluster_id(self, monkeypatch):
        monkeypatch.setattr(opm, "OPENSHIFT_CLUSTER_ID", None)
        with pytest.raises(SystemExit):
            opm.validate_config()

    def test_missing_token(self, monkeypatch):
        monkeypatch.setattr(opm, "OPENSHIFT_TOKEN", None)
        with pytest.raises(SystemExit):
            opm.validate_config()


class TestMain:
    @pytest.fixture
    def run_main(self, monkeypatch, mocker, tmp_path):
        """Runs main() with a mocked prometheus client and returns the written metrics."""

        def _run(cluster_type, responses):
            output_file = tmp_path / "metrics.json"

            monkeypatch.setattr(opm, "CLUSTER_TYPE", cluster_type)
            monkeypatch.setattr(opm, "OPENSHIFT_CLUSTER_ID", "abc-123")
            monkeypatch.setattr(opm, "OPENSHIFT_TOKEN", "token")
            monkeypatch.setattr(opm, "OPENSHIFT_CLUSTER_NAME", "ocp-prod")
            monkeypatch.setattr(
                sys,
                "argv",
                [
                    "prog",
                    "--openshift-url",
                    "https://prometheus.example.com",
                    "--report-start-date",
                    "2026-10-01",
                    "--report-end-date",
                    "2026-10-01",
                    "--output-file",
                    str(output_file),
                ],
            )

            def query_metric(query, start, end):
                for prefix, result in responses.items():
                    if query.startswith(prefix):
                        return json.loads(json.dumps(result))
                raise opm.utils.EmptyResultError()

            client = mocker.patch.object(opm, "PrometheusClient").return_value
            client.query_metric.side_effect = query_metric

            opm.main()
            queries = [c.args[0] for c in client.query_metric.call_args_list]
            return json.loads(output_file.read_text()), queries

        return _run

    def test_standalone_queries_labels(self, run_main):
        metrics, queries = run_main(
            "standalone",
            {
                'kube_pod_resource_request{resource="cpu"': CPU_METRICS,
                'kube_pod_resource_request{resource="memory"': CPU_METRICS,
                'kube_pod_resource_request{resource=~"nvidia': CPU_METRICS,
                "kube_pod_labels": POD_LABELS,
                "kube_node_labels": NODE_LABELS,
            },
        )
        assert metrics["cluster_id"] == "abc-123"
        assert metrics["cluster_name"] == "ocp-prod"
        assert any(q.startswith("kube_pod_labels") for q in queries)
        assert any(q.startswith("kube_node_labels") for q in queries)
        assert (
            metrics["cpu_metrics"][0]["metric"]["label_nerc_mghpcc_org_class"] == "cls"
        )
        assert (
            metrics["gpu_metrics"][0]["metric"]["label_nvidia_com_gpu_product"]
            == "gpu-a"
        )

    def test_hypershift_skips_labels(self, run_main):
        metrics, queries = run_main(
            "hypershift",
            {
                'kube_pod_resource_request{resource="cpu"': HYPERSHIFT_CPU_METRICS,
                'kube_pod_resource_request{resource="memory"': HYPERSHIFT_CPU_METRICS,
                'kube_pod_resource_request{resource=~"nvidia': HYPERSHIFT_CPU_METRICS,
            },
        )
        assert metrics["cluster_name"] == "ocp-prod"
        assert not any(q.startswith("kube_pod_labels") for q in queries)
        assert not any(q.startswith("kube_node_labels") for q in queries)
        assert all('_id="abc-123"' in q for q in queries)
        for key in ("cpu_metrics", "memory_metrics", "gpu_metrics"):
            labels = metrics[key][0]["metric"]
            assert labels["pod"] == "pod1"
            assert labels["namespace"] == "ns1"
            assert "exported_pod" not in labels

    def test_no_gpu_metrics(self, run_main):
        metrics, _ = run_main(
            "hypershift",
            {
                'kube_pod_resource_request{resource="cpu"': HYPERSHIFT_CPU_METRICS,
                'kube_pod_resource_request{resource="memory"': HYPERSHIFT_CPU_METRICS,
            },
        )
        assert "gpu_metrics" not in metrics
