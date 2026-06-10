"""End-to-end integration test for openshift_prometheus_metrics.py.

Mocks the Thanos/Prometheus endpoint via PrometheusClient.
Covers the full flow (arg parsing -> independent queries for all metric types
-> MetricsProcessor enrichment/condense/build -> JSON output) with basic
structure/schema assertions.
Includes data covering all SU types (CPU + multiple GPU variants).
Does not duplicate unit-test coverage of fine details in MetricsProcessor.

Also verifies that the produced JSON can be fed directly into merge.py.
"""

import json
from decimal import Decimal
from unittest import mock

import pytest

from openshift_metrics import openshift_prometheus_metrics as main_script


@pytest.fixture
def mock_query_responses():
    """Sample responses covering CPU/Mem + GPU (multiple SU types) + labels."""
    cpu = [
        {
            "metric": {
                "pod": "cpu-pod",
                "namespace": "ai-performance-profiling",
                "resource": "cpu",
                "node": "wrk-0",
            },
            "values": [[1780358400, "2"], [1780359000, "2"]],
        }
    ]
    memory = [
        {
            "metric": {
                "pod": "mem-pod",
                "namespace": "ai-performance-profiling",
                "resource": "memory",
                "node": "wrk-0",
            },
            "values": [[1780358400, "4294967296"], [1780359000, "4294967296"]],
        }
    ]
    # GPU data covers multiple SU types (A100, H100 whole GPU + MIG)
    gpu = [
        {
            "metric": {
                "pod": "gpu-a100-pod",
                "namespace": "ai-performance-profiling",
                "resource": "nvidia.com/gpu",
                "node": "wrk-gpu-a100",
            },
            "values": [[1780358400, "1"]],
        },
        {
            "metric": {
                "pod": "gpu-h100-pod",
                "namespace": "ai-performance-profiling",
                "resource": "nvidia.com/gpu",
                "node": "wrk-gpu-h100",
            },
            "values": [[1780358400, "1"]],
        },
        {
            "metric": {
                "pod": "mig-pod",
                "namespace": "ai-performance-profiling",
                "resource": "nvidia.com/mig-1g.5gb",
                "node": "wrk-gpu-mig",
            },
            "values": [[1780358400, "1"]],
        },
    ]
    node_labels = [
        {
            "metric": {
                "node": "wrk-gpu-a100",
                "label_nvidia_com_gpu_product": "NVIDIA-A100-SXM4-40GB",
                "label_nvidia_com_gpu_machine": "Dell",
            },
            "values": [[1780358400, "1"]],
        },
        {
            "metric": {
                "node": "wrk-gpu-h100",
                "label_nvidia_com_gpu_product": "NVIDIA-H100-80GB",
                "label_nvidia_com_gpu_machine": "Dell",
            },
            "values": [[1780358400, "1"]],
        },
    ]
    pod_labels = [
        {
            "metric": {
                "pod": "cpu-pod",
                "label_nerc_mghpcc_org_class": "cpu",
            },
            "values": [[1780358400, "1"]],
        }
    ]
    return {
        "cpu_request": cpu,
        "memory_request": memory,
        "gpu_request": gpu,
        "kube_node_labels": node_labels,
        "kube_pod_labels": pod_labels,
    }


@mock.patch(
    "openshift_metrics.openshift_prometheus_metrics.OPENSHIFT_TOKEN", "fake-token"
)
@mock.patch(
    "openshift_metrics.openshift_prometheus_metrics.PROM_QUERY_INTERVAL_MINUTES", 15
)
@mock.patch("openshift_metrics.openshift_prometheus_metrics.PrometheusClient")
def test_metrics_end_to_end(mock_client_class, mock_query_responses, tmp_path):
    """Full start-to-finish run with mocked Thanos queries covering all SU types."""
    mock_client = mock_client_class.return_value
    # Order of calls in main(): cpu, pod_labels, memory, gpu, node_labels
    responses = [
        mock_query_responses["cpu_request"],
        mock_query_responses["kube_pod_labels"],
        mock_query_responses["memory_request"],
        mock_query_responses["gpu_request"],
        mock_query_responses["kube_node_labels"],
    ]
    mock_client.query_metric.side_effect = responses

    output_file = tmp_path / "metrics-test.json"
    test_url = "https://thanos-querier-openshift-monitoring.apps.shift.nerc.mghpcc.org"

    with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
        mock_parse.return_value = mock.Mock(
            openshift_url=test_url,
            report_start_date="2026-06-02",
            report_end_date="2026-06-02",
            upload_to_s3=False,
            output_file=str(output_file),
        )
        main_script.main()

    # Verify file written and basic schema
    assert output_file.exists()
    data = json.loads(output_file.read_text())

    # Top-level schema checks (new pod-centric format)
    assert data["start_date"] == "2026-06-02"
    assert data["end_date"] == "2026-06-02"
    assert data["interval_minutes"] == 15
    assert data["cluster_name"] == "ocp-prod"  # mapped from URL
    assert "namespaces" in data
    assert isinstance(data["namespaces"], dict)

    namespaces = data["namespaces"]
    # Should have pods from the mocked data
    assert "ai-performance-profiling" in namespaces
    ns_pods = namespaces["ai-performance-profiling"]
    assert "cpu-pod" in ns_pods or "mem-pod" in ns_pods or "gpu-a100-pod" in ns_pods

    # Spot-check that segments contain the expected request keys
    # (detailed condense/insert logic covered by unit tests)
    found_cpu = found_mem = found_gpu = False
    for pod_data in ns_pods.values():
        for seg in pod_data.get("segments", []):
            if "cpu_request" in seg:
                found_cpu = True
            if "memory_request" in seg:
                found_mem = True
            if "gpu_request" in seg:
                found_gpu = True
    assert found_cpu
    assert found_mem
    assert found_gpu

    # Verify client was instantiated and called the expected number of times
    mock_client_class.assert_called_once_with(test_url, "fake-token", 15)
    assert mock_client.query_metric.call_count == 5


# ---------------------------------------------------------------------------
# Merge pipeline integration (feed the output of the collection script into merge)
# ---------------------------------------------------------------------------


@mock.patch("openshift_metrics.merge.rates.load_from_url")
@mock.patch("openshift_metrics.merge.outages.load_from_url")
@mock.patch("openshift_metrics.merge.utils.write_metrics_by_namespace")
@mock.patch("openshift_metrics.merge.utils.write_metrics_by_classes")
@mock.patch("openshift_metrics.merge.utils.write_metrics_by_pod")
def test_collection_output_feeds_merge(
    mock_write_pod,
    mock_write_classes,
    mock_write_ns,
    mock_outages,
    mock_rates,
    tmp_path,
):
    """The JSON produced by openshift_prometheus_metrics can be consumed by merge.py."""
    # Minimal rates / outages return values
    mock_rates.return_value.get_value_at.return_value = Decimal("1.0")
    mock_outages.return_value.get_outages_during.return_value = []

    # Create a minimal valid metrics file (as would be produced by the collection script)
    metrics_file = tmp_path / "metrics-2026-06-02.json"
    metrics_file.write_text(
        json.dumps(
            {
                "cluster_name": "ocp-prod",
                "start_date": "2026-06-02",
                "end_date": "2026-06-02",
                "interval_minutes": 15,
                "namespaces": {
                    "ai-performance-profiling": {
                        "gpu-a100-pod": {
                            "segments": [
                                {
                                    "start": 0,
                                    "duration": 3600,
                                    "cpu_request": 24,
                                    "memory_request": 98304,
                                    "gpu_request": 1,
                                    "gpu_type": "NVIDIA-A100-SXM4-40GB",
                                    "gpu_resource": "nvidia.com/gpu",
                                    "node_model": "Dell",
                                }
                            ]
                        }
                    }
                },
            }
        )
    )

    # Simulate merge.main() argument parsing
    with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
        mock_parse.return_value = mock.Mock(
            files=[str(metrics_file)],
            invoice_file=str(tmp_path / "invoice.csv"),
            pod_report_file=str(tmp_path / "pod.csv"),
            class_invoice_file=str(tmp_path / "class.csv"),
            upload_to_s3=False,
            ignore_hours=[],
            use_nerc_rates=True,
            rate_cpu_su=None,
            rate_gpu_v100_su=None,
            rate_gpu_a100sxm4_su=None,
            rate_gpu_a100_su=None,
            rate_gpu_h100_su=None,
        )
        from openshift_metrics import merge as merge_module

        merge_module.main()

    # The merge pipeline should have called the three report writers
    assert mock_write_ns.called
    assert mock_write_classes.called
    assert mock_write_pod.called

    # Basic sanity: the processor received the GPU pod data (SU type coverage)
    # (we don't inspect the full invoice output here; unit tests cover that)
    call_args = mock_write_ns.call_args[1]
    condensed = call_args["condensed_metrics_dict"]
    assert "ai-performance-profiling" in condensed
    assert "gpu-a100-pod" in condensed["ai-performance-profiling"]
