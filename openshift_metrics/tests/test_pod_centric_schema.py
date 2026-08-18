from unittest import TestCase

from openshift_metrics.metrics_processor import MetricsProcessor


class TestCondenseMetricSeries(TestCase):
    def test_identical_values_collapse_to_one_segment(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 10], [900, 10], [1800, 10]],
            }
        ]
        segments = MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["cpu_request"], 10)
        self.assertEqual(segments[0]["duration"], 2700)
        self.assertEqual(segments[0]["pod"], "pod1")

    def test_value_change_starts_new_segment(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 10], [900, 10], [1800, 20], [2700, 20]],
            }
        ]
        segments = MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["cpu_request"], 10)
        self.assertEqual(segments[1]["cpu_request"], 20)

    def test_gap_larger_than_interval_starts_new_segment(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 5], [900, 5], [5400, 5]],
            }
        ]
        segments = MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["start"], 0)
        self.assertEqual(segments[1]["start"], 5400)

    def test_empty_series(self):
        self.assertEqual(MetricsProcessor.condense_metric_series([], 900, "cpu_request"), [])


class TestBuildNamespacesDict(TestCase):
    def test_cpu_and_memory_group_under_one_pod(self):
        cpu_segs = [
            {
                "start": 0,
                "duration": 900,
                "cpu_request": 4,
                "pod": "p1",
                "namespace": "ns1",
                "node": "n1",
            }
        ]
        mem_segs = [
            {
                "start": 0,
                "duration": 900,
                "memory_request": 8,
                "pod": "p1",
                "namespace": "ns1",
                "node": "n1",
            }
        ]
        namespaces = MetricsProcessor.build_namespaces_dict(cpu_segs, mem_segs)
        self.assertEqual(list(namespaces["ns1"].keys()), ["p1"])
        self.assertEqual(len(namespaces["ns1"]["p1"]["segments"]), 2)

    def test_gpu_label_becomes_gpu_type(self):
        gpu_segs = [
            {
                "start": 0,
                "duration": 900,
                "gpu_request": 1,
                "pod": "gpu-pod",
                "namespace": "ns1",
                "node": "wrk-1",
                "resource": "nvidia.com/gpu",
                "label_nvidia_com_gpu_product": "NVIDIA-H100-80GB",
            }
        ]
        namespaces = MetricsProcessor.build_namespaces_dict(gpu_segs)
        seg = namespaces["ns1"]["gpu-pod"]["segments"][0]
        self.assertEqual(seg["gpu_type"], "NVIDIA-H100-80GB")
        self.assertEqual(seg["gpu_resource"], "nvidia.com/gpu")
        self.assertNotIn("label_nvidia_com_gpu_product", seg)
        self.assertNotIn("pod", seg)
        self.assertNotIn("namespace", seg)

    def test_gpu_mapping_used_when_label_missing(self):
        gpu_segs = [
            {
                "start": 0,
                "duration": 900,
                "gpu_request": 1,
                "pod": "gpu-pod",
                "namespace": "ns1",
                "node": "wrk-gpu-1",
                "resource": "nvidia.com/gpu",
            }
        ]
        namespaces = MetricsProcessor.build_namespaces_dict(
            gpu_segs, gpu_mapping={"wrk-gpu-1": "NVIDIA-A100-SXM4-40GB"}
        )
        self.assertEqual(
            namespaces["ns1"]["gpu-pod"]["segments"][0]["gpu_type"],
            "NVIDIA-A100-SXM4-40GB",
        )

    def test_gpu_label_wins_over_mapping(self):
        gpu_segs = [
            {
                "start": 0,
                "duration": 900,
                "gpu_request": 1,
                "pod": "gpu-pod",
                "namespace": "ns1",
                "node": "wrk-gpu-1",
                "resource": "nvidia.com/gpu",
                "label_nvidia_com_gpu_product": "NVIDIA-H100-80GB",
            }
        ]
        namespaces = MetricsProcessor.build_namespaces_dict(
            gpu_segs, gpu_mapping={"wrk-gpu-1": "NVIDIA-A100-SXM4-40GB"}
        )
        self.assertEqual(
            namespaces["ns1"]["gpu-pod"]["segments"][0]["gpu_type"],
            "NVIDIA-H100-80GB",
        )


class TestLoadSegmentData(TestCase):
    def test_cpu_and_memory_at_same_start_are_merged(self):
        namespaces = {
            "ns1": {
                "pod1": {
                    "segments": [
                        {"start": 0, "duration": 900, "cpu_request": 2},
                        {"start": 0, "duration": 900, "memory_request": 4096},
                    ]
                }
            }
        }
        processor = MetricsProcessor()
        processor.load_segment_data(namespaces)
        entry = processor.merged_data["ns1"]["pod1"]["metrics"][0]
        self.assertEqual(entry["cpu_request"], 2)
        self.assertEqual(entry["memory_request"], 4096)

    def test_class_label_is_copied_to_pod(self):
        namespaces = {
            "ns1": {
                "pod1": {
                    "segments": [
                        {
                            "start": 0,
                            "duration": 900,
                            "cpu_request": 2,
                            "label_nerc_mghpcc_org_class": "cs-101",
                        }
                    ]
                }
            }
        }
        processor = MetricsProcessor()
        processor.load_segment_data(namespaces)
        self.assertEqual(
            processor.merged_data["ns1"]["pod1"]["label_nerc_mghpcc_org_class"],
            "cs-101",
        )

    def test_roundtrip_condense_then_load(self):
        cpu_segs = MetricsProcessor.condense_metric_series(
            [
                {
                    "metric": {"pod": "p1", "namespace": "ns1", "node": "n1"},
                    "values": [[0, 4], [900, 4]],
                }
            ],
            900,
            "cpu_request",
        )
        namespaces = MetricsProcessor.build_namespaces_dict(cpu_segs)
        processor = MetricsProcessor()
        processor.load_segment_data(namespaces)
        entry = processor.merged_data["ns1"]["p1"]["metrics"][0]
        self.assertEqual(entry["cpu_request"], 4)
        self.assertEqual(entry["duration"], 1800)


class TestStripEssentialLabels(TestCase):
    def test_drops_non_essential_labels(self):
        raw = [
            {
                "metric": {
                    "pod": "p1",
                    "namespace": "ns1",
                    "container": "main",
                    "job": "kube-state-metrics",
                },
                "values": [[0, "1"]],
            }
        ]
        stripped = MetricsProcessor.strip_to_essential_labels(raw)
        self.assertIn("pod", stripped[0]["metric"])
        self.assertNotIn("container", stripped[0]["metric"])
        self.assertNotIn("job", stripped[0]["metric"])


def test_producer_writes_legacy_and_namespaces(mocker, tmp_path):
    import json

    from openshift_metrics import openshift_prometheus_metrics as producer
    from openshift_metrics import utils

    cpu = [
        {
            "metric": {
                "pod": "cpu-pod",
                "namespace": "ns1",
                "resource": "cpu",
                "node": "n1",
            },
            "values": [[1000, "2"], [1900, "2"]],
        }
    ]
    memory = [
        {
            "metric": {
                "pod": "cpu-pod",
                "namespace": "ns1",
                "resource": "memory",
                "node": "n1",
            },
            "values": [[1000, "4096"], [1900, "4096"]],
        }
    ]

    def query(metric, start, end):
        if metric == producer.CPU_REQUEST:
            return cpu
        if metric == producer.MEMORY_REQUEST:
            return memory
        raise utils.EmptyResultError()

    mock_client = mocker.Mock()
    mock_client.query_metric.side_effect = query
    mocker.patch(
        "openshift_metrics.openshift_prometheus_metrics.PrometheusClient",
        return_value=mock_client,
    )
    mocker.patch(
        "openshift_metrics.openshift_prometheus_metrics.OPENSHIFT_TOKEN", "fake-token"
    )
    mocker.patch(
        "openshift_metrics.openshift_prometheus_metrics.PROM_QUERY_INTERVAL_MINUTES",
        15,
    )

    output_file = tmp_path / "metrics.json"
    mocker.patch(
        "argparse.ArgumentParser.parse_args",
        return_value=mocker.Mock(
            openshift_url="https://example.invalid",
            report_start_date="2026-06-02",
            report_end_date="2026-06-02",
            upload_to_s3=False,
            output_file=str(output_file),
        ),
    )
    producer.main()
    legacy = json.loads(output_file.read_text())
    namespaces_file = tmp_path / "metrics-namespaces.json"
    namespaces_data = json.loads(namespaces_file.read_text())

    assert "cpu_metrics" in legacy
    assert "memory_metrics" in legacy
    assert "namespaces" not in legacy
    assert legacy["cpu_metrics"] == cpu

    assert "namespaces" in namespaces_data
    assert "cpu_metrics" not in namespaces_data
    assert "cpu-pod" in namespaces_data["namespaces"]["ns1"]
    segments = namespaces_data["namespaces"]["ns1"]["cpu-pod"]["segments"]
    assert any("cpu_request" in seg for seg in segments)
    assert any("memory_request" in seg for seg in segments)

