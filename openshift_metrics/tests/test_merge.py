import pytest
from decimal import Decimal

from openshift_metrics import metrics_processor
from openshift_metrics.merge import (
    compare_dates,
    get_su_definitions,
    load_and_merge_metrics,
    load_metrics_metadata,
)


@pytest.mark.parametrize(
    "date1, date2, expected_result",
    [
        ("2025-01-18", "2025-01-20", True),
        ("2025-01-18", "2025-01-16", False),
        ("2025-01-18", "2025-01-18", False),
    ],
)
def test_compare_dates(date1, date2, expected_result):
    assert compare_dates(date1, date2) is expected_result


def test_get_su_definitions(mocker):
    mock_rates = {
        "vCPUs in GPUV100 SU": Decimal("20"),
        "RAM in GPUV100 SU": Decimal("8192"),
        "GPUs in GPUV100 SU": Decimal("1"),
        "vCPUs in CPU SU": Decimal("5"),
        "RAM in CPU SU": Decimal("1024"),
        "GPUs in CPU SU": Decimal("0"),
    }
    mock_rates_data = mocker.MagicMock()

    def mock_get_value_at(key, month, value_type):
        return mock_rates.get(key, Decimal("67"))

    mock_rates_data.get_value_at.side_effect = mock_get_value_at
    mocker.patch(
        "openshift_metrics.merge.rates.load_from_url", return_value=mock_rates_data
    )
    report_month = "2025-10"
    su_definitions = get_su_definitions(report_month)

    assert "OpenShift GPUV100" in su_definitions
    assert su_definitions["OpenShift GPUV100"]["vCPUs"] == Decimal("20")
    assert su_definitions["OpenShift GPUV100"]["RAM"] == Decimal("8192")
    assert su_definitions["OpenShift GPUV100"]["GPUs"] == Decimal("1")

    assert "OpenShift CPU" in su_definitions
    assert su_definitions["OpenShift CPU"]["vCPUs"] == Decimal("5")
    assert su_definitions["OpenShift CPU"]["RAM"] == Decimal("1024")
    assert su_definitions["OpenShift CPU"]["GPUs"] == Decimal("0")

    # This should get the default test value
    assert su_definitions["OpenShift GPUH100"]["GPUs"] == Decimal("67")


def test_load_and_merge_data(
    create_metrics_file, mock_metrics_file1, mock_metrics_file2
):
    """
    Test that we can load metrics from the 2 files and merge the metrics from those.

    Note that we already have tests that test the merging of the data, this mostly
    focuses on the loading part.
    """
    p1 = create_metrics_file(mock_metrics_file1, "file1.json")
    p2 = create_metrics_file(mock_metrics_file2, "file2.json")

    processor = load_and_merge_metrics(2, [p1, p2])

    pod1_metrics = processor.merged_data["namespace1"]["pod1"]["metrics"]

    # check values from file1.json are in the merged_data
    assert 60 in pod1_metrics  # 60 is the epoch time stamp
    assert pod1_metrics[60]["cpu_request"] == 15
    assert pod1_metrics[60]["memory_request"] == 15

    # check values from file2.json are in the merged_data
    assert 180 in pod1_metrics
    assert pod1_metrics[180]["cpu_request"] == 10
    assert pod1_metrics[180]["memory_request"] == 10


def test_load_metrics_metadata(
    create_metrics_file, mock_metrics_file1, mock_metrics_file2
):
    """Test we can load metadata from the metrics files."""

    p1 = create_metrics_file(mock_metrics_file1, "file1.json")
    p2 = create_metrics_file(mock_metrics_file2, "file2.json")

    metadata = load_metrics_metadata([p1, p2])
    assert metadata.cluster_name == "ocp-prod"
    assert metadata.report_start_date == "2025-09-20"
    assert metadata.report_end_date == "2025-09-21"
    assert metadata.interval_minutes == 15


class TestIngestFormat:
    """Tests for the new namespaces + segments ingest path."""

    def test_load_segment_data_basic(self, create_metrics_file):
        """Tests loading a simple namespaces file -> asserts merged_data is populated correctly."""
        data = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "pod1": {
                        "segments": [
                            {"start": 100, "duration": 900, "cpu_request": 2, "memory_request": 4}
                        ]
                    }
                }
            },
        }
        path = create_metrics_file(data, "newformat.json")
        processor = load_and_merge_metrics(15, [path])
        assert "ns1" in processor.merged_data
        assert "pod1" in processor.merged_data["ns1"]
        assert 100 in processor.merged_data["ns1"]["pod1"]["metrics"]

    def test_load_segment_data_with_gpu_fields(self, create_metrics_file):
        """Tests GPU fields in segments -> asserts gpu_type, gpu_resource, node_hostname are preserved."""
        data = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01" ,
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "gpu-pod": {
                        "segments": [
                            {
                                "start": 200,
                                "duration": 1800,
                                "gpu_request": 1,
                                "gpu_type": "NVIDIA-A100",
                                "gpu_resource": "nvidia.com/gpu",
                                "node_hostname": "gpu-node-1",
                            }
                        ]
                    }
                }
            },
        }
        path = create_metrics_file(data, "gpu.json")
        processor = load_and_merge_metrics(15, [path])
        seg = processor.merged_data["ns1"]["gpu-pod"]["metrics"][200]
        assert seg["gpu_request"] == 1
        assert seg["gpu_type"] == "NVIDIA-A100"

    def test_load_multiple_files_merges_namespaces(self, create_metrics_file):
        """Tests loading two files -> asserts pods from both files appear in merged_data."""
        file1 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "pod1": {"segments": [{"start": 0, "duration": 900, "cpu_request": 1}]}
                }
            },
        }
        file2 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-02",
            "end_date": "2025-01-02",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "pod2": {"segments": [{"start": 1000, "duration": 900, "cpu_request": 2}]}
                }
            },
        }
        p1 = create_metrics_file(file1, "f1.json")
        p2 = create_metrics_file(file2, "f2.json")
        processor = load_and_merge_metrics(15, [p1, p2])
        assert "pod1" in processor.merged_data["ns1"]
        assert "pod2" in processor.merged_data["ns1"]

    def test_empty_namespaces_file(self, create_metrics_file):
        """Tests file with empty namespaces -> asserts merged_data stays empty without crashing."""
        data = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {},
        }
        path = create_metrics_file(data, "empty.json")
        processor = load_and_merge_metrics(15, [path])
        assert processor.merged_data == {}

    def test_mismatched_interval_raises(self, create_metrics_file):
        """Tests files with different interval_minutes -> asserts load_metadata fails early."""
        file1 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {},
        }
        file2 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-02",
            "end_date": "2025-01-02",
            "interval_minutes": 5,
            "namespaces": {},
        }
        p1 = create_metrics_file(file1, "f1.json")
        p2 = create_metrics_file(file2, "f2.json")
        with pytest.raises(SystemExit):
            load_metrics_metadata([p1, p2])

    def test_overlapping_pod_data(self, create_metrics_file):
        """Tests same pod in two files with overlapping times -> asserts both segments are loaded."""
        file1 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "pod1": {"segments": [{"start": 0, "duration": 900, "cpu_request": 1}]}
                }
            },
        }
        file2 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "pod1": {"segments": [{"start": 1000, "duration": 900, "cpu_request": 2}]}
                }
            },
        }
        p1 = create_metrics_file(file1, "f1.json")
        p2 = create_metrics_file(file2, "f2.json")
        processor = load_and_merge_metrics(15, [p1, p2])
        metrics = processor.merged_data["ns1"]["pod1"]["metrics"]
        assert 0 in metrics and 1000 in metrics

    def test_roundtrip_producer_to_ingest(self, create_metrics_file):
        """Tests producer output shape -> asserts it can be loaded by ingest without data loss."""
        # Use only CPU for this round-trip to avoid timestamp collision in load_segment_data
        cpu_segs = [
            {"start": 0, "duration": 1800, "cpu_request": 4, "pod": "p1", "namespace": "ns1", "node": "n1"}
        ]
        namespaces = metrics_processor.MetricsProcessor.build_namespaces_dict(cpu_segs)

        data = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": namespaces,
        }
        path = create_metrics_file(data, "roundtrip.json")
        processor = load_and_merge_metrics(15, [path])
        seg = processor.merged_data["ns1"]["p1"]["metrics"][0]
        assert seg["cpu_request"] == 4

    def test_pod_spans_two_days(self, create_metrics_file):
        """Tests same pod appearing in two consecutive daily files -> asserts segments from both days are merged correctly."""
        day1 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-01",
            "end_date": "2025-01-01",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "long-running-pod": {
                        "segments": [
                            {"start": 0, "duration": 86400, "cpu_request": 2, "memory_request": 4}
                        ]
                    }
                }
            },
        }
        day2 = {
            "cluster_name": "ocp-prod",
            "start_date": "2025-01-02",
            "end_date": "2025-01-02",
            "interval_minutes": 15,
            "namespaces": {
                "ns1": {
                    "long-running-pod": {
                        "segments": [
                            {"start": 86400, "duration": 86400, "cpu_request": 2, "memory_request": 4}
                        ]
                    }
                }
            },
        }
        p1 = create_metrics_file(day1, "day1.json")
        p2 = create_metrics_file(day2, "day2.json")
        processor = load_and_merge_metrics(15, [p1, p2])
        pod_metrics = processor.merged_data["ns1"]["long-running-pod"]["metrics"]
        assert 0 in pod_metrics
        assert 86400 in pod_metrics
        assert pod_metrics[0]["cpu_request"] == 2
        assert pod_metrics[86400]["cpu_request"] == 2

        total_duration = sum(seg["duration"] for seg in pod_metrics.values())
        assert total_duration == 86400 * 2


def test_load_metrics_metadata_failure(
    create_metrics_file, mock_metrics_file2, mock_metrics_file3
):
    """Test that loading metadata fails when files have different interval_minutes."""

    p2 = create_metrics_file(mock_metrics_file2, "file2.json")
    p3 = create_metrics_file(mock_metrics_file3, "file3.json")

    with pytest.raises(SystemExit):
        load_metrics_metadata([p2, p3])
