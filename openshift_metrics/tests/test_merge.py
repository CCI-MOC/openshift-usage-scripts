import pytest
from decimal import Decimal

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


def test_load_metrics_metadata_failure(
    create_metrics_file, mock_metrics_file2, mock_metrics_file3
):
    """Test that loading metadata fails when files have different interval_minutes."""

    p2 = create_metrics_file(mock_metrics_file2, "file2.json")
    p3 = create_metrics_file(mock_metrics_file3, "file3.json")

    with pytest.raises(SystemExit):
        load_metrics_metadata([p2, p3])


def test_load_pod_centric_schema(create_metrics_file):
    """New namespaces/segments files are ingested without the legacy keys."""
    data = {
        "cluster_name": "ocp-prod",
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "interval_minutes": 15,
        "namespaces": {
            "ns1": {
                "pod1": {
                    "segments": [
                        {
                            "start": 100,
                            "duration": 900,
                            "cpu_request": 2,
                            "memory_request": 4,
                        }
                    ]
                }
            }
        },
    }
    path = create_metrics_file(data, "new-schema.json")
    processor = load_and_merge_metrics(15, [path])
    metrics = processor.merged_data["ns1"]["pod1"]["metrics"]
    assert metrics[100]["cpu_request"] == 2
    assert metrics[100]["memory_request"] == 4
    assert metrics[100]["duration"] == 900


def test_load_legacy_and_pod_centric_schemas_together(
    create_metrics_file, mock_metrics_file1
):
    """A report can mix historical Prometheus-series files and new files."""
    new_data = {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-21",
        "end_date": "2025-09-21",
        "interval_minutes": 15,
        "namespaces": {
            "namespace1": {
                "pod-new": {
                    "segments": [
                        {"start": 180, "duration": 60, "cpu_request": 8},
                    ]
                }
            }
        },
    }
    old_path = create_metrics_file(mock_metrics_file1, "old.json")
    new_path = create_metrics_file(new_data, "new.json")
    processor = load_and_merge_metrics(15, [old_path, new_path])
    assert "pod1" in processor.merged_data["namespace1"]
    assert (
        processor.merged_data["namespace1"]["pod-new"]["metrics"][180]["cpu_request"]
        == 8
    )


def test_file_with_both_schemas_uses_legacy_keys(create_metrics_file):
    """If cpu_metrics is present, namespaces is ignored so data is not double counted."""
    data = {
        "cluster_name": "ocp-prod",
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "interval_minutes": 15,
        "cpu_metrics": [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 3]],
            }
        ],
        "memory_metrics": [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 5]],
            }
        ],
        "namespaces": {
            "ns1": {
                "pod1": {
                    "segments": [
                        {
                            "start": 0,
                            "duration": 900,
                            "cpu_request": 99,
                            "memory_request": 99,
                        }
                    ]
                }
            }
        },
    }
    path = create_metrics_file(data, "both.json")
    processor = load_and_merge_metrics(15, [path])
    entry = processor.merged_data["ns1"]["pod1"]["metrics"][0]
    assert entry["cpu_request"] == 3
    assert entry["memory_request"] == 5


def test_load_pod_centric_gpu_fields(create_metrics_file):
    data = {
        "cluster_name": "ocp-prod",
        "start_date": "2025-01-01",
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
    assert seg["duration"] == 1800


def test_load_empty_namespaces(create_metrics_file):
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


def test_two_pod_centric_files_merge(create_metrics_file):
    day1 = {
        "cluster_name": "ocp-prod",
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "interval_minutes": 15,
        "namespaces": {
            "ns1": {
                "long-pod": {
                    "segments": [
                        {"start": 0, "duration": 86400, "cpu_request": 2},
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
                "long-pod": {
                    "segments": [
                        {"start": 86400, "duration": 86400, "cpu_request": 2},
                    ]
                }
            }
        },
    }
    p1 = create_metrics_file(day1, "day1.json")
    p2 = create_metrics_file(day2, "day2.json")
    processor = load_and_merge_metrics(15, [p1, p2])
    metrics = processor.merged_data["ns1"]["long-pod"]["metrics"]
    assert 0 in metrics and 86400 in metrics
    assert metrics[0]["duration"] + metrics[86400]["duration"] == 86400 * 2
