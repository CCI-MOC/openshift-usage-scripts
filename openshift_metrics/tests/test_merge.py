import pytest
import json
from decimal import Decimal

from openshift_metrics.merge import (
    compare_dates,
    get_su_definitions,
    load_and_merge_metrics,
    load_metadata,
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


def test_load_and_merge_data(tmp_path, mock_metrics_file1, mock_metrics_file2):
    """
    Test that we can load metrics from the 2 files and merge the metrics from those.

    Note that we already have tests that test the merging of the data, this mostly
    focuses on the loading part.
    """
    p1 = tmp_path / "file1.json"
    p2 = tmp_path / "file2.json"

    p1.write_text(json.dumps(mock_metrics_file1))
    p2.write_text(json.dumps(mock_metrics_file2))

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


def test_load_metadata(tmp_path, mock_metrics_file1, mock_metrics_file2):
    """Test we can load metadata from the metrics files."""

    p1 = tmp_path / "file1.json"
    p2 = tmp_path / "file2.json"
    p1.write_text(json.dumps(mock_metrics_file1))
    p2.write_text(json.dumps(mock_metrics_file2))
    metadata = load_metadata([p1, p2])
    assert metadata.cluster_name == "ocp-prod"
    assert metadata.report_start_date == "2025-09-20"
    assert metadata.report_end_date == "2025-09-21"
    assert metadata.interval_minutes == "15"


def test_load_metadata_failure(tmp_path, mock_metrics_file2, mock_metrics_file3):
    """Test that loading metadata fails when files have different interval_minutes."""

    p2 = tmp_path / "file2.json"
    p3 = tmp_path / "file3.json"
    p2.write_text(json.dumps(mock_metrics_file2))
    p3.write_text(json.dumps(mock_metrics_file3))

    with pytest.raises(SystemExit):
        load_metadata([p2, p3])
