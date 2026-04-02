import pytest
import json


@pytest.fixture
def mock_metrics_file1():
    cpu_metrics = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [0, 10],
                [60, 15],
                [120, 20],
            ],
        },
        {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [0, 30],
                [60, 35],
                [120, 40],
            ],
        },
    ]
    memory_metrics = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "memory",
            },
            "values": [
                [0, 10],
                [60, 15],
                [120, 20],
            ],
        },
        {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [0, 30],
                [60, 35],
                [120, 40],
            ],
        },
    ]
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-20",
        "end_date": "2025-09-20",
        "interval_minutes": 15,
        "cpu_metrics": cpu_metrics,
        "memory_metrics": memory_metrics,
    }


@pytest.fixture
def mock_metrics_file2():
    cpu_metrics = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [180, 10],
                [240, 15],
                [300, 20],
            ],
        },
        {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [180, 30],
                [240, 35],
                [300, 40],
            ],
        },
    ]
    memory_metrics = [
        {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "memory",
            },
            "values": [
                [180, 10],
                [240, 15],
                [300, 20],
            ],
        },
        {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "cpu",
            },
            "values": [
                [180, 30],
                [240, 35],
                [300, 40],
            ],
        },
    ]
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-21",
        "end_date": "2025-09-21",
        "cpu_metrics": cpu_metrics,
        "memory_metrics": memory_metrics,
        "interval_minutes": 15,
    }


@pytest.fixture
def mock_metrics_file3():
    cpu_metrics = []
    memory_metrics = []
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-21",
        "end_date": "2025-09-21",
        "interval_minutes": 3,  # file1 and file2 have 15 minutes
        "cpu_metrics": cpu_metrics,
        "memory_metrics": memory_metrics,
    }


@pytest.fixture
def create_metrics_file(tmp_path):
    """Fixture that returns the path to a file with json data"""

    def _create(data, filename):
        path = tmp_path / filename
        path.write_text(json.dumps(data))
        return path

    return _create
