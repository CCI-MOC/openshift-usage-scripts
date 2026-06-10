import pytest
import json


@pytest.fixture
def mock_metrics_file1():
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-20",
        "end_date": "2025-09-20",
        "interval_minutes": 15,
        "namespaces": {
            "namespace1": {
                "pod1": {
                    "segments": [
                        {
                            "start": 0,
                            "duration": 60,
                            "cpu_request": 10,
                            "memory_request": 10,
                        },
                        {
                            "start": 60,
                            "duration": 60,
                            "cpu_request": 15,
                            "memory_request": 15,
                        },
                        {
                            "start": 120,
                            "duration": 60,
                            "cpu_request": 20,
                            "memory_request": 20,
                        },
                    ]
                },
                "pod2": {
                    "segments": [
                        {
                            "start": 0,
                            "duration": 60,
                            "cpu_request": 30,
                            "memory_request": 30,
                        },
                        {
                            "start": 60,
                            "duration": 60,
                            "cpu_request": 35,
                            "memory_request": 35,
                        },
                        {
                            "start": 120,
                            "duration": 60,
                            "cpu_request": 40,
                            "memory_request": 40,
                        },
                    ]
                },
            }
        },
    }


@pytest.fixture
def mock_metrics_file2():
    # New pod-centric format (already condensed segments)
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-21",
        "end_date": "2025-09-21",
        "interval_minutes": 15,
        "namespaces": {
            "namespace1": {
                "pod1": {
                    "segments": [
                        {
                            "start": 180,
                            "duration": 60,
                            "cpu_request": 10,
                            "memory_request": 10,
                        },
                        {
                            "start": 240,
                            "duration": 60,
                            "cpu_request": 15,
                            "memory_request": 15,
                        },
                        {
                            "start": 300,
                            "duration": 60,
                            "cpu_request": 20,
                            "memory_request": 20,
                        },
                    ]
                },
                "pod2": {
                    "segments": [
                        {
                            "start": 180,
                            "duration": 60,
                            "cpu_request": 30,
                            "memory_request": 30,
                        },
                        {
                            "start": 240,
                            "duration": 60,
                            "cpu_request": 35,
                            "memory_request": 35,
                        },
                        {
                            "start": 300,
                            "duration": 60,
                            "cpu_request": 40,
                            "memory_request": 40,
                        },
                    ]
                },
            }
        },
    }


@pytest.fixture
def mock_metrics_file3():
    # Empty file in new format
    return {
        "cluster_name": "ocp-prod",
        "start_date": "2025-09-21",
        "end_date": "2025-09-21",
        "interval_minutes": 3,  # file1 and file2 have 15 minutes
        "namespaces": {},
    }


@pytest.fixture
def create_metrics_file(tmp_path):
    """Fixture that returns the path to a file with json data"""

    def _create(data, filename):
        path = tmp_path / filename
        path.write_text(json.dumps(data))
        return path

    return _create
