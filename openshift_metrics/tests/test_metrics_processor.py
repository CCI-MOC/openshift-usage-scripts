from unittest import TestCase
from openshift_metrics import metrics_processor, invoice


class TestMergeMetrics(TestCase):
    def test_merge_metrics_empty(self):
        test_metric_list = [
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
        expected_output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 10},
                        60: {"cpu": 15},
                        120: {"cpu": 20},
                    },
                },
                "pod2": {
                    "metrics": {
                        0: {"cpu": 30},
                        60: {"cpu": 35},
                        120: {"cpu": 40},
                    },
                },
            }
        }
        processor = metrics_processor.MetricsProcessor()
        processor.merge_metrics("cpu", test_metric_list)
        self.assertEqual(processor.merged_data, expected_output_dict)

    def test_merge_metrics_not_empty(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "mem",
                },
                "values": [
                    [0, 100],
                    [60, 150],
                    [120, 200],
                ],
            },
            {
                "metric": {
                    "pod": "pod2",
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [60, 300],
                ],
            },
        ]
        output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 10},
                        60: {"cpu": 15},
                        120: {"cpu": 20},
                    },
                },
                "pod2": {
                    "metrics": {
                        0: {"cpu": 30},
                        60: {"cpu": 35},
                        120: {"cpu": 40},
                    },
                },
            }
        }
        expected_output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 10, "mem": 100},
                        60: {"cpu": 15, "mem": 150},
                        120: {"cpu": 20, "mem": 200},
                    },
                },
                "pod2": {
                    "metrics": {
                        0: {"cpu": 30},
                        60: {"cpu": 35, "mem": 300},
                        120: {"cpu": 40},
                    },
                },
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=output_dict)
        processor.merge_metrics("mem", test_metric_list)
        self.assertEqual(processor.merged_data, expected_output_dict)

    def test_merge_metrics_overlapping_range(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [0, 10],
                    [60, 10],
                    [120, 10],
                ],
            },
        ]
        test_metric_list_2 = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "cpu",
                },
                "values": [
                    [60, 8],
                    [120, 8],
                    [180, 10],
                ],
            },
        ]
        expected_output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 10},
                        60: {"cpu": 8},
                        120: {"cpu": 8},
                        180: {"cpu": 10},
                    },
                },
            }
        }
        processor = metrics_processor.MetricsProcessor()
        processor.merge_metrics("cpu", test_metric_list)
        processor.merge_metrics("cpu", test_metric_list_2)
        self.assertEqual(processor.merged_data, expected_output_dict)

        # trying to merge the same metrics again should not change anything
        processor.merge_metrics("cpu", test_metric_list_2)
        self.assertEqual(processor.merged_data, expected_output_dict)

    def test_merge_metrics_same_pod_name(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "podA",
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
                    "pod": "podA",
                    "namespace": "namespace2",
                    "resource": "cpu",
                },
                "values": [
                    [0, 30],
                    [60, 35],
                    [120, 40],
                ],
            },
        ]
        expected_output_dict = {
            "namespace1": {
                "podA": {
                    "metrics": {
                        0: {"cpu": 10},
                        60: {"cpu": 15},
                        120: {"cpu": 20},
                    },
                }
            },
            "namespace2": {
                "podA": {
                    "metrics": {
                        0: {"cpu": 30},
                        60: {"cpu": 35},
                        120: {"cpu": 40},
                    },
                },
            },
        }
        processor = metrics_processor.MetricsProcessor()
        processor.merge_metrics("cpu", test_metric_list)
        self.assertEqual(processor.merged_data, expected_output_dict)

    def test_merge_metrics_not_empty_with_gpu(self):
        test_metric_list = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "namespace1",
                    "resource": "nvidia.com/gpu",
                    "label_nvidia_com_gpu_product": "Tesla-V100-PCIE-32GB",
                },
                "values": [
                    [0, 1],
                    [60, 1],
                    [120, 2],
                ],
            },
        ]
        output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 10},
                        60: {"cpu": 15},
                        120: {"cpu": 20},
                    },
                },
            }
        }
        expected_output_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu": 10,
                            "gpu_request": 1,
                            "gpu_type": "Tesla-V100-PCIE-32GB",
                            "gpu_resource": "nvidia.com/gpu",
                        },
                        60: {
                            "cpu": 15,
                            "gpu_request": 1,
                            "gpu_type": "Tesla-V100-PCIE-32GB",
                            "gpu_resource": "nvidia.com/gpu",
                        },
                        120: {
                            "cpu": 20,
                            "gpu_request": 2,
                            "gpu_type": "Tesla-V100-PCIE-32GB",
                            "gpu_resource": "nvidia.com/gpu",
                        },
                    },
                },
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=output_dict)
        processor.merge_metrics("gpu_request", test_metric_list)
        self.assertEqual(processor.merged_data, expected_output_dict)


class TestCondenseMetrics(TestCase):
    def test_condense_metrics(self):
        test_input_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu": 10,
                            "mem": 15,
                        },
                        900: {
                            "cpu": 10,
                            "mem": 15,
                        },
                    }
                },
                "pod2": {
                    "metrics": {
                        0: {
                            "cpu": 2,
                            "mem": 256,
                        },
                        900: {
                            "cpu": 2,
                            "mem": 256,
                        },
                    }
                },
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod1": {"metrics": {0: {"cpu": 10, "mem": 15, "duration": 1800}}},
                "pod2": {"metrics": {0: {"cpu": 2, "mem": 256, "duration": 1800}}},
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(["cpu", "mem"])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_no_interval(self):
        test_input_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu": 10,
                            "mem": 15,
                        }
                    }
                },
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod1": {"metrics": {0: {"cpu": 10, "mem": 15, "duration": 900}}},
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(["cpu", "mem"])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_change(self):
        test_input_dict = {
            "namespace1": {
                "pod2": {
                    "metrics": {
                        0: {
                            "cpu": 20,
                            "mem": 25,
                        },
                        900: {
                            "cpu": 20,
                            "mem": 25,
                        },
                        1800: {
                            "cpu": 25,
                            "mem": 25,
                        },
                        2700: {
                            "cpu": 20,
                            "mem": 25,
                        },
                    }
                },
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod2": {
                    "metrics": {
                        0: {"cpu": 20, "mem": 25, "duration": 1800},
                        1800: {"cpu": 25, "mem": 25, "duration": 900},
                        2700: {"cpu": 20, "mem": 25, "duration": 900},
                    }
                },
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(["cpu", "mem"])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_skip_metric(self):
        test_input_dict = {
            "namespace1": {
                "pod3": {
                    "metrics": {
                        0: {
                            "cpu": 30,
                            "mem": 35,
                            "gpu": 1,
                        },
                        900: {
                            "cpu": 30,
                            "mem": 35,
                            "gpu": 2,
                        },
                    }
                }
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod3": {
                    "metrics": {0: {"cpu": 30, "mem": 35, "gpu": 1, "duration": 1800}}
                },
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(["cpu", "mem"])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_timeskips(self):
        test_input_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        900: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        1800: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        5400: {  # time skipped
                            "cpu": 1,
                            "mem": 4,
                        },
                        6300: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        8100: {  # metric changed and time skipped
                            "cpu": 2,
                            "mem": 8,
                        },
                        9000: {
                            "cpu": 2,
                            "mem": 8,
                        },
                    }
                },
                "pod2": {
                    "metrics": {
                        0: {
                            "cpu": 2,
                            "mem": 16,
                        },
                        900: {
                            "cpu": 2,
                            "mem": 16,
                        },
                    }
                },
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 1, "mem": 4, "duration": 2700},
                        5400: {"cpu": 1, "mem": 4, "duration": 1800},
                        8100: {"cpu": 2, "mem": 8, "duration": 1800},
                    }
                },
                "pod2": {"metrics": {0: {"cpu": 2, "mem": 16, "duration": 1800}}},
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(["cpu", "mem"])
        self.assertEqual(condensed_dict, expected_condensed_dict)

    def test_condense_metrics_with_changing_gpu(self):
        test_input_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        900: {
                            "cpu": 1,
                            "mem": 4,
                        },
                        1800: {  # pod acquires a GPU
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_V100,
                        },
                        2700: {
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_V100,
                        },
                        3600: {  # type of GPU is changed
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        4500: {
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        5400: {
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        6300: {  # count of GPU is changed
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 3,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        7200: {
                            "cpu": 1,
                            "mem": 4,
                            "gpu_request": 3,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        8100: {  # no longer using GPUs
                            "cpu": 1,
                            "mem": 4,
                        },
                    }
                },
            }
        }
        expected_condensed_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {"cpu": 1, "mem": 4, "duration": 1800},
                        1800: {
                            "cpu": 1,
                            "mem": 4,
                            "duration": 1800,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_V100,
                        },
                        3600: {
                            "cpu": 1,
                            "mem": 4,
                            "duration": 2700,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        6300: {
                            "cpu": 1,
                            "mem": 4,
                            "duration": 1800,
                            "gpu_request": 3,
                            "gpu_type": invoice.GPU_A100_SXM4,
                        },
                        8100: {
                            "cpu": 1,
                            "mem": 4,
                            "duration": 900,
                        },
                    }
                },
            }
        }
        processor = metrics_processor.MetricsProcessor(merged_data=test_input_dict)
        condensed_dict = processor.condense_metrics(
            ["cpu", "mem", "gpu_request", "gpu_type"]
        )
        self.assertEqual(condensed_dict, expected_condensed_dict)
