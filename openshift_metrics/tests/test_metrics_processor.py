from unittest import TestCase
from openshift_metrics import metrics_processor


class TestExtractGPUInfo(TestCase):
    def test_extract_gpu_info(self):
        metric_with_label = {
            "metric": {
                "pod": "pod2",
                "namespace": "namespace1",
                "resource": "nvidia.com/gpu",
                "label_nvidia_com_gpu_product": "V100-GPU",
                "node": "node-1",
                "label_nvidia_com_gpu_machine": "Dell PowerEdge",
            },
        }
        gpu_info = metrics_processor.MetricsProcessor._extract_gpu_info(
            "gpu_request", metric_with_label
        )
        self.assertEqual(gpu_info.gpu_type, "V100-GPU")
        self.assertEqual(gpu_info.gpu_resource, "nvidia.com/gpu")
        self.assertEqual(gpu_info.node_model, "Dell PowerEdge")

    def test_extract_gpu_info_with_missing_labels(self):
        metric = {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "nvidia.com/gpu",
                "node": "wrk-1",
            },
        }
        gpu_info = metrics_processor.MetricsProcessor._extract_gpu_info(
            "gpu_request", metric
        )
        self.assertEqual(gpu_info.gpu_type, metrics_processor.GPU_UNKNOWN_TYPE)
        self.assertEqual(gpu_info.gpu_resource, "nvidia.com/gpu")
        self.assertIsNone(gpu_info.node_model)

    def test_extract_gpu_info_no_info_anywhere(self):
        metric = {
            "metric": {
                "pod": "pod1",
                "namespace": "namespace1",
                "resource": "cpu",
            },
        }
        gpu_info = metrics_processor.MetricsProcessor._extract_gpu_info(
            "cpu_request", metric
        )
        self.assertIsNone(gpu_info.gpu_type)
        self.assertIsNone(gpu_info.gpu_resource)
        self.assertIsNone(gpu_info.node_model)


class TestInsertNodeLabels(TestCase):
    def test_insert_node_labels(self):
        resource_request_metrics = [
            {
                "metric": {
                    "pod": "TestPodA",
                    "node": "wrk-1",
                    "namespace": "namespace1",
                },
                "values": [[1730939400, "4"], [1730940300, "4"], [1730941200, "4"]],
            },
        ]
        node_labels = [
            {
                "metric": {
                    "node": "wrk-1",
                    "label_nvidia_com_gpu_product": "A100",
                    "label_nvidia_com_gpu_machine": "Dell",
                }
            }
        ]
        result = metrics_processor.MetricsProcessor.insert_node_labels(
            node_labels, resource_request_metrics
        )
        self.assertEqual(
            result[0]["metric"]["label_nvidia_com_gpu_product"], "A100"
        )
        self.assertEqual(
            result[0]["metric"]["label_nvidia_com_gpu_machine"], "Dell"
        )


class TestStripEssentialLabels(TestCase):
    def test_strip_to_essential_labels(self):
        raw = [
            {
                "metric": {
                    "pod": "p1",
                    "namespace": "ns1",
                    "node": "n1",
                    "container": "main",
                    "job": "kube-state-metrics",
                    "label_nerc_mghpcc_org_class": "classA",
                },
                "values": [[0, "1"]],
            }
        ]
        stripped = metrics_processor.MetricsProcessor.strip_to_essential_labels(raw)
        self.assertIn("pod", stripped[0]["metric"])
        self.assertIn("label_nerc_mghpcc_org_class", stripped[0]["metric"])
        self.assertNotIn("container", stripped[0]["metric"])
        self.assertNotIn("job", stripped[0]["metric"])

    def test_strip_empty(self):
        self.assertEqual(
            metrics_processor.MetricsProcessor.strip_to_essential_labels([]), []
        )


class TestCondenseValues(TestCase):
    def test_condense_values(self):
        vals = [[0, "1"], [900, "1"], [1800, "2"]]
        out = metrics_processor.MetricsProcessor._condense_values(vals, 900, "cpu_request")
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["cpu_request"], "1")
        self.assertEqual(out[1]["cpu_request"], "2")

    def test_condense_metric_series(self):
        raw = [{"metric": {"pod": "p1"}, "values": [[0, "2"], [900, "2"]]}]
        out = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["cpu_request"], "2")
        self.assertEqual(out[0]["pod"], "p1")


class TestProducerCondenseLogic(TestCase):
    """Tests that replace the coverage lost from the old TestCondenseMetrics / TestMergeMetrics classes."""

    def test_basic_condense(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 10], [900, 10], [1800, 10]],
            }
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["cpu_request"], 10)
        self.assertEqual(segments[0]["duration"], 2700)

    def test_value_change(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 10], [900, 10], [1800, 20], [2700, 20]],
            }
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["cpu_request"], 10)
        self.assertEqual(segments[1]["cpu_request"], 20)

    def test_gpu_type_change(self):
        raw = [
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "ns1",
                    "label_nvidia_com_gpu_product": "V100",
                },
                "values": [[0, 1], [900, 1], [1800, 1]],
            },
            {
                "metric": {
                    "pod": "pod1",
                    "namespace": "ns1",
                    "label_nvidia_com_gpu_product": "A100",
                },
                "values": [[2700, 1], [3600, 1]],
            },
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "gpu_request")
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["label_nvidia_com_gpu_product"], "V100")
        self.assertEqual(segments[1]["label_nvidia_com_gpu_product"], "A100")

    def test_time_skip_creates_new_segment(self):
        raw = [
            {
                "metric": {"pod": "pod1", "namespace": "ns1"},
                "values": [[0, 5], [900, 5], [5400, 5]],  # large gap
            }
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 2)

    def test_build_namespaces_dict(self):
        cpu_segs = [
            {"start": 0, "duration": 900, "cpu_request": 4, "pod": "p1", "namespace": "ns1", "node": "n1"}
        ]
        mem_segs = [
            {"start": 0, "duration": 900, "memory_request": 8, "pod": "p1", "namespace": "ns1", "node": "n1"}
        ]
        namespaces = metrics_processor.MetricsProcessor.build_namespaces_dict(cpu_segs, mem_segs)
        self.assertIn("ns1", namespaces)
        self.assertIn("p1", namespaces["ns1"])
        self.assertEqual(len(namespaces["ns1"]["p1"]["segments"]), 2)

    def test_separate_series_same_pod_namespace_merge_into_one_pod(self):
        """Tests that two distinct raw Prometheus series sharing the same pod and
        namespace are collapsed into a single pod entry during preprocessing.

        When a pod is killed and recreated with the same name/namespace but
        different resources, Prometheus reports it as two separate series (two
        'metric' entries with identical pod/namespace labels but different
        values). build_namespaces_dict keys by (namespace, pod), so both series
        end up under one pod key rather than two.
        """
        raw = [
            {
                "metric": {"pod": "database", "namespace": "ns1", "node": "n1"},
                "values": [[0, 1], [900, 1]],       # first lifetime, 1 core
            },
            {
                "metric": {"pod": "database", "namespace": "ns1", "node": "n1"},
                "values": [[1800, 4], [2700, 4]],   # recreated, 4 cores -> separate series
            },
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(
            raw, 900, "cpu_request"
        )
        namespaces = metrics_processor.MetricsProcessor.build_namespaces_dict(segments)

        # The two separate series collapse into a single pod key.
        self.assertEqual(list(namespaces["ns1"].keys()), ["database"])
        pod_segments = namespaces["ns1"]["database"]["segments"]
        self.assertEqual(len(pod_segments), 2)

        by_start = {seg["start"]: seg for seg in pod_segments}
        self.assertEqual(by_start[0]["cpu_request"], 1)
        self.assertEqual(by_start[1800]["cpu_request"], 4)

    def test_same_name_namespace_grouped_as_one_pod(self):
        """Tests that a same name + namespace pod stopped and restarted after a
        long gap is grouped under a single pod key, with the gap excluded.

        Edge case: a pod 'database' runs, is killed, and is later recreated with
        the same name and namespace and the same resources. The downtime spans
        more than one sampling slot, so the restart shows up as a gap of 3x the
        interval inside a single series (interval = 900s, gap = 2700s).

        Because a name is unique within a namespace at any instant, the two
        lifetimes are necessarily sequential. _was_pod_stopped detects the gap
        (> interval), so the producer emits two segments grouped under one pod
        key, and the downtime between them is not billed.
        """
        interval = 900
        # Same value/series throughout so the split is driven by _was_pod_stopped,
        # not by a value change. Slots at 0, 900, then a 3x-interval jump to 3600.
        raw = [
            {
                "metric": {"pod": "database", "namespace": "ns1", "node": "n1"},
                "values": [
                    [0, 1],       # first lifetime starts
                    [900, 1],     # still running (gap to next = 2700s = 3x interval)
                    [3600, 1],    # recreated after downtime
                    [4500, 1],    # still running
                ],
            },
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(
            raw, interval, "cpu_request"
        )
        self.assertEqual(len(segments), 2)

        namespaces = metrics_processor.MetricsProcessor.build_namespaces_dict(segments)
        # Both lifetimes are grouped under the single pod key.
        self.assertEqual(list(namespaces["ns1"].keys()), ["database"])
        pod_segments = namespaces["ns1"]["database"]["segments"]
        self.assertEqual(len(pod_segments), 2)

        # Distinct start times mean both segments survive (no collision/overwrite).
        by_start = {seg["start"]: seg for seg in pod_segments}
        self.assertEqual(set(by_start), {0, 3600})
        self.assertEqual(by_start[0]["cpu_request"], 1)
        self.assertEqual(by_start[3600]["cpu_request"], 1)

        # Each lifetime covers two slots -> 900 + interval = 1800s billed.
        self.assertEqual(by_start[0]["duration"], 1800)
        self.assertEqual(by_start[3600]["duration"], 1800)

        # The 2700s downtime between 1800 and 3600 is excluded from billing:
        # total billed (3600s) is less than the 5400s wall-clock span.
        total_billed = sum(seg["duration"] for seg in pod_segments)
        self.assertEqual(total_billed, 3600)

    def test_detects_pod_stop_and_restart(self):
        """Tests that a large gap between samples creates separate segments (pod stopped then restarted)."""
        raw = [
            {
                "metric": {"pod": "p1", "namespace": "ns1"},
                "values": [
                    [0, 2],
                    [900, 2],
                    [1800, 2],
                    # large gap here (pod stopped)
                    [7200, 2],
                    [8100, 2],
                ],
            }
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]["start"], 0)
        self.assertEqual(segments[0]["duration"], 2700)   # 0 -> 1800 + interval
        self.assertEqual(segments[1]["start"], 7200)
        self.assertEqual(segments[1]["duration"], 1800)   # 7200 -> 8100 + interval

    def test_pod_restart_within_interval_is_single_segment(self):
        """Tests that a pod killed and restarted within one query interval is treated as a single segment.

        Edge case: a pod named 'database' runs from 1:00 PM to 1:15 PM,
        is killed, and restarted at 1:20 PM with the same name and resources.
        The gap between 1:15 PM (sample at 900s) and 1:20 PM (sample at 1000s) is
        only 100s, which is less than the 900s interval, so the condense logic
        does not split it into separate segments.

        Note: the current condense logic calculates duration as
        last_timestamp - first_timestamp + interval, which includes the gap.
        The actual active runtime is 1800s (900 + 900), but the segment
        duration is 2800s (1900 - 0 + 900).
        """
        raw = [
            {
                "metric": {"pod": "database", "namespace": "ns1"},
                "values": [
                    [0, 2],      # 1:00 PM
                    [900, 2],    # 1:15 PM (pod killed here)
                    [1000, 2],   # 1:20 PM (pod restarted, same name/resources)
                    [1900, 2],   # 1:35 PM
                ],
            }
        ]
        segments = metrics_processor.MetricsProcessor.condense_metric_series(raw, 900, "cpu_request")
        # The gap (100s) is less than the interval (900s), so it stays as one segment
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["start"], 0)
        # Duration spans the full range including the gap: 1900 - 0 + 900 = 2800
        self.assertEqual(segments[0]["duration"], 2800)
        self.assertEqual(segments[0]["cpu_request"], 2)
