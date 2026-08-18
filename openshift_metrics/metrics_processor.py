import json
from typing import List, Dict
from collections import namedtuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GPU_UNKNOWN_TYPE = "GPU_UNKNOWN_TYPE"
GPUInfo = namedtuple("GPUInfo", ["gpu_type", "gpu_resource", "node_model"])


class MetricsProcessor:
    """Provides methods for merging metrics and processing it for billing purposes"""

    def __init__(
        self,
        interval_minutes: int = 15,
        merged_data: dict = None,
        gpu_mapping_file: str = "gpu_node_map.json",
    ):
        self.interval_minutes = interval_minutes
        self.merged_data = merged_data if merged_data is not None else {}
        self.gpu_mapping = self._load_gpu_mapping(gpu_mapping_file)

    def merge_metrics(self, metric_name, metric_list):
        """Merge metrics (cpu, memory, gpu) by pod"""
        for metric in metric_list:
            pod = metric["metric"]["pod"]
            namespace = metric["metric"]["namespace"]
            node = metric["metric"].get("node")

            self.merged_data.setdefault(namespace, {})
            self.merged_data[namespace].setdefault(pod, {"metrics": {}})

            if metric_name == "cpu_request":
                class_name = metric["metric"].get("label_nerc_mghpcc_org_class")
                if class_name is not None:
                    self.merged_data[namespace][pod]["label_nerc_mghpcc_org_class"] = (
                        class_name
                    )

            gpu_type, gpu_resource, node_model = self._extract_gpu_info(
                metric_name, metric
            )

            for epoch_time, metric_value in metric["values"]:
                self.merged_data[namespace][pod]["metrics"].setdefault(epoch_time, {})

                self.merged_data[namespace][pod]["metrics"][epoch_time][metric_name] = (
                    metric_value
                )
                if gpu_type:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "gpu_type"
                    ] = gpu_type
                if gpu_resource:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "gpu_resource"
                    ] = gpu_resource
                if node_model:
                    self.merged_data[namespace][pod]["metrics"][epoch_time][
                        "node_model"
                    ] = node_model
                if node:
                    self.merged_data[namespace][pod]["metrics"][epoch_time]["node"] = (
                        node
                    )

    def _extract_gpu_info(self, metric_name: str, metric: Dict) -> GPUInfo:
        """Extract GPU related info"""
        gpu_type = None
        gpu_resource = None
        node_model = None

        if metric_name == "gpu_request":
            gpu_type = metric["metric"].get(
                "label_nvidia_com_gpu_product", GPU_UNKNOWN_TYPE
            )
            gpu_resource = metric["metric"].get("resource")
            node_model = metric["metric"].get("label_nvidia_com_gpu_machine")

            # Sometimes GPU labels from the nodes can be missing, in that case
            # we get the gpu_type from the gpu-node file
            if gpu_type == GPU_UNKNOWN_TYPE:
                node_name = metric["metric"].get("node")
                gpu_type = self.gpu_mapping.get(node_name, GPU_UNKNOWN_TYPE)

        return GPUInfo(gpu_type, gpu_resource, node_model)

    @staticmethod
    def _load_gpu_mapping(file_path: str) -> Dict[str, str]:
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            logger.warning("Could not load gpu-node map file: %s", file_path)
            return {}

    def condense_metrics(self, metrics_to_check: List[str]) -> Dict:
        """
        Checks if the value of metrics is the same, and removes redundant
        metrics while updating the duration. If there's a gap in the reported
        metrics then don't count that as part of duration.
        """
        interval = self.interval_minutes * 60
        condensed_dict = {}

        for namespace, pods in self.merged_data.items():
            condensed_dict.setdefault(namespace, {})

            for pod, pod_dict in pods.items():
                metrics_dict = pod_dict["metrics"]
                new_metrics_dict = {}
                epoch_times_list = sorted(metrics_dict.keys())

                start_epoch_time = epoch_times_list[0]

                start_metric_dict = metrics_dict[start_epoch_time].copy()

                for i in range(1, len(epoch_times_list)):
                    current_time = epoch_times_list[i]
                    previous_time = epoch_times_list[i - 1]

                    metrics_changed = self._are_metrics_different(
                        metrics_dict[start_epoch_time],
                        metrics_dict[current_time],
                        metrics_to_check,
                    )

                    pod_was_stopped = self._was_pod_stopped(
                        current_time=current_time,
                        previous_time=previous_time,
                        interval=interval,
                    )

                    if metrics_changed or pod_was_stopped:
                        duration = previous_time - start_epoch_time + interval
                        start_metric_dict["duration"] = duration
                        new_metrics_dict[start_epoch_time] = start_metric_dict

                        # Reset start_epoch_time and start_metric_dict
                        start_epoch_time = current_time
                        start_metric_dict = metrics_dict[start_epoch_time].copy()

                # Final block after the loop
                duration = epoch_times_list[-1] - start_epoch_time + interval
                start_metric_dict["duration"] = duration
                new_metrics_dict[start_epoch_time] = start_metric_dict

                # Update the pod dict with the condensed data
                new_pod_dict = pod_dict.copy()
                new_pod_dict["metrics"] = new_metrics_dict
                condensed_dict[namespace][pod] = new_pod_dict

        return condensed_dict

    @staticmethod
    def _are_metrics_different(
        metrics_a: Dict, metrics_b: Dict, metrics_to_check: List[str]
    ) -> bool:
        """Method that compares all the metrics in metrics_to_check are different in
        metrics_a and metrics_b
        """
        return any(
            metrics_a.get(metric, 0) != metrics_b.get(metric, 0)
            for metric in metrics_to_check
        )

    @staticmethod
    def _was_pod_stopped(current_time: int, previous_time: int, interval: int) -> bool:
        """
        A pod is assumed to be stopped if the the gap between two consecutive timestamps
        is more than the frequency of our metric collection
        """
        return (current_time - previous_time) > interval

    @staticmethod
    def _condense_values(values: list, interval: int, key: str = "value") -> list:
        """Return list of condensed segments for one Prometheus series."""
        if not values:
            return []
        values = sorted(values, key=lambda x: x[0])
        segments = []
        start_t, start_v = values[0]
        prev_t = start_t
        for t, v in values[1:]:
            if v != start_v or MetricsProcessor._was_pod_stopped(t, prev_t, interval):
                segments.append(
                    {
                        "start": start_t,
                        "duration": prev_t - start_t + interval,
                        key: start_v,
                    }
                )
                start_t, start_v = t, v
            prev_t = t
        segments.append(
            {
                "start": start_t,
                "duration": values[-1][0] - start_t + interval,
                key: start_v,
            }
        )
        return segments

    @staticmethod
    def condense_metric_series(metric_list: list, interval: int, key: str) -> list:
        """Apply per-series condense to a list of Prometheus metric objects."""
        if not metric_list:
            return []
        result = []
        for item in metric_list:
            segs = MetricsProcessor._condense_values(item["values"], interval, key)
            for seg in segs:
                seg.update(item["metric"])
            result.extend(segs)
        return result

    ESSENTIAL_LABEL_KEYS = {
        "pod",
        "namespace",
        "node",
        "label_nerc_mghpcc_org_class",
        "label_nvidia_com_gpu_product",
        "resource",
        "label_nvidia_com_gpu_machine",
    }

    @staticmethod
    def build_namespaces_dict(
        *segment_lists: list, gpu_mapping: dict | None = None
    ) -> dict:
        """Group condensed segments into the pod-centric export format."""
        namespaces = {}
        essential = MetricsProcessor.ESSENTIAL_LABEL_KEYS
        for seg_list in segment_lists:
            for seg in seg_list:
                ns = seg.get("namespace")
                pod = seg.get("pod")
                if not ns or not pod:
                    continue
                namespaces.setdefault(ns, {}).setdefault(pod, {"segments": []})
                clean = {
                    k: v
                    for k, v in seg.items()
                    if k in essential
                    or k
                    in (
                        "start",
                        "duration",
                        "cpu_request",
                        "memory_request",
                        "gpu_request",
                    )
                }
                clean.pop("pod", None)
                clean.pop("namespace", None)

                if "label_nvidia_com_gpu_product" in clean:
                    clean["gpu_type"] = clean.pop("label_nvidia_com_gpu_product")
                elif gpu_mapping is not None and "gpu_request" in clean:
                    node_name = clean.get("node")
                    if node_name:
                        clean["gpu_type"] = gpu_mapping.get(node_name, GPU_UNKNOWN_TYPE)
                if "label_nvidia_com_gpu_machine" in clean:
                    clean["node_model"] = clean.pop("label_nvidia_com_gpu_machine")
                if "resource" in clean and clean.get("gpu_request") is not None:
                    clean["gpu_resource"] = clean.pop("resource")

                namespaces[ns][pod]["segments"].append(clean)
        return namespaces

    def load_segment_data(self, namespaces: dict):
        """Load already-condensed pod-centric export data into merged_data."""
        for ns, pods in namespaces.items():
            self.merged_data.setdefault(ns, {})
            for pod, pod_data in pods.items():
                self.merged_data[ns].setdefault(pod, {"metrics": {}})
                for seg in pod_data.get("segments", []):
                    if "label_nerc_mghpcc_org_class" in seg:
                        self.merged_data[ns][pod].setdefault(
                            "label_nerc_mghpcc_org_class",
                            seg["label_nerc_mghpcc_org_class"],
                        )
                    start = seg["start"]
                    entry = self.merged_data[ns][pod]["metrics"].setdefault(start, {})
                    entry.update({k: v for k, v in seg.items() if k != "start"})

    @staticmethod
    def insert_node_labels(node_labels: list, resource_request_metrics: list) -> list:
        """Inserts node labels into resource_request_metrics"""
        node_label_dict = {}
        for node_label in node_labels:
            node = node_label["metric"]["node"]
            gpu = node_label["metric"].get("label_nvidia_com_gpu_product")
            machine = node_label["metric"].get("label_nvidia_com_gpu_machine")
            node_label_dict[node] = {"gpu": gpu, "machine": machine}
        for pod in resource_request_metrics:
            node = pod["metric"]["node"]
            if node not in node_label_dict:
                logger.warning("Could not find labels for node: %s", node)
                continue
            pod["metric"]["label_nvidia_com_gpu_product"] = node_label_dict[node].get(
                "gpu"
            )
            pod["metric"]["label_nvidia_com_gpu_machine"] = node_label_dict[node].get(
                "machine"
            )
        return resource_request_metrics

    @staticmethod
    def insert_pod_labels(pod_labels: list, resource_request_metrics: list) -> list:
        """Inserts `label_nerc_mghpcc_org_class` label into resource_request_metrics"""
        pod_label_dict = {}
        for pod_label in pod_labels:
            pod_name = pod_label["metric"]["pod"]
            class_name = pod_label["metric"].get("label_nerc_mghpcc_org_class")
            pod_label_dict[pod_name] = {"class": class_name}

        for pod in resource_request_metrics:
            pod_name = pod["metric"]["pod"]
            if pod_name not in pod_label_dict:
                continue
            pod["metric"]["label_nerc_mghpcc_org_class"] = pod_label_dict[pod_name].get(
                "class"
            )
        return resource_request_metrics

    @staticmethod
    def strip_to_essential_labels(metric_list: list) -> list:
        """Return a new list with only the labels used by merge and export."""
        if not metric_list:
            return metric_list
        essential = MetricsProcessor.ESSENTIAL_LABEL_KEYS
        stripped = []
        for item in metric_list:
            slim_metric = {k: v for k, v in item["metric"].items() if k in essential}
            stripped.append({"metric": slim_metric, "values": item["values"]})

        return stripped
