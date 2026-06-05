import json
from typing import Dict
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
        merged_data: dict | None = None,
        gpu_mapping_file: str = "gpu_node_map.json",
    ):
        self.interval_minutes = interval_minutes
        self.merged_data = merged_data if merged_data is not None else {}
        self.gpu_mapping = self._load_gpu_mapping(gpu_mapping_file)

    @staticmethod
    def _extract_gpu_info(metric_name: str, metric: Dict) -> GPUInfo:
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

        return GPUInfo(gpu_type, gpu_resource, node_model)

    @staticmethod
    def _load_gpu_mapping(file_path: str) -> Dict[str, str]:
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            logger.warning("Could not load gpu-node map file: %s", file_path)
            return {}

    @staticmethod
    def _was_pod_stopped(current_time: int, previous_time: int, interval: int) -> bool:
        """Return True if the gap between samples is larger than the query interval (pod likely stopped)."""
        return (current_time - previous_time) > interval

    @staticmethod
    def _condense_values(values: list, interval: int, key: str = "value") -> list:
        """Return list of condensed segments."""
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
        """Group condensed segments (from condense_metric_series) into the
        new pod-centric export format expected by load_segment_data().
        Only essential labels are kept.
        """
        namespaces = {}
        essential = MetricsProcessor.ESSENTIAL_LABEL_KEYS
        for seg_list in segment_lists:
            for seg in seg_list:
                ns = seg.get("namespace")
                pod = seg.get("pod")
                if not ns or not pod:
                    continue
                namespaces.setdefault(ns, {}).setdefault(pod, {"segments": []})
                # keep only essential labels + the metric value keys
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
                # remove pod/namespace from the segment body (they are keys)
                clean.pop("pod", None)
                clean.pop("namespace", None)

                # Rename Prometheus label keys to the nice names used by the invoice layer
                if "label_nvidia_com_gpu_product" in clean:
                    clean["gpu_type"] = clean.pop("label_nvidia_com_gpu_product")
                elif gpu_mapping is not None and "gpu_request" in clean:
                    # Fallback: resolve GPU type from the node-name mapping file
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
        """Load already-condensed segment data from the export format."""
        for ns, pods in namespaces.items():
            self.merged_data.setdefault(ns, {})
            for pod, pod_data in pods.items():
                self.merged_data[ns].setdefault(pod, {"metrics": {}})
                for seg in pod_data.get("segments", []):
                    # Promote the class label to pod level so write_metrics_by_classes can find it
                    if "label_nerc_mghpcc_org_class" in seg:
                        self.merged_data[ns][pod].setdefault(
                            "label_nerc_mghpcc_org_class",
                            seg["label_nerc_mghpcc_org_class"],
                        )
                    start = seg["start"]
                    # Use update so multiple segments at the same timestamp are merged,
                    # not overwritten (e.g. separate CPU and memory series share start times)
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
        """Return a new list with only the labels that merge_metrics() reads."""
        if not metric_list:
            return metric_list
        essential = MetricsProcessor.ESSENTIAL_LABEL_KEYS
        stripped = []
        for item in metric_list:
            slim_metric = {k: v for k, v in item["metric"].items() if k in essential}
            stripped.append({"metric": slim_metric, "values": item["values"]})

        return stripped
