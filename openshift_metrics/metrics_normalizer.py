"""Normalizes metrics from different cluster types to consistent format."""


class MetricsNormalizer:
    """Normalizes raw Prometheus metrics to consistent key names."""

    @staticmethod
    def normalize(metrics: list, cluster_type: str) -> list:
        """
        Normalize metrics based on cluster type.

        For hypershift clusters, exported_namespace/exported_pod are renamed to
        namespace/pod to match standalone cluster format.
        """
        if cluster_type == "standalone":
            return metrics

        if cluster_type == "hypershift":
            return MetricsNormalizer._normalize_hypershift(metrics)

        raise ValueError(f"Unknown cluster type: {cluster_type}")

    @staticmethod
    def _normalize_hypershift(metrics: list) -> list:
        """Rename hypershift metric keys to standalone format."""
        for metric in metrics:
            metric_labels = metric.get("metric", {})

            if "exported_namespace" in metric_labels:
                metric_labels["namespace"] = metric_labels.pop("exported_namespace")

            if "exported_pod" in metric_labels:
                metric_labels["pod"] = metric_labels.pop("exported_pod")

        return metrics
