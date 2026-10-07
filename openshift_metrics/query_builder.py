"""Constructs Prometheus queries based on cluster type."""


class QueryBuilder:
    """Builds metric queries for standalone or hypershift clusters."""

    def __init__(self, cluster_type: str, cluster_id: str):
        self.cluster_type = cluster_type
        self.cluster_id = cluster_id

    def _build_resource_request(self, resource_selector: str) -> str:
        """Build a kube_pod_resource_request query, scoped to the cluster for hypershift."""
        selectors = f'{resource_selector}, node!=""'
        if self.cluster_type == "hypershift":
            selectors += f', _id="{self.cluster_id}"'
        return f"kube_pod_resource_request{{{selectors}}}"

    def build_cpu_request(self) -> str:
        """Build CPU request query based on cluster type."""
        return self._build_resource_request('resource="cpu"')

    def build_memory_request(self) -> str:
        """Build memory request query based on cluster type."""
        return self._build_resource_request('resource="memory"')

    def build_gpu_request(self) -> str:
        """Build GPU request query based on cluster type."""
        return self._build_resource_request('resource=~"nvidia.com.*"')

    @staticmethod
    def build_kube_node_labels() -> str:
        """Build kube node labels query (same for both cluster types)."""
        return 'kube_node_labels{label_nvidia_com_gpu_product!=""}'

    @staticmethod
    def build_kube_pod_labels() -> str:
        """Build kube pod labels query (same for both cluster types)."""
        return 'kube_pod_labels{label_nerc_mghpcc_org_class!=""}'
