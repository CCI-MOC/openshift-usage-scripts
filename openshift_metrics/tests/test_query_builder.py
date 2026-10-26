"""Tests for query_builder module."""

from openshift_metrics.query_builder import QueryBuilder


class TestQueryBuilder:
    def test_build_cpu_request_standalone(self):
        query = QueryBuilder("standalone", "test-cluster-id").build_cpu_request()
        assert 'kube_pod_resource_request{resource="cpu", node!=""' in query
        assert "_id=" not in query

    def test_build_cpu_request_hypershift(self):
        query = QueryBuilder("hypershift", "test-cluster-id").build_cpu_request()
        assert 'kube_pod_resource_request{resource="cpu", node!=""' in query
        assert '_id="test-cluster-id"' in query

    def test_build_memory_request_standalone(self):
        query = QueryBuilder("standalone", "test-cluster-id").build_memory_request()
        assert 'kube_pod_resource_request{resource="memory", node!=""' in query
        assert "_id=" not in query

    def test_build_memory_request_hypershift(self):
        query = QueryBuilder("hypershift", "test-cluster-id").build_memory_request()
        assert 'kube_pod_resource_request{resource="memory", node!=""' in query
        assert '_id="test-cluster-id"' in query

    def test_build_gpu_request_standalone(self):
        query = QueryBuilder("standalone", "test-cluster-id").build_gpu_request()
        assert 'kube_pod_resource_request{resource=~"nvidia.com.*", node!=""' in query
        assert "_id=" not in query

    def test_build_gpu_request_hypershift(self):
        query = QueryBuilder("hypershift", "test-cluster-id").build_gpu_request()
        assert 'kube_pod_resource_request{resource=~"nvidia.com.*", node!=""' in query
        assert '_id="test-cluster-id"' in query

    def test_build_kube_node_labels(self):
        query = QueryBuilder.build_kube_node_labels()
        assert query == 'kube_node_labels{label_nvidia_com_gpu_product!=""}'

    def test_build_kube_pod_labels(self):
        query = QueryBuilder.build_kube_pod_labels()
        assert query == 'kube_pod_labels{label_nerc_mghpcc_org_class!=""}'
