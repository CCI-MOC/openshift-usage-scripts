from requests.exceptions import ConnectionError
from unittest import TestCase, mock

from openshift_metrics.prometheus_client import PrometheusClient


class TestQueryMetric(TestCase):
    @mock.patch("requests.Session.get")
    @mock.patch("time.sleep")
    def test_query_metric(self, mock_sleep, mock_get):
        mock_response = mock.Mock(status_code=200)
        mock_response.json.return_value = {"data": {"result": "this is data"}}
        mock_get.return_value = mock_response
        prom_client = PrometheusClient("https://fake-url", "fake-token")
        metrics = prom_client.query_metric("fake-metric", "2022-03-14", "2022-03-14")
        self.assertEqual(metrics, "this is data")
        self.assertEqual(mock_get.call_count, 1)

    @mock.patch("requests.Session.get")
    @mock.patch("time.sleep")
    def test_query_metric_exception(self, mock_sleep, mock_get):
        mock_get.return_value = mock.Mock(status_code=404)
        prom_client = PrometheusClient("https://fake-url", "fake-token")
        self.assertRaises(
            Exception,
            prom_client.query_metric,
            "fake-metric",
            "2022-03-14",
            "2022-03-14",
        )
        self.assertEqual(mock_get.call_count, 3)

    @mock.patch("requests.Session.get")
    @mock.patch("time.sleep")
    def test_query_metric_connection_error(self, mock_sleep, mock_get):
        mock_get.side_effect = [ConnectionError]
        prom_client = PrometheusClient("https://fake-url", "fake-token")
        self.assertRaises(
            ConnectionError,
            prom_client.query_metric,
            "fake-metric",
            "2022-03-14",
            "2022-03-14",
        )
        self.assertEqual(mock_get.call_count, 1)
