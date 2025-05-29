import requests
import time
import logging

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from openshift_metrics.utils import EmptyResultError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PrometheusClient:
    def __init__(self, prometheus_url: str, token: str, step_min: int = 15):
        self.prometheus_url = prometheus_url
        self.token = token
        self.step_min = step_min

    def query_metric(self, metric, start_date, end_date):
        """Queries metric from the provided prometheus_url"""
        data = None
        headers = {"Authorization": f"Bearer {self.token}"}
        day_url_vars = f"start={start_date}T00:00:00Z&end={end_date}T23:59:59Z"
        url = f"{self.prometheus_url}/api/v1/query_range?query={metric}&{day_url_vars}&step={self.step_min}m"

        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retries))

        logger.info(f"Retrieving metric: {metric}")

        for _ in range(3):
            response = session.get(url, headers=headers, verify=True)

            if response.status_code != 200:
                print(f"{response.status_code} Response: {response.reason}")
            else:
                data = response.json()["data"]["result"]
                if data:
                    break
                logger.warning("Empty result set")
            time.sleep(3)

        if not data:
            raise EmptyResultError(f"Error retrieving metric: {metric}")
        return data
