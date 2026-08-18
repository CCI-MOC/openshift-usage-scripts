#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.
#

"""Collect and save metrics from prometheus"""

import argparse
from datetime import datetime, timedelta
import sys
import json
import logging

from openshift_metrics import utils
from openshift_metrics.prometheus_client import PrometheusClient
from openshift_metrics.metrics_processor import MetricsProcessor
from openshift_metrics.config import (
    OPENSHIFT_PROMETHEUS_URL,
    OPENSHIFT_TOKEN,
    S3_METRICS_BUCKET,
    PROM_QUERY_INTERVAL_MINUTES,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CPU_REQUEST = 'kube_pod_resource_request{resource="cpu", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable'
MEMORY_REQUEST = 'kube_pod_resource_request{resource="memory", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable'
GPU_REQUEST = 'kube_pod_resource_request{resource=~"nvidia.com.*", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable'
KUBE_NODE_LABELS = 'kube_node_labels{label_nvidia_com_gpu_product!=""}'
KUBE_POD_LABELS = 'kube_pod_labels{label_nerc_mghpcc_org_class!=""}'

URL_CLUSTER_NAME_MAPPING = {
    "https://thanos-querier-openshift-monitoring.apps.shift.nerc.mghpcc.org": "ocp-prod",
    "https://thanos-querier-openshift-monitoring.apps.ocp-test.nerc.mghpcc.org": "ocp-test",
    "https://thanos-querier-openshift-monitoring.apps.edu.nerc.mghpcc.org": "academic",
    "https://thanos-querier-openshift-monitoring.apps.barcelona.nerc.mghpcc.org": "barcelona",
}


def main():
    """This method kick starts the process of collecting and saving the metrics"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--openshift-url",
        help="OpenShift Prometheus URL",
        default=OPENSHIFT_PROMETHEUS_URL,
    )
    parser.add_argument(
        "--report-start-date",
        help="report date (ex: 2022-03-14)",
        default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "--report-end-date",
        help="report date (ex: 2022-03-14)",
        default=(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    parser.add_argument("--upload-to-s3", action="store_true")
    parser.add_argument("--output-file")

    args = parser.parse_args()
    if not args.openshift_url:
        sys.exit(
            "Must specify --openshift-url or set OPENSHIFT_PROMETHEUS_URL in your environment"
        )
    openshift_url = args.openshift_url

    report_start_date = args.report_start_date
    report_end_date = args.report_end_date

    report_length = datetime.strptime(report_end_date, "%Y-%m-%d") - datetime.strptime(
        report_start_date, "%Y-%m-%d"
    )
    assert report_length.days >= 0, "report_start_date cannot be after report_end_date"

    if args.output_file:
        output_file = args.output_file
    elif report_start_date == report_end_date:
        output_file = f"metrics-{report_start_date}.json"
    else:
        output_file = f"metrics-{report_start_date}-to-{report_end_date}.json"

    logger.info(
        f"Generating report starting {report_start_date} and ending {report_end_date} in {output_file} with interval {PROM_QUERY_INTERVAL_MINUTES} minute"
    )

    prom_client = PrometheusClient(
        openshift_url, OPENSHIFT_TOKEN, PROM_QUERY_INTERVAL_MINUTES
    )

    metrics_dict = {}
    metrics_dict["start_date"] = report_start_date
    metrics_dict["end_date"] = report_end_date
    metrics_dict["interval_minutes"] = PROM_QUERY_INTERVAL_MINUTES
    metrics_dict["cluster_name"] = URL_CLUSTER_NAME_MAPPING.get(
        args.openshift_url, args.openshift_url
    )

    interval_seconds = PROM_QUERY_INTERVAL_MINUTES * 60

    cpu_request_metrics = prom_client.query_metric(
        CPU_REQUEST, report_start_date, report_end_date
    )

    try:
        pod_labels = prom_client.query_metric(
            KUBE_POD_LABELS, report_start_date, report_end_date
        )
        metrics_dict["cpu_metrics"] = MetricsProcessor.insert_pod_labels(
            pod_labels, cpu_request_metrics
        )
    except utils.EmptyResultError:
        logger.info(
            f"No pod labels found for the period {report_start_date} to {report_end_date}"
        )
        metrics_dict["cpu_metrics"] = cpu_request_metrics

    memory_request_metrics = prom_client.query_metric(
        MEMORY_REQUEST, report_start_date, report_end_date
    )
    metrics_dict["memory_metrics"] = memory_request_metrics

    # because if nobody requests a GPU then we will get an empty set
    try:
        gpu_request_metrics = prom_client.query_metric(
            GPU_REQUEST, report_start_date, report_end_date
        )
        node_labels = prom_client.query_metric(
            KUBE_NODE_LABELS, report_start_date, report_end_date
        )
        metrics_dict["gpu_metrics"] = MetricsProcessor.insert_node_labels(
            node_labels, gpu_request_metrics
        )
    except utils.EmptyResultError:
        logger.info(
            f"No GPU metrics found for the period {report_start_date} to {report_end_date}"
        )
        pass

    cpu_segments = MetricsProcessor.condense_metric_series(
        metrics_dict["cpu_metrics"], interval_seconds, "cpu_request"
    )
    mem_segments = MetricsProcessor.condense_metric_series(
        metrics_dict["memory_metrics"], interval_seconds, "memory_request"
    )
    gpu_segments = MetricsProcessor.condense_metric_series(
        metrics_dict.get("gpu_metrics") or [], interval_seconds, "gpu_request"
    )
    gpu_mapping = MetricsProcessor._load_gpu_mapping("gpu_node_map.json")
    namespaces_dict = {
        "start_date": metrics_dict["start_date"],
        "end_date": metrics_dict["end_date"],
        "interval_minutes": metrics_dict["interval_minutes"],
        "cluster_name": metrics_dict["cluster_name"],
        "namespaces": MetricsProcessor.build_namespaces_dict(
            cpu_segments, mem_segments, gpu_segments, gpu_mapping=gpu_mapping
        ),
    }

    month_year = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y-%m")

    if report_start_date == report_end_date:
        s3_location = f"data_{month_year}/metrics-{report_start_date}.json"
        s3_namespaces_location = (
            f"data_{month_year}/metrics-{report_start_date}-namespaces.json"
        )
    else:
        s3_location = (
            f"data_{month_year}/metrics-{report_start_date}-to-{report_end_date}.json"
        )
        s3_namespaces_location = f"data_{month_year}/metrics-{report_start_date}-to-{report_end_date}-namespaces.json"

    namespaces_file = (
        output_file[: -len(".json")] + "-namespaces.json"
        if output_file.endswith(".json")
        else output_file + "-namespaces.json"
    )

    with open(output_file, "w") as file:
        logger.info(f"Writing metrics to {output_file}")
        json.dump(metrics_dict, file)

    with open(namespaces_file, "w") as file:
        logger.info(f"Writing namespaces metrics to {namespaces_file}")
        json.dump(namespaces_dict, file)

    if args.upload_to_s3:
        utils.upload_to_s3(output_file, S3_METRICS_BUCKET, s3_location)
        utils.upload_to_s3(namespaces_file, S3_METRICS_BUCKET, s3_namespaces_location)


if __name__ == "__main__":
    main()
