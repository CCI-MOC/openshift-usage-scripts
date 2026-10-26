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
from openshift_metrics.query_builder import QueryBuilder
from openshift_metrics.metrics_normalizer import MetricsNormalizer
from openshift_metrics.config import (
    OPENSHIFT_PROMETHEUS_URL,
    OPENSHIFT_TOKEN,
    OPENSHIFT_CLUSTER_ID,
    CLUSTER_TYPE,
    OPENSHIFT_CLUSTER_NAME,
    S3_METRICS_BUCKET,
    PROM_QUERY_INTERVAL_MINUTES,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLUSTER_TYPES = ("standalone", "hypershift")


def validate_config():
    """Exits if the configuration needed to collect metrics is missing or invalid"""
    if CLUSTER_TYPE not in CLUSTER_TYPES:
        sys.exit(f"CLUSTER_TYPE must be one of {CLUSTER_TYPES}, got {CLUSTER_TYPE!r}")
    if not OPENSHIFT_CLUSTER_ID:
        sys.exit("OPENSHIFT_CLUSTER_ID must be set in your environment")
    if not OPENSHIFT_TOKEN:
        sys.exit("OPENSHIFT_TOKEN must be set in your environment")


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
    validate_config()
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
    logger.info(f"Using cluster type: {CLUSTER_TYPE}")

    prom_client = PrometheusClient(
        openshift_url, OPENSHIFT_TOKEN, PROM_QUERY_INTERVAL_MINUTES
    )

    metrics_dict = {}
    metrics_dict["start_date"] = report_start_date
    metrics_dict["end_date"] = report_end_date
    metrics_dict["interval_minutes"] = PROM_QUERY_INTERVAL_MINUTES
    metrics_dict["cluster_id"] = OPENSHIFT_CLUSTER_ID
    metrics_dict["cluster_name"] = OPENSHIFT_CLUSTER_NAME

    # Hosted clusters don't export kube_pod_labels or kube_node_labels to the
    # management cluster, so we skip the label queries for hypershift.
    query_labels = CLUSTER_TYPE == "standalone"
    if not query_labels:
        logger.info("Skipping pod and node label queries for hypershift cluster")

    query_builder = QueryBuilder(CLUSTER_TYPE, OPENSHIFT_CLUSTER_ID)

    cpu_request_query = query_builder.build_cpu_request()
    cpu_request_metrics = prom_client.query_metric(
        cpu_request_query, report_start_date, report_end_date
    )
    cpu_request_metrics = MetricsNormalizer.normalize(cpu_request_metrics, CLUSTER_TYPE)
    metrics_dict["cpu_metrics"] = cpu_request_metrics

    if query_labels:
        try:
            pod_labels = prom_client.query_metric(
                query_builder.build_kube_pod_labels(),
                report_start_date,
                report_end_date,
            )
            metrics_dict["cpu_metrics"] = MetricsProcessor.insert_pod_labels(
                pod_labels, cpu_request_metrics
            )
        except utils.EmptyResultError:
            logger.info(
                f"No pod labels found for the period {report_start_date} to {report_end_date}"
            )

    memory_request_query = query_builder.build_memory_request()
    memory_request_metrics = prom_client.query_metric(
        memory_request_query, report_start_date, report_end_date
    )
    memory_request_metrics = MetricsNormalizer.normalize(
        memory_request_metrics, CLUSTER_TYPE
    )
    metrics_dict["memory_metrics"] = memory_request_metrics

    # because if nobody requests a GPU then we will get an empty set
    try:
        gpu_request_query = query_builder.build_gpu_request()
        gpu_request_metrics = prom_client.query_metric(
            gpu_request_query, report_start_date, report_end_date
        )
        gpu_request_metrics = MetricsNormalizer.normalize(
            gpu_request_metrics, CLUSTER_TYPE
        )
        if query_labels:
            node_labels = prom_client.query_metric(
                query_builder.build_kube_node_labels(),
                report_start_date,
                report_end_date,
            )
            gpu_request_metrics = MetricsProcessor.insert_node_labels(
                node_labels, gpu_request_metrics
            )
        metrics_dict["gpu_metrics"] = gpu_request_metrics
    except utils.EmptyResultError:
        logger.info(
            f"No GPU metrics found for the period {report_start_date} to {report_end_date}"
        )

    month_year = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y-%m")

    if report_start_date == report_end_date:
        s3_location = f"data_{month_year}/metrics-{report_start_date}.json"
    else:
        s3_location = (
            f"data_{month_year}/metrics-{report_start_date}-to-{report_end_date}.json"
        )

    with open(output_file, "w") as file:
        logger.info(f"Writing metrics to {output_file}")
        json.dump(metrics_dict, file)

    if args.upload_to_s3:
        utils.upload_to_s3(output_file, S3_METRICS_BUCKET, s3_location)


if __name__ == "__main__":
    main()
