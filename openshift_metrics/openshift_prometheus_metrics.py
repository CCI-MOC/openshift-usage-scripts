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
import os
import sys
import json

from openshift_metrics import utils
from openshift_metrics.prometheus_client import PrometheusClient


CPU_REQUEST = 'kube_pod_resource_request{unit="cores"} unless on(pod, namespace) kube_pod_status_unschedulable'
MEMORY_REQUEST = 'kube_pod_resource_request{unit="bytes"} unless on(pod, namespace) kube_pod_status_unschedulable'

# For GPU requests, we don't need to exclude unscheduled pods because the join on node will eliminate those as unscheduled
# pods don't have a node value
GPU_REQUEST = 'kube_pod_resource_request{resource=~"nvidia.com.*"} * on(node) group_left(label_nvidia_com_gpu_product, label_nvidia_com_gpu_machine) kube_node_labels'


def main():
    """This method kick starts the process of collecting and saving the metrics"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--openshift-url",
        help="OpenShift Prometheus URL",
        default=os.getenv("OPENSHIFT_PROMETHEUS_URL"),
    )
    parser.add_argument(
        "--report-start-date",
        help="report date (ex: 2022-03-14)",
        default=(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    )
    parser.add_argument(
        "--report-end-date",
        help="report date (ex: 2022-03-14)",
        default=(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    )
    parser.add_argument(
        "--upload-to-s3",
        action="store_true"
    )
    parser.add_argument("--output-file")

    args = parser.parse_args()
    if not args.openshift_url:
        sys.exit("Must specify --openshift-url or set OPENSHIFT_PROMETHEUS_URL in your environment")
    openshift_url = args.openshift_url

    report_start_date = args.report_start_date
    report_end_date = args.report_end_date

    report_length = (datetime.strptime(report_end_date, "%Y-%m-%d") - datetime.strptime(report_start_date, "%Y-%m-%d"))
    assert report_length.days >= 0, "report_start_date cannot be after report_end_date"

    if args.output_file:
        output_file = args.output_file
    elif report_start_date == report_end_date:
        output_file = f"metrics-{report_start_date}.json"
    else:
        output_file = f"metrics-{report_start_date}-to-{report_end_date}.json"

    print(f"Generating report starting {report_start_date} and ending {report_end_date} in {output_file}")


    token = os.environ.get("OPENSHIFT_TOKEN")
    prom_client = PrometheusClient(openshift_url, token)

    metrics_dict = {}
    metrics_dict["start_date"] = report_start_date
    metrics_dict["end_date"] = report_end_date

    cpu_request_metrics = prom_client.query_metric(
        CPU_REQUEST, report_start_date, report_end_date
    )
    memory_request_metrics = prom_client.query_metric(
        MEMORY_REQUEST, report_start_date, report_end_date
    )
    metrics_dict["cpu_metrics"] = cpu_request_metrics
    metrics_dict["memory_metrics"] = memory_request_metrics

    # because if nobody requests a GPU then we will get an empty set
    try:
        gpu_request_metrics = prom_client.query_metric(
            GPU_REQUEST, report_start_date, report_end_date
        )
        metrics_dict["gpu_metrics"] = gpu_request_metrics
    except utils.EmptyResultError:
        pass

    month_year = datetime.strptime(report_start_date, "%Y-%m-%d").strftime("%Y-%m")
    directory_name = f"data_{month_year}"

    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    output_file = os.path.join(directory_name, output_file)

    with open(output_file, "w") as file:
        json.dump(metrics_dict, file)

    if args.upload_to_s3:
        utils.upload_to_s3(output_file, "openshift-metrics", output_file)


if __name__ == "__main__":
    main()
