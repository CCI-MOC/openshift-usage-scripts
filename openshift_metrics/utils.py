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

"""Holds bunch of utility functions"""

import csv
import boto3
import logging

from openshift_metrics import invoice
from openshift_metrics.config import (
    S3_ENDPOINT_URL,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
)
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmptyResultError(Exception):
    """Raise when no results are retrieved for a query"""


def upload_to_s3(file, bucket, location):
    if not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
        raise Exception(
            "Must provide S3_OUTPUT_ACCESS_KEY_ID and"
            " S3_OUTPUT_SECRET_ACCESS_KEY environment variables."
        )
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    )
    logger.info(f"Uploading {file} to s3://{bucket}/{location}")
    s3.upload_file(file, Bucket=bucket, Key=location)


def csv_writer(rows, file_name):
    """Writes rows as csv to file_name"""
    logger.info(f"Writing report to {file_name}")
    with open(file_name, "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(rows)


def write_metrics_by_namespace(
    condensed_metrics_dict,
    file_name,
    report_metadata: invoice.ReportMetadata,
    rates,
    su_definitions,
    ignore_hours=None,
):
    """
    Process metrics dictionary to aggregate usage by namespace and then write that to a file
    """
    invoices = {}
    rows = []
    headers = [
        "Invoice Month",
        "Report Start Time",
        "Report End Time",
        "Project - Allocation",
        "Project - Allocation ID",
        "Manager (PI)",
        "Cluster Name",
        "Invoice Email",
        "Invoice Address",
        "Institution",
        "Institution - Specific Code",
        "SU Hours (GBhr or SUhr)",
        "SU Type",
        "Rate",
        "Cost",
        "Generated At",
    ]

    rows.append(headers)

    for namespace, pods in condensed_metrics_dict.items():
        if namespace not in invoices:
            project_invoice = invoice.ProjectInvoce(
                project=namespace,
                project_id=namespace,
                rates=rates,
                su_definitions=su_definitions,
                ignore_hours=ignore_hours,
            )
            invoices[namespace] = project_invoice

        project_invoice = invoices[namespace]

        for pod, pod_dict in pods.items():
            for epoch_time, pod_metric_dict in pod_dict["metrics"].items():
                pod_obj = invoice.Pod(
                    pod_name=pod,
                    namespace=namespace,
                    start_time=epoch_time,
                    duration=pod_metric_dict["duration"],
                    cpu_request=Decimal(pod_metric_dict.get("cpu_request", 0)),
                    gpu_request=Decimal(pod_metric_dict.get("gpu_request", 0)),
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0))
                    / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node"),
                    node_model=pod_metric_dict.get("node_model"),
                )
                project_invoice.add_pod(pod_obj)

    for project_invoice in invoices.values():
        rows.extend(project_invoice.generate_invoice_rows(report_metadata))

    csv_writer(rows, file_name)


def write_metrics_by_pod(
    condensed_metrics_dict, file_name, su_definitions, ignore_hours=None
):
    """
    Generates metrics report by pod.
    """
    rows = []
    headers = [
        "Namespace",
        "Pod Start Time",
        "Pod End Time",
        "Duration (Hours)",
        "Pod Name",
        "CPU Request",
        "GPU Request",
        "GPU Type",
        "GPU Resource",
        "Node",
        "Node Model",
        "Memory Request (GiB)",
        "Determining Resource",
        "SU Type",
        "SU Count",
    ]
    rows.append(headers)

    for namespace, pods in condensed_metrics_dict.items():
        for pod_name, pod_dict in pods.items():
            pod_metrics_dict = pod_dict["metrics"]
            for epoch_time, pod_metric_dict in pod_metrics_dict.items():
                pod_obj = invoice.Pod(
                    pod_name=pod_name,
                    namespace=namespace,
                    start_time=epoch_time,
                    duration=pod_metric_dict["duration"],
                    cpu_request=Decimal(pod_metric_dict.get("cpu_request", 0)),
                    gpu_request=Decimal(pod_metric_dict.get("gpu_request", 0)),
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0))
                    / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node", "Unknown Node"),
                    node_model=pod_metric_dict.get("node_model", "Unknown Model"),
                )
                rows.append(pod_obj.generate_pod_row(ignore_hours, su_definitions))

    csv_writer(rows, file_name)


def write_metrics_by_classes(
    condensed_metrics_dict,
    file_name,
    report_metadata: invoice.ReportMetadata,
    rates,
    namespaces_with_classes,
    su_definitions,
    ignore_hours=None,
):
    """
    Process metrics dictionary to aggregate usage by the class label.

    If a pod has a class label, then the project name is composed of namespace:class_name
    otherwise it's namespace:noclass.
    """
    invoices = {}
    rows = []
    headers = [
        "Invoice Month",
        "Report Start Time",
        "Report End Time",
        "Project - Allocation",
        "Project - Allocation ID",
        "Manager (PI)",
        "Cluster Name",
        "Invoice Email",
        "Invoice Address",
        "Institution",
        "Institution - Specific Code",
        "SU Hours (GBhr or SUhr)",
        "SU Type",
        "Rate",
        "Cost",
        "Generated At",
    ]

    rows.append(headers)

    for namespace, pods in condensed_metrics_dict.items():
        if namespace not in namespaces_with_classes:
            continue

        for pod, pod_dict in pods.items():
            class_name = pod_dict.get("label_nerc_mghpcc_org_class")
            if class_name:
                project_name = f"{namespace}:{class_name}"
            else:
                project_name = f"{namespace}:noclass"

            if project_name not in invoices:
                project_invoice = invoice.ProjectInvoce(
                    project=project_name,
                    project_id=project_name,
                    su_definitions=su_definitions,
                    rates=rates,
                    ignore_hours=ignore_hours,
                )
                invoices[project_name] = project_invoice
            project_invoice = invoices[project_name]

            for epoch_time, pod_metric_dict in pod_dict["metrics"].items():
                pod_obj = invoice.Pod(
                    pod_name=pod,
                    namespace=project_name,
                    start_time=epoch_time,
                    duration=pod_metric_dict["duration"],
                    cpu_request=Decimal(pod_metric_dict.get("cpu_request", 0)),
                    gpu_request=Decimal(pod_metric_dict.get("gpu_request", 0)),
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0))
                    / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node"),
                    node_model=pod_metric_dict.get("node_model"),
                )
                project_invoice.add_pod(pod_obj)

    for project_invoice in invoices.values():
        rows.extend(project_invoice.generate_invoice_rows(report_metadata))

    csv_writer(rows, file_name)
