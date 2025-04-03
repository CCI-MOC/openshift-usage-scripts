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

import os
import csv
import requests
import boto3
import logging

from openshift_metrics import invoice
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmptyResultError(Exception):
    """Raise when no results are retrieved for a query"""


class ColdFrontClient(object):

    def __init__(self, keycloak_url, keycloak_client_id, keycloak_client_secret):
        self.session = self.get_session(keycloak_url,
                                        keycloak_client_id,
                                        keycloak_client_secret)

    @staticmethod
    def get_session(keycloak_url, keycloak_client_id, keycloak_client_secret):
        """Authenticate as a client with Keycloak to receive an access token."""
        token_url = f"{keycloak_url}/auth/realms/mss/protocol/openid-connect/token"

        r = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=requests.auth.HTTPBasicAuth(keycloak_client_id, keycloak_client_secret),
        )
        client_token = r.json()["access_token"]

        session = requests.session()
        headers = {
            "Authorization": f"Bearer {client_token}",
            "Content-Type": "application/json",
        }
        session.headers.update(headers)
        return session


def upload_to_s3(file, bucket, location):
    s3_endpoint = os.getenv("S3_OUTPUT_ENDPOINT_URL",
                            "https://s3.us-east-005.backblazeb2.com")
    s3_key_id = os.getenv("S3_OUTPUT_ACCESS_KEY_ID")
    s3_secret = os.getenv("S3_OUTPUT_SECRET_ACCESS_KEY")

    if not s3_key_id or not s3_secret:
        raise Exception("Must provide S3_OUTPUT_ACCESS_KEY_ID and"
                        " S3_OUTPUT_SECRET_ACCESS_KEY environment variables.")
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_key_id,
        aws_secret_access_key=s3_secret,
    )
    logger.info(f"Uploading {file} to s3://{bucket}/{location}")
    response = s3.upload_file(file, Bucket=bucket, Key=location)


def get_namespace_attributes():
    """
    Returns allocation attributes from coldfront associated
    with all projects/namespaces.

    Used for finding coldfront PI name and institution ID.
    """
    client = ColdFrontClient(
        "https://keycloak.mss.mghpcc.org",
        os.environ.get("CLIENT_ID"),
        os.environ.get("CLIENT_SECRET")
    )

    coldfront_url = os.environ.get("COLDFRONT_URL",
        "https://coldfront.mss.mghpcc.org/api/allocations?all=true")
    responses = client.session.get(coldfront_url)

    namespaces_dict = {}

    for response in responses.json():
        project_name = response["attributes"].get("Allocated Project Name")
        cf_pi = response["project"].get("pi", project_name)
        cf_project_id = response["project"].get("id", 0)
        institution_code = response["attributes"].get("Institution-Specific Code", "")
        namespaces_dict[project_name] = { "cf_pi": cf_pi, "cf_project_id": cf_project_id, "institution_code": institution_code }

    return namespaces_dict


def csv_writer(rows, file_name):
    """Writes rows as csv to file_name"""
    logger.info(f"Writing report to {file_name}")
    with open(file_name, "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(rows)


def write_metrics_by_namespace(condensed_metrics_dict, file_name, report_month, rates, su_definitions, ignore_hours=None):
    """
    Process metrics dictionary to aggregate usage by namespace and then write that to a file
    """
    invoices = {}
    rows = []
    namespace_annotations = get_namespace_attributes()
    headers = [
        "Invoice Month",
        "Project - Allocation",
        "Project - Allocation ID",
        "Manager (PI)",
        "Invoice Email",
        "Invoice Address",
        "Institution",
        "Institution - Specific Code",
        "SU Hours (GBhr or SUhr)",
        "SU Type",
        "Rate",
        "Cost",
    ]

    rows.append(headers)

    for namespace, pods in condensed_metrics_dict.items():
        namespace_annotation_dict = namespace_annotations.get(namespace, {})
        cf_pi = namespace_annotation_dict.get("cf_pi")
        cf_institution_code = namespace_annotation_dict.get("institution_code", "")

        if namespace not in invoices:
            project_invoice = invoice.ProjectInvoce(
                invoice_month=report_month,
                project=namespace,
                project_id=namespace,
                pi=cf_pi,
                invoice_email="",
                invoice_address="",
                intitution="",
                institution_specific_code=cf_institution_code,
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
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0)) / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node"),
                    node_model=pod_metric_dict.get("node_model"),
                )
                project_invoice.add_pod(pod_obj)

    for project_invoice in invoices.values():
        rows.extend(project_invoice.generate_invoice_rows(report_month))

    csv_writer(rows, file_name)


def write_metrics_by_pod(condensed_metrics_dict, file_name, su_definitions, ignore_hours=None):
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
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0)) / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node", "Unknown Node"),
                    node_model=pod_metric_dict.get("node_model", "Unknown Model"),
                )
                rows.append(pod_obj.generate_pod_row(ignore_hours, su_definitions))

    csv_writer(rows, file_name)

def write_metrics_by_classes(condensed_metrics_dict, file_name, report_month, rates, namespaces_with_classes, su_definitions, ignore_hours=None):
    """
    Process metrics dictionary to aggregate usage by the class label.

    If a pod has a class label, then the project name is composed of namespace:class_name
    otherwise it's namespace:noclass.
    """
    invoices = {}
    rows = []
    headers = [
        "Invoice Month",
        "Project - Allocation",
        "Project - Allocation ID",
        "Manager (PI)",
        "Invoice Email",
        "Invoice Address",
        "Institution",
        "Institution - Specific Code",
        "SU Hours (GBhr or SUhr)",
        "SU Type",
        "Rate",
        "Cost",
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
                    invoice_month=report_month,
                    project=project_name,
                    project_id=project_name,
                    pi="",
                    invoice_email="",
                    invoice_address="",
                    intitution="",
                    institution_specific_code="",
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
                    memory_request=Decimal(pod_metric_dict.get("memory_request", 0)) / 2**30,
                    gpu_type=pod_metric_dict.get("gpu_type"),
                    gpu_resource=pod_metric_dict.get("gpu_resource"),
                    node_hostname=pod_metric_dict.get("node"),
                    node_model=pod_metric_dict.get("node_model"),
                )
                project_invoice.add_pod(pod_obj)

    for project_invoice in invoices.values():
        rows.extend(project_invoice.generate_invoice_rows(report_month))

    csv_writer(rows, file_name)
