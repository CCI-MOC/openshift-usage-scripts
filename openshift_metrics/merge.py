"""
Merges metrics from files and produces reports by pod and by namespace
"""

import argparse
from datetime import datetime
import json

from openshift_metrics import utils
from openshift_metrics.metrics_processor import MetricsProcessor

def compare_dates(date_str1, date_str2):
    """Returns true is date1 is earlier than date2"""
    date1 = datetime.strptime(date_str1, "%Y-%m-%d")
    date2 = datetime.strptime(date_str2, "%Y-%m-%d")
    return date1 < date2


def main():
    """Reads the metrics from files and generates the reports"""
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument("--output-file")
    parser.add_argument(
        "--upload-to-s3",
        action="store_true"
    )
    args = parser.parse_args()
    files = args.files

    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f"{datetime.today().strftime('%Y-%m-%d')}.csv"

    report_start_date = None
    report_end_date = None

    processor = MetricsProcessor()

    for file in files:
        with open(file, "r") as jsonfile:
            metrics_from_file = json.load(jsonfile)
            cpu_request_metrics = metrics_from_file["cpu_metrics"]
            memory_request_metrics = metrics_from_file["memory_metrics"]
            gpu_request_metrics = metrics_from_file.get("gpu_metrics", None)
            processor.merge_metrics("cpu_request", cpu_request_metrics)
            processor.merge_metrics("memory_request", memory_request_metrics)
            if gpu_request_metrics is not None:
                processor.merge_metrics("gpu_request", gpu_request_metrics)

            if report_start_date is None:
                report_start_date = metrics_from_file["start_date"]
            elif compare_dates(metrics_from_file["start_date"], report_start_date):
                report_start_date = metrics_from_file["start_date"]

            if report_end_date is None:
                report_end_date = metrics_from_file["end_date"]
            elif compare_dates(report_end_date, metrics_from_file["end_date"]):
                report_end_date = metrics_from_file["end_date"]

    print(report_start_date)
    print(report_end_date)
    report_start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
    report_end_date = datetime.strptime(report_end_date, "%Y-%m-%d")

    report_month = datetime.strftime(report_start_date, "%Y-%m")

    if report_start_date.month != report_end_date.month:
        print("Warning: The report spans multiple months")
        report_month += " to " + datetime.strftime(report_end_date, "%Y-%m")

    condensed_metrics_dict = processor.condense_metrics(
        ["cpu_request", "memory_request", "gpu_request", "gpu_type"]
    )
    utils.write_metrics_by_namespace(
        condensed_metrics_dict,
        output_file,
        report_month
    )
    utils.write_metrics_by_pod(condensed_metrics_dict, "pod-" + output_file)

    if args.upload_to_s3:
        primary_location = (
            f"Invoices/{report_month}/"
            f"Service Invoices/NERC OpenShift {report_month}.csv"
        )
        utils.upload_to_s3(output_file, "nerc-invoicing", primary_location)

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        secondary_location = (
            f"Invoices/{report_month}/"
            f"Archive/NERC OpenShift {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3(output_file, "nerc-invoicing", secondary_location)
        pod_report = (
            f"Invoices/{report_month}/"
            f"Archive/Pod-NERC OpenShift {report_month} {timestamp}.csv"
        )
        utils.upload_to_s3("pod-" + output_file, "nerc-invoicing", pod_report)

if __name__ == "__main__":
    main()
