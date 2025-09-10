"""
Config for the openshift metrics.
All values are set in the .env file
All variables in the .env file are lexicographically identical to the python variables below
"""

import os
from datetime import datetime, timedelta

# =============================================================================
# HARDCODED CONSTANTS (rarely change, application-specific)
# =============================================================================

# Prometheus query strings
PROMETHEUS_QUERIES = {
    "CPU_REQUEST": 'kube_pod_resource_request{resource="cpu", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable',
    "MEMORY_REQUEST": 'kube_pod_resource_request{resource="memory", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable',
    "GPU_REQUEST": 'kube_pod_resource_request{resource=~"nvidia.com.*", node!=""} unless on(pod, namespace) kube_pod_status_unschedulable',
    "KUBE_NODE_LABELS": 'kube_node_labels{label_nvidia_com_gpu_product!=""}',
    "KUBE_POD_LABELS": 'kube_pod_labels{label_nerc_mghpcc_org_class!=""}',
}

# Cluster name mappings
CLUSTER_NAME_MAPPING = {
    "https://thanos-querier-openshift-monitoring.apps.shift.nerc.mghpcc.org": "ocp-prod",
    "https://thanos-querier-openshift-monitoring.apps.ocp-test.nerc.mghpcc.org": "ocp-test",
    "https://thanos-querier-openshift-monitoring.apps.edu.nerc.mghpcc.org": "academic",
}

# Default values for empty fields
DEFAULT_VALUES = {
    "UNKNOWN_NODE": "Unknown Node",
    "UNKNOWN_MODEL": "Unknown Model",
    "EMPTY_STRING": "",
}

# =============================================================================
# BUSINESS LOGIC CONSTANTS
# =============================================================================
# Note: Business logic constants (GPU types, SU types, etc.) are now in constants.py
# This file only contains truly configurable values that change between deployments

# =============================================================================
# INFRASTRUCTURE CONFIGURATION
# =============================================================================

# OpenShift/Prometheus
OPENSHIFT_PROMETHEUS_URL = os.getenv("OPENSHIFT_PROMETHEUS_URL")
OPENSHIFT_TOKEN = os.getenv("OPENSHIFT_TOKEN")

# S3 Configuration
S3_ENDPOINT_URL = os.getenv(
    "S3_OUTPUT_ENDPOINT_URL", "https://s3.us-east-005.backblazeb2.com"
)
S3_ACCESS_KEY_ID = os.getenv("S3_OUTPUT_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_OUTPUT_SECRET_ACCESS_KEY")
S3_INVOICE_BUCKET = os.getenv("S3_INVOICE_BUCKET", "nerc-invoicing")
S3_METRICS_BUCKET = os.getenv("S3_METRICS_BUCKET", "openshift_metrics")

# =============================================================================
# PROCESSING CONFIGURATION
# =============================================================================

# Metrics processing
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "15"))
STEP_MINUTES = int(os.getenv("STEP_MINUTES", "15"))
GPU_MAPPING_FILE = os.getenv("GPU_MAPPING_FILE", "gpu_node_map.json")

# HTTP retry configuration
HTTP_RETRY_CONFIG = {
    "total": int(os.getenv("HTTP_RETRY_TOTAL", "3")),
    "backoff_factor": int(os.getenv("HTTP_RETRY_BACKOFF_FACTOR", "1")),
    "status_forcelist": [429, 500, 502, 503, 504],
}

# =============================================================================
# REPORT CONFIGURATION (formerly CLI arguments)
# =============================================================================

# Report dates (with defaults)
REPORT_START_DATE = os.getenv(
    "REPORT_START_DATE", (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
)
REPORT_END_DATE = os.getenv(
    "REPORT_END_DATE", (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
)

# Upload configuration
UPLOAD_TO_S3 = os.getenv("UPLOAD_TO_S3", "false").lower() == "true"

# File configuration
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
INVOICE_FILE = os.getenv("INVOICE_FILE")
POD_REPORT_FILE = os.getenv("POD_REPORT_FILE")
CLASS_INVOICE_FILE = os.getenv("CLASS_INVOICE_FILE")

# Ignore hours configuration (comma-separated timestamp ranges)
IGNORE_HOURS = os.getenv("IGNORE_HOURS", "")

# =============================================================================
# RATES AND BILLING CONFIGURATION
# =============================================================================

# Rate source configuration
USE_NERC_RATES = os.getenv("USE_NERC_RATES", "false").lower() == "true"

# Individual rates (Decimal values)
RATE_CPU_SU = os.getenv("RATE_CPU_SU")
RATE_GPU_V100_SU = os.getenv("RATE_GPU_V100_SU")
RATE_GPU_A100SXM4_SU = os.getenv("RATE_GPU_A100SXM4_SU")
RATE_GPU_A100_SU = os.getenv("RATE_GPU_A100_SU")
RATE_GPU_H100_SU = os.getenv("RATE_GPU_H100_SU")

# Legacy rates dictionary (for backward compatibility)
# Note: This would need to import constants if used, but it's marked as legacy
RATES = {
    # "NVIDIA-A100-40GB": Decimal(os.getenv("GPU_A100_RATE")) if os.getenv("GPU_A100_RATE") else None,
}

# =============================================================================
# BUSINESS LOGIC CONFIGURATION
# =============================================================================

# Namespaces that support class-based reporting
NAMESPACES_WITH_CLASSES = os.getenv("NAMESPACES_WITH_CLASSES", "rhods-notebooks").split(
    ","
)

# Default filename patterns
DEFAULT_FILENAME_PATTERNS = {
    "INVOICE_FILE": "NERC OpenShift {report_month}.csv",
    "POD_REPORT_FILE": "Pod NERC OpenShift {report_month}.csv",
    "CLASS_INVOICE_FILE": "NERC OpenShift Classes {report_month}.csv",
    "OUTPUT_FILE_SINGLE": "metrics-{report_date}.json",
    "OUTPUT_FILE_RANGE": "metrics-{start_date}-to-{end_date}.json",
}
