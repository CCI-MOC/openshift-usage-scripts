"""Infrastructure configuration for OpenShift metrics and S3 storage."""

import os

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

# Billing Configuration
# Service Unit Names for Rate Lookup from nerc-rates
SU_NAMES = ["GPUV100", "GPUA100", "GPUA100SXM4", "GPUH100", "CPU"]
RESOURCE_NAMES = ["vCPUs", "RAM", "GPUs"]

# Rate Configuration
USE_NERC_RATES_ENV = os.getenv("USE_NERC_RATES")
USE_NERC_RATES = USE_NERC_RATES_ENV.lower() == "true" if USE_NERC_RATES_ENV else None

# Individual rates (used when USE_NERC_RATES=false)
RATE_CPU_SU = os.getenv("RATE_CPU_SU")
RATE_GPU_V100_SU = os.getenv("RATE_GPU_V100_SU")
RATE_GPU_A100SXM4_SU = os.getenv("RATE_GPU_A100SXM4_SU")
RATE_GPU_A100_SU = os.getenv("RATE_GPU_A100_SU")
RATE_GPU_H100_SU = os.getenv("RATE_GPU_H100_SU")

# Legacy rate (for backward compatibility)
GPU_A100_RATE = os.getenv("GPU_A100_RATE")

# Prometheus Query Configuration
PROM_QUERY_INTERVAL_MINUTES = int(os.getenv("PROM_QUERY_INTERVAL_MINUTES", 15))
assert PROM_QUERY_INTERVAL_MINUTES >= 1, "Query interval must be at least 1 minute"
