import math
from dataclasses import dataclass, field
from collections import namedtuple
from typing import List, Tuple, Optional
from decimal import Decimal, ROUND_HALF_UP
import datetime

# GPU types
GPU_A100 = "NVIDIA-A100-40GB"
GPU_A100_SXM4 = "NVIDIA-A100-SXM4-40GB"
GPU_V100 = "Tesla-V100-PCIE-32GB"
GPU_H100 = "NVIDIA-H100-80GB-HBM3"
GPU_UNKNOWN_TYPE = "GPU_UNKNOWN_TYPE"

# GPU Resource - MIG Geometries
# A100 Strategies
MIG_1G_5GB = "nvidia.com/mig-1g.5gb"
MIG_2G_10GB = "nvidia.com/mig-2g.10gb"
MIG_3G_20GB = "nvidia.com/mig-3g.20gb"
WHOLE_GPU = "nvidia.com/gpu"

# SU Types
SU_CPU = "OpenShift CPU"
SU_A100_GPU = "OpenShift GPUA100"
SU_A100_SXM4_GPU = "OpenShift GPUA100SXM4"
SU_V100_GPU = "OpenShift GPUV100"
SU_H100_GPU = "OpenShift GPUH100"
SU_UNKNOWN_GPU = "OpenShift Unknown GPU"
SU_UNKNOWN_MIG_GPU = "OpenShift Unknown MIG GPU"
SU_UNKNOWN = "Openshift Unknown"

ServiceUnit = namedtuple("ServiceUnit", ["su_type", "su_count", "determinig_resource"])


@dataclass
class Pod:
    """Object that represents a pod"""

    pod_name: str
    namespace: str
    start_time: int
    duration: int
    cpu_request: Decimal
    gpu_request: Decimal
    memory_request: Decimal
    gpu_type: str
    gpu_resource: str
    node_hostname: str
    node_model: str

    def get_service_unit(self, su_definitions) -> ServiceUnit:
        """
        Returns the type of service unit, the count, and the determining resource
        """
        su_type = SU_UNKNOWN
        su_count = 0

        # pods that requested a specific GPU but weren't scheduled may report 0 GPU
        if self.gpu_resource is not None and self.gpu_request == 0:
            return ServiceUnit(SU_UNKNOWN_GPU, 0, "GPU")

        # pods in weird states
        if self.cpu_request == 0 or self.memory_request == 0:
            return ServiceUnit(SU_UNKNOWN, 0, "CPU")

        known_gpu_su = {
            GPU_A100: SU_A100_GPU,
            GPU_A100_SXM4: SU_A100_SXM4_GPU,
            GPU_V100: SU_V100_GPU,
            GPU_H100: SU_H100_GPU,
        }

        A100_SXM4_MIG = {
            MIG_1G_5GB: SU_UNKNOWN_MIG_GPU,
            MIG_2G_10GB: SU_UNKNOWN_MIG_GPU,
            MIG_3G_20GB: SU_UNKNOWN_MIG_GPU,
        }

        if self.gpu_resource is None and self.gpu_request == 0:
            su_type = SU_CPU
        elif self.gpu_type is not None and self.gpu_resource == WHOLE_GPU:
            su_type = known_gpu_su.get(self.gpu_type, SU_UNKNOWN_GPU)
        elif self.gpu_type == GPU_A100_SXM4:  # for MIG GPU of type A100_SXM4
            su_type = A100_SXM4_MIG.get(self.gpu_resource, SU_UNKNOWN_MIG_GPU)
        else:
            return ServiceUnit(SU_UNKNOWN_GPU, 0, "GPU")

        cpu_multiplier = self.cpu_request / int(su_definitions[su_type]["vCPUs"])
        memory_multiplier = self.memory_request / int((int(su_definitions[su_type]["RAM"])/1024))
        if int(su_definitions[su_type]["vGPUs"]) != 0:
            gpu_multiplier = self.gpu_request / int(su_definitions[su_type]["vGPUs"])
        else:
            gpu_multiplier = 0

        su_count = max(cpu_multiplier, gpu_multiplier, memory_multiplier)

        # no fractional SUs for GPU SUs
        if su_type != SU_CPU:
            su_count = math.ceil(su_count)

        if gpu_multiplier >= cpu_multiplier and gpu_multiplier >= memory_multiplier:
            determining_resource = "GPU"
        elif cpu_multiplier >= gpu_multiplier and cpu_multiplier >= memory_multiplier:
            determining_resource = "CPU"
        else:
            determining_resource = "RAM"

        return ServiceUnit(su_type, su_count, determining_resource)

    def get_runtime(
        self, ignore_times: List[Tuple[datetime.datetime, datetime.datetime]] = None
    ) -> Decimal:
        """Return runtime eligible for billing in hours"""

        total_runtime = self.duration

        if ignore_times:
            for ignore_start_date, ignore_end_date in ignore_times:
                ignore_start = int(ignore_start_date.timestamp())
                ignore_end = int(ignore_end_date.timestamp())
                if ignore_end <= self.start_time or ignore_start >= self.end_time:
                    continue
                overlap_start = max(self.start_time, ignore_start)
                overlap_end = min(self.end_time, ignore_end)

                overlap_duration = max(0, overlap_end - overlap_start)
                total_runtime = max(0, total_runtime - overlap_duration)

        return Decimal(total_runtime) / 3600

    @property
    def end_time(self) -> int:
        return self.start_time + self.duration

    def generate_pod_row(self, ignore_times, su_definitions):
        """
        This returns a row to represent pod data.
        It converts the epoch_time stamps to datetime timestamps so it's more readable.
        Additionally, some metrics are rounded for readibility.
        """
        su_type, su_count, determining_resource = self.get_service_unit(su_definitions)
        start_time = datetime.datetime.fromtimestamp(
            self.start_time, datetime.UTC
        ).strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime.datetime.fromtimestamp(
            self.end_time, datetime.UTC
        ).strftime("%Y-%m-%dT%H:%M:%S")
        memory_request = self.memory_request.quantize(
            Decimal(".0001"), rounding=ROUND_HALF_UP
        )
        runtime = self.get_runtime(ignore_times).quantize(Decimal(".0001"), rounding=ROUND_HALF_UP)
        return [
            self.namespace,
            start_time,
            end_time,
            runtime,
            self.pod_name,
            self.cpu_request,
            self.gpu_request,
            self.gpu_type,
            self.gpu_resource,
            self.node_hostname,
            self.node_model,
            memory_request,
            determining_resource,
            su_type,
            su_count,
        ]


@dataclass()
class Rates:
    cpu: Decimal
    gpu_a100: Decimal
    gpu_a100sxm4: Decimal
    gpu_v100: Decimal
    gpu_h100: Decimal


@dataclass
class ProjectInvoce:
    """Represents the invoicing data for a project."""

    invoice_month: str
    project: str
    project_id: str
    pi: str
    invoice_email: str
    invoice_address: str
    intitution: str
    institution_specific_code: str
    rates: Rates
    su_definitions: dict
    ignore_hours: Optional[List[Tuple[datetime.datetime, datetime.datetime]]] = None
    su_hours: dict = field(
        default_factory=lambda: {
            SU_CPU: 0,
            SU_A100_GPU: 0,
            SU_A100_SXM4_GPU: 0,
            SU_V100_GPU: 0,
            SU_H100_GPU: 0,
            SU_UNKNOWN_GPU: 0,
            SU_UNKNOWN_MIG_GPU: 0,
            SU_UNKNOWN: 0,
        }
    )

    def add_pod(self, pod: Pod) -> None:
        """Aggregate a pods data"""
        su_type, su_count, _ = pod.get_service_unit(self.su_definitions)
        duration_in_hours = pod.get_runtime(self.ignore_hours)
        self.su_hours[su_type] += su_count * duration_in_hours

    def get_rate(self, su_type) -> Decimal:
        if su_type == SU_CPU:
            return self.rates.cpu
        if su_type == SU_A100_GPU:
            return self.rates.gpu_a100
        if su_type == SU_A100_SXM4_GPU:
            return self.rates.gpu_a100sxm4
        if su_type == SU_V100_GPU:
            return self.rates.gpu_v100
        if su_type == SU_H100_GPU:
            return self.rates.gpu_h100
        return Decimal(0)

    def generate_invoice_rows(self, report_month) -> List[str]:
        rows = []
        for su_type, hours in self.su_hours.items():
            if hours > 0:
                hours = math.ceil(hours)
                rate = self.get_rate(su_type)
                cost = (rate * hours).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
                row = [
                    report_month,
                    self.project,
                    self.project_id,
                    self.pi,
                    self.invoice_email,
                    self.invoice_address,
                    self.intitution,
                    self.institution_specific_code,
                    hours,
                    su_type,
                    rate,
                    cost,
                ]
                rows.append(row)
        return rows
