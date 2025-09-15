"""
Business logic constants for the openshift metrics system.

These are fixed constants that define the business logic and don't change between deployments.
For configurable values, see config.py
"""

# =============================================================================
# GPU TYPES
# =============================================================================

GPU_A100 = "NVIDIA-A100-40GB"
GPU_A100_SXM4 = "NVIDIA-A100-SXM4-40GB"
GPU_V100 = "Tesla-V100-PCIE-32GB"
GPU_H100 = "NVIDIA-H100-80GB-HBM3"
GPU_UNKNOWN_TYPE = "GPU_UNKNOWN_TYPE"

# =============================================================================
# GPU RESOURCE - MIG GEOMETRIES
# =============================================================================

MIG_1G_5GB = "nvidia.com/mig-1g.5gb"
MIG_2G_10GB = "nvidia.com/mig-2g.10gb"
MIG_3G_20GB = "nvidia.com/mig-3g.20gb"
WHOLE_GPU = "nvidia.com/gpu"

# =============================================================================
# VM GPU RESOURCES
# =============================================================================

VM_GPU_H100 = "nvidia.com/H100_SXM5_80GB"
VM_GPU_A100_SXM4 = "nvidia.com/A100_SXM4_40GB"
VM_GPU_V100 = "nvidia.com/GV100GL_Tesla_V100"

# =============================================================================
# SERVICE UNIT TYPES
# =============================================================================

SU_CPU = "OpenShift CPU"
SU_A100_GPU = "OpenShift GPUA100"
SU_A100_SXM4_GPU = "OpenShift GPUA100SXM4"
SU_V100_GPU = "OpenShift GPUV100"
SU_H100_GPU = "OpenShift GPUH100"
SU_UNKNOWN_GPU = "OpenShift Unknown GPU"
SU_UNKNOWN_MIG_GPU = "OpenShift Unknown MIG GPU"
SU_UNKNOWN = "Openshift Unknown"
