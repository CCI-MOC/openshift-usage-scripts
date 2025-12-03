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
import tempfile
from unittest import TestCase
from decimal import Decimal

from openshift_metrics import utils, invoice, merge
from datetime import datetime, UTC

RATES = invoice.Rates(
    cpu=Decimal("0.013"),
    gpu_a100sxm4=Decimal("2.078"),
    gpu_a100=Decimal("1.803"),
    gpu_v100=Decimal("1.214"),
    gpu_h100=Decimal("6.04"),
)

SU_DEFINITIONS = merge.get_su_definitions("2025-04")


class TestWriteMetricsByPod(TestCase):
    def test_write_metrics_log(self):
        test_metrics_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu_request": 10,
                            "memory_request": 1048576,
                            "duration": 120,
                            "node": "wrk-1",
                            "node_model": "Dell",
                        },
                        120: {
                            "cpu_request": 20,
                            "memory_request": 1048576,
                            "duration": 60,
                            "node": "wrk-2",
                            "node_model": "Lenovo",
                        },
                    }
                },
                "pod2": {
                    "metrics": {
                        0: {
                            "cpu_request": 20,
                            "memory_request": 10485760,
                            "duration": 60,
                        },
                        60: {
                            "cpu_request": 25,
                            "memory_request": 10485760,
                            "duration": 60,
                        },
                        120: {
                            "cpu_request": 20,
                            "memory_request": 10485760,
                            "duration": 60,
                        },
                    }
                },
            },
            "namespace2": {
                "pod3": {
                    "metrics": {
                        0: {
                            "cpu_request": 45,
                            "memory_request": 104857600,
                            "duration": 180,
                        },
                    }
                },
                "pod4": {  # this results in 0.5 SU
                    "metrics": {
                        0: {
                            "cpu_request": 0.5,
                            "memory_request": 2147483648,
                            "duration": 3600,
                        },
                    }
                },
            },
        }

        expected_output = (
            "Namespace,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,GPU Resource,Node,Node Model,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
            "namespace1,1970-01-01T00:00:00,1970-01-01T00:02:00,0.0333,pod1,10,0,,,wrk-1,Dell,0.0010,CPU,OpenShift CPU,10\n"
            "namespace1,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod1,20,0,,,wrk-2,Lenovo,0.0010,CPU,OpenShift CPU,20\n"
            "namespace1,1970-01-01T00:00:00,1970-01-01T00:01:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
            "namespace1,1970-01-01T00:01:00,1970-01-01T00:02:00,0.0167,pod2,25,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,25\n"
            "namespace1,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
            "namespace2,1970-01-01T00:00:00,1970-01-01T00:03:00,0.0500,pod3,45,0,,,Unknown Node,Unknown Model,0.0977,CPU,OpenShift CPU,45\n"
            "namespace2,1970-01-01T00:00:00,1970-01-01T01:00:00,1.0000,pod4,0.5,0,,,Unknown Node,Unknown Model,2.0000,CPU,OpenShift CPU,0.5\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_pod(test_metrics_dict, tmp.name, SU_DEFINITIONS)
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsByNamespace(TestCase):
    def setUp(self) -> None:
        self.report_metadata = invoice.ReportMetadata(
            report_month="2023-01",
            cluster_name="test-cluster",
            report_start_time=datetime(2023, 1, 1, tzinfo=UTC),
            report_end_time=datetime(2023, 1, 3, tzinfo=UTC),
            generated_at=datetime(2023, 1, 5, tzinfo=UTC),
        )

    def test_write_metrics_log(self):
        test_metrics_dict = {
            "namespace1": {
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu_request": 2,
                            "memory_request": 4 * 2**30,
                            "duration": 43200,
                        },
                        43200: {
                            "cpu_request": 4,
                            "memory_request": 4 * 2**30,
                            "duration": 43200,
                        },
                    }
                },
                "pod2": {
                    "metrics": {
                        0: {
                            "cpu_request": 4,
                            "memory_request": 1 * 2**30,
                            "duration": 86400,
                        },
                        86400: {
                            "cpu_request": 20,
                            "memory_request": 1 * 2**30,
                            "duration": 172800,
                        },
                    }
                },
            },
            "namespace2": {
                "pod3": {
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 8 * 2**30,
                            "duration": 172800,
                        },
                    }
                },
                "pod4": {
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 8 * 2**30,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100,
                            "gpu_resource": invoice.WHOLE_GPU,
                            "duration": 172700,  # little under 48 hours, expect to be rounded up in the output
                        },
                    }
                },
                "pod5": {
                    "gpu_type": invoice.GPU_A100_SXM4,
                    "metrics": {
                        0: {
                            "cpu_request": 24,
                            "memory_request": 8 * 2**30,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                            "gpu_resource": invoice.WHOLE_GPU,
                            "duration": 172800,
                        },
                    },
                },
            },
        }

        expected_output = (
            "Invoice Month,Report Start Time,Report End Time,Project - Allocation,Project - Allocation ID,Manager (PI),Cluster Name,Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost,Generated At\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace1,namespace1,,test-cluster,,,,,1128,OpenShift CPU,0.013,14.66,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2,namespace2,,test-cluster,,,,,96,OpenShift CPU,0.013,1.25,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2,namespace2,,test-cluster,,,,,48,OpenShift GPUA100,1.803,86.54,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2,namespace2,,test-cluster,,,,,48,OpenShift GPUA100SXM4,2.078,99.74,2023-01-05T00:00:00+00:00\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(
                condensed_metrics_dict=test_metrics_dict,
                file_name=tmp.name,
                report_metadata=self.report_metadata,
                rates=RATES,
                su_definitions=SU_DEFINITIONS,
            )
            self.assertEqual(tmp.read(), expected_output)

    def test_write_metrics_for_vms(self):
        test_metrics_dict = {
            "namespace1": {
                "vm_pod1": {
                    "metrics": {
                        0: {
                            "cpu_request": 2,
                            "gpu_request": 1,
                            "gpu_type": invoice.VM_GPU_A100_SXM4,
                            "gpu_resource": invoice.VM_GPU_A100_SXM4,
                            "memory_request": 4 * 2**30,
                            "duration": 86400,
                        },
                    }
                },
                "vm_pod2": {
                    "metrics": {
                        0: {
                            "cpu_request": 4,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_H100,
                            "gpu_resource": invoice.VM_GPU_H100,
                            "memory_request": 1 * 2**30,
                            "duration": 86400,
                        },
                    }
                },
            },
        }

        expected_output = (
            "Invoice Month,Report Start Time,Report End Time,Project - Allocation,Project - Allocation ID,Manager (PI),Cluster Name,Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost,Generated At\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace1,namespace1,,test-cluster,,,,,24,OpenShift GPUA100SXM4,2.078,49.87,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace1,namespace1,,test-cluster,,,,,24,OpenShift GPUH100,6.04,144.96,2023-01-05T00:00:00+00:00\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(
                condensed_metrics_dict=test_metrics_dict,
                file_name=tmp.name,
                report_metadata=self.report_metadata,
                rates=RATES,
                su_definitions=SU_DEFINITIONS,
            )
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsByClasses(TestCase):
    def setUp(self) -> None:
        self.report_metadata = invoice.ReportMetadata(
            report_month="2023-01",
            cluster_name="test-cluster",
            report_start_time=datetime(2023, 1, 1, tzinfo=UTC),
            report_end_time=datetime(2023, 1, 3, tzinfo=UTC),
            generated_at=datetime(2023, 1, 5, tzinfo=UTC),
        )

    def test_write_metrics_log(self):
        test_metrics_dict = {
            "namespace1": {  # namespace is ignored entirely from the report
                "pod1": {
                    "metrics": {
                        0: {
                            "cpu_request": 2,
                            "memory_request": 4 * 2**30,
                            "duration": 43200,
                        },
                    }
                },
            },
            "namespace2": {
                "pod2": {  # pod which doesn't belong to a class
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 8 * 2**30,
                            "duration": 172800,
                        },
                    }
                },
                "pod3": {
                    "label_nerc_mghpcc_org_class": "math-201",
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 8 * 2**30,
                            "duration": 86400,
                        },
                    },
                },
                "pod4": {
                    "label_nerc_mghpcc_org_class": "math-201",
                    "metrics": {
                        0: {
                            "cpu_request": 2,
                            "memory_request": 8 * 2**30,
                            "duration": 86400,
                        },
                    },
                },
                "pod5": {
                    "label_nerc_mghpcc_org_class": "math-201",
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 8 * 2**30,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100,
                            "gpu_resource": invoice.WHOLE_GPU,
                            "duration": 86400,
                        },
                    },
                },
                "pod6": {
                    "label_nerc_mghpcc_org_class": "cs-101",
                    "gpu_type": invoice.GPU_A100_SXM4,
                    "metrics": {
                        0: {
                            "cpu_request": 24,
                            "memory_request": 8 * 2**30,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                            "gpu_resource": invoice.WHOLE_GPU,
                            "duration": 172800,
                        },
                    },
                },
            },
        }

        expected_output = (
            "Invoice Month,Report Start Time,Report End Time,Project - Allocation,Project - Allocation ID,Manager (PI),Cluster Name,Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost,Generated At\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2:noclass,namespace2:noclass,,test-cluster,,,,,96,OpenShift CPU,0.013,1.25,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2:math-201,namespace2:math-201,,test-cluster,,,,,96,OpenShift CPU,0.013,1.25,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2:math-201,namespace2:math-201,,test-cluster,,,,,24,OpenShift GPUA100,1.803,43.27,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2:cs-101,namespace2:cs-101,,test-cluster,,,,,48,OpenShift GPUA100SXM4,2.078,99.74,2023-01-05T00:00:00+00:00\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_classes(
                condensed_metrics_dict=test_metrics_dict,
                file_name=tmp.name,
                report_metadata=self.report_metadata,
                rates=RATES,
                su_definitions=SU_DEFINITIONS,
                namespaces_with_classes=["namespace2"],
            )
            self.assertEqual(tmp.read(), expected_output)

    def test_write_metrics_by_namespace_decimal(self):
        """This tests the inaccurate result we get when using floating
        point instead of decimals.

        If floating points are used then the cost is 0.45499999999999996
        which is then rounded down to 0.45.
        """

        duration = 35  # hours
        rate = 0.013

        test_metrics_dict = {
            "namespace1": {
                "pod1": {
                    "namespace": "namespace1",
                    "metrics": {
                        0: {
                            "cpu_request": 1,
                            "memory_request": 4 * 2**30,
                            "duration": 35 * 3600,
                        },
                    },
                }
            }
        }

        cost = round(duration * rate, 2)
        self.assertEqual(cost, 0.45)

        expected_output = (
            "Invoice Month,Report Start Time,Report End Time,Project - Allocation,Project - Allocation ID,Manager (PI),Cluster Name,Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost,Generated At\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace1,namespace1,,test-cluster,,,,,35,OpenShift CPU,0.013,0.46,2023-01-05T00:00:00+00:00\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(
                condensed_metrics_dict=test_metrics_dict,
                file_name=tmp.name,
                report_metadata=self.report_metadata,
                su_definitions=SU_DEFINITIONS,
                rates=RATES,
            )
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsWithIgnoreHours(TestCase):
    def setUp(self):
        """Creates a test dictionary with condensed data that can be used to test WriteMetricsByPod and WriteMetricsByNamespace"""
        start_dt = int(datetime.fromisoformat("2024-04-10T11:00:00Z").timestamp())

        self.ignore_times = [
            (
                datetime(2024, 4, 9, 11, 0, 0, tzinfo=UTC),
                datetime(2024, 4, 10, 15, 0, 0, tzinfo=UTC),
            ),
            (
                datetime(2024, 4, 10, 22, 0, 0, tzinfo=UTC),
                datetime(2024, 4, 11, 5, 0, 0, tzinfo=UTC),
            ),
        ]
        HOUR = 60 * 60
        self.test_metrics_dict = {
            "namespace1": {
                "pod1": {  # runs from 2024-04-10T11:00:00Z to 2024-04-10T21:00:00Z - 2 SU * 6 billable hours
                    "metrics": {
                        start_dt: {
                            "cpu_request": 2,
                            "memory_request": 4 * 2**30,
                            "duration": 10 * HOUR,
                        },
                    }
                },
            },
            "namespace2": {
                "pod2": {
                    "metrics": {
                        start_dt: {  # runs from 2024-04-10T11:00:00Z to 2024-04-11T11:00:00Z - 2 SU * 13 billable hours
                            "cpu_request": 2,
                            "memory_request": 4 * 2**30,
                            "duration": 24 * HOUR,
                        },
                        start_dt
                        + 24
                        * HOUR: {  # runs from 2024-04-11T11:00:00Z to 2024-04-13T11:00:00Z - 3 SU * 48 billable hours
                            "cpu_request": 3,
                            "memory_request": 4 * 2**30,
                            "duration": 48 * HOUR,
                        },
                    }
                },
                "pod3": {  # runs from 2024-04-10T11:00:00Z to 2024-04-12T11:00:00Z - 1 SU * 37 billable hours
                    "gpu_type": invoice.GPU_A100_SXM4,
                    "metrics": {
                        start_dt: {
                            "cpu_request": 24,
                            "memory_request": 8 * 2**30,
                            "gpu_request": 1,
                            "gpu_type": invoice.GPU_A100_SXM4,
                            "gpu_resource": invoice.WHOLE_GPU,
                            "duration": 48 * HOUR,
                        },
                    },
                },
            },
        }

        self.report_metadata = invoice.ReportMetadata(
            report_month="2023-01",
            cluster_name="test-cluster",
            report_start_time=datetime(2023, 1, 1, tzinfo=UTC),
            report_end_time=datetime(2023, 1, 3, tzinfo=UTC),
            generated_at=datetime(2023, 1, 5, tzinfo=UTC),
        )

    def test_write_metrics_by_namespace_with_ignore_hours(self):
        expected_output = (
            "Invoice Month,Report Start Time,Report End Time,Project - Allocation,Project - Allocation ID,Manager (PI),Cluster Name,Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost,Generated At\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace1,namespace1,,test-cluster,,,,,12,OpenShift CPU,0.013,0.16,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2,namespace2,,test-cluster,,,,,170,OpenShift CPU,0.013,2.21,2023-01-05T00:00:00+00:00\n"
            "2023-01,2023-01-01T00:00:00+00:00,2023-01-03T00:00:00+00:00,namespace2,namespace2,,test-cluster,,,,,37,OpenShift GPUA100SXM4,2.078,76.89,2023-01-05T00:00:00+00:00\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(
                condensed_metrics_dict=self.test_metrics_dict,
                file_name=tmp.name,
                report_metadata=self.report_metadata,
                rates=RATES,
                su_definitions=SU_DEFINITIONS,
                ignore_hours=self.ignore_times,
            )
            self.assertEqual(tmp.read(), expected_output)

    def test_write_metrics_by_pod_with_ignore_hours(self):
        expected_output = (
            "Namespace,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,GPU Resource,Node,Node Model,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
            "namespace1,2024-04-10T11:00:00,2024-04-10T21:00:00,6.0000,pod1,2,0,,,Unknown Node,Unknown Model,4.0000,CPU,OpenShift CPU,2\n"
            "namespace2,2024-04-10T11:00:00,2024-04-11T11:00:00,13.0000,pod2,2,0,,,Unknown Node,Unknown Model,4.0000,CPU,OpenShift CPU,2\n"
            "namespace2,2024-04-11T11:00:00,2024-04-13T11:00:00,48.0000,pod2,3,0,,,Unknown Node,Unknown Model,4.0000,CPU,OpenShift CPU,3\n"
            "namespace2,2024-04-10T11:00:00,2024-04-12T11:00:00,37.0000,pod3,24,1,NVIDIA-A100-SXM4-40GB,nvidia.com/gpu,Unknown Node,Unknown Model,8.0000,GPU,OpenShift GPUA100SXM4,1\n"
        )

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_pod(
                self.test_metrics_dict, tmp.name, SU_DEFINITIONS, self.ignore_times
            )
            self.assertEqual(tmp.read(), expected_output)


class TestGetServiceUnit(TestCase):
    def make_pod(
        self, cpu_request, memory_request, gpu_request, gpu_type, gpu_resource
    ):
        return invoice.Pod(
            pod_name="pod1",
            namespace="namespace1",
            start_time=600,
            duration=3600,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            gpu_type=gpu_type,
            gpu_resource=gpu_resource,
            node_hostname="node-1",
            node_model="model-1",
        )

    def test_cpu_only(self):
        pod = self.make_pod(4, 16, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_CPU)
        self.assertEqual(su_count, 4)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu(self):
        pod = self.make_pod(24, 74, 1, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_A100_SXM4(self):
        pod = self.make_pod(31, 240, 1, invoice.GPU_A100_SXM4, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_SXM4_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_V100(self):
        pod = self.make_pod(48, 192, 1, invoice.GPU_V100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_V100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_H100(self):
        pod = self.make_pod(124, 360, 1, invoice.GPU_H100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_H100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_high_cpu(self):
        pod = self.make_pod(50, 96, 1, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_GPU)
        self.assertEqual(su_count, 3)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu_high_memory(self):
        pod = self.make_pod(24, 100, 1, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_GPU)
        self.assertEqual(su_count, 2)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_low_cpu_memory(self):
        pod = self.make_pod(2, 4, 1, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu(self):
        pod = self.make_pod(8, 64, 1, "Unknown_GPU_Type", invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_zero_count(self):
        pod = self.make_pod(8, 64, 0, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_known_mig_gpu(self):
        pod = self.make_pod(1, 4, 1, invoice.GPU_A100_SXM4, invoice.MIG_1G_5GB)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN_MIG_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_unknown_resource(self):
        pod = self.make_pod(1, 4, 1, invoice.GPU_A100, "nvidia.com/mig_20G_500GB")
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu_known_resource(self):
        pod = self.make_pod(1, 4, 1, "Unknown GPU", invoice.MIG_2G_10GB)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_zero_memory(self):
        pod = self.make_pod(1, 0, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_zero_cpu(self):
        pod = self.make_pod(0, 1, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_memory_dominant(self):
        pod = self.make_pod(8, 64, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_CPU)
        self.assertEqual(su_count, 16)
        self.assertEqual(determining_resource, "RAM")

    def test_fractional_su_cpu_dominant(self):
        pod = self.make_pod(0.5, 0.5, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_CPU)
        self.assertEqual(su_count, 0.5)
        self.assertEqual(determining_resource, "CPU")

    def test_fractional_su_memory_dominant(self):
        pod = self.make_pod(0.1, 1, 0, None, None)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_CPU)
        self.assertEqual(su_count, 0.25)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_fractional_cpu_memory(self):
        pod = self.make_pod(0.8, 0.8, 1, invoice.GPU_A100, invoice.WHOLE_GPU)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_decimal_return_type(self):
        from decimal import Decimal

        pod = self.make_pod(Decimal("1"), Decimal("8.1"), Decimal("0"), None, None)
        _, su_count, _ = pod.get_service_unit(SU_DEFINITIONS)
        self.assertIsInstance(su_count, Decimal)
        self.assertEqual(su_count, Decimal("2.025"))

    def test_not_decimal_return_type_when_gpu_su_type(self):
        from decimal import Decimal

        pod = self.make_pod(
            Decimal("1"),
            Decimal("76"),
            Decimal("1"),
            invoice.GPU_A100,
            invoice.WHOLE_GPU,
        )
        # for GPU SUs, we always round up to the nearest integer
        su_type, su_count, _ = pod.get_service_unit(SU_DEFINITIONS)
        self.assertIsInstance(su_count, int)
        self.assertEqual(su_count, 2)
        self.assertEqual(su_type, invoice.SU_A100_GPU)

    def test_vm_a100sxm4_gpu(self):
        pod = self.make_pod(1, 4, 1, invoice.GPU_UNKNOWN_TYPE, invoice.VM_GPU_A100_SXM4)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_A100_SXM4_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_vm_h100_gpu(self):
        pod = self.make_pod(1, 4, 1, invoice.GPU_UNKNOWN_TYPE, invoice.VM_GPU_H100)
        su_type, su_count, determining_resource = pod.get_service_unit(SU_DEFINITIONS)
        self.assertEqual(su_type, invoice.SU_H100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")
