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
from requests.exceptions import ConnectionError
import tempfile
from unittest import TestCase, mock

from openshift_metrics import utils
import os

class TestGetNamespaceAnnotations(TestCase):

    @mock.patch('openshift_metrics.utils.requests.post')
    @mock.patch('openshift_metrics.utils.requests.session')
    def test_get_namespace_attributes(self, mock_session, mock_post):
        mock_response_json = [
            {
                "attributes": {
                    "Allocated Project Name": "Project 1",
                    "Institution-Specific Code": "123"
                },
                "project": {
                    "pi": "PI 1",
                    "id": "1"
                }
            },
            {
                "attributes": {
                    "Allocated Project Name": "Project 2",
                    "Institution-Specific Code": "456"
                },
                "project": {
                    "pi": "PI 2",
                    "id": "2"
                }
            }
        ]
        mock_response = mock.Mock()
        mock_response.json.return_value = mock_response_json

        mock_session_instance = mock_session.return_value
        mock_session_instance.get.return_value = mock_response

        with mock.patch.dict(os.environ, {"CLIENT_ID": "your_client_id", "CLIENT_SECRET": "your_client_secret"}):
            namespaces_dict = utils.get_namespace_attributes()


        expected_namespaces_dict = {
            "Project 1": {
                "cf_pi": "PI 1",
                "cf_project_id": "1",
                "institution_code": "123"
            },
            "Project 2": {
                "cf_pi": "PI 2",
                "cf_project_id": "2",
                "institution_code": "456"
            }
        }
        self.assertEqual(namespaces_dict, expected_namespaces_dict)


class TestWriteMetricsByPod(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
            },
            'namespace2': {
                'cf_pi': 'PI2',
                'cf_project_id': '456',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
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
                        "node_model": "Lenovo"
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu_request": 20,
                        "memory_request": 10485760,
                        "duration": 60
                    },
                    60: {
                        "cpu_request": 25,
                        "memory_request": 10485760,
                        "duration": 60
                    },
                    120: {
                        "cpu_request": 20,
                        "memory_request": 10485760,
                        "duration": 60
                    }
                }
            },
            "pod3": {
                "namespace": "namespace2",
                "metrics": {
                    0: {
                        "cpu_request": 45,
                        "memory_request": 104857600,
                        "duration": 180
                    },
                }
            },
            "pod4": { # this results in 0.5 SU
                "namespace": "namespace2",
                "metrics": {
                    0: {
                        "cpu_request": 0.5,
                        "memory_request": 2147483648,
                        "duration": 3600
                    },
                }
            },
        }

        expected_output = ("Namespace,Coldfront_PI Name,Coldfront Project ID ,Pod Start Time,Pod End Time,Duration (Hours),Pod Name,CPU Request,GPU Request,GPU Type,GPU Resource,Node,Node Model,Memory Request (GiB),Determining Resource,SU Type,SU Count\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:02:00,0.0333,pod1,10,0,,,wrk-1,Dell,0.0010,CPU,OpenShift CPU,10\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod1,20,0,,,wrk-2,Lenovo,0.0010,CPU,OpenShift CPU,20\n"
                           "namespace1,PI1,123,1970-01-01T00:00:00,1970-01-01T00:01:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
                           "namespace1,PI1,123,1970-01-01T00:01:00,1970-01-01T00:02:00,0.0167,pod2,25,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,25\n"
                           "namespace1,PI1,123,1970-01-01T00:02:00,1970-01-01T00:03:00,0.0167,pod2,20,0,,,Unknown Node,Unknown Model,0.0098,CPU,OpenShift CPU,20\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T00:03:00,0.0500,pod3,45,0,,,Unknown Node,Unknown Model,0.0977,CPU,OpenShift CPU,45\n"
                           "namespace2,PI2,456,1970-01-01T00:00:00,1970-01-01T01:00:00,1.0000,pod4,0.5,0,,,Unknown Node,Unknown Model,2.0000,CPU,OpenShift CPU,0.5\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_pod(test_metrics_dict, tmp.name)
            self.assertEqual(tmp.read(), expected_output)


class TestWriteMetricsByNamespace(TestCase):

    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
    def test_write_metrics_log(self, mock_gna):
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
                'institution_code': '76'
            },
            'namespace2': {
                'cf_pi': 'PI2',
                'cf_project_id': '456',
            }
        }
        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu_request": 2,
                        "memory_request": 4 * 2**30,
                        "duration": 43200
                    },
                    43200: {
                        "cpu_request": 4,
                        "memory_request": 4 * 2**30,
                        "duration": 43200
                    }
                }
            },
            "pod2": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu_request": 4,
                        "memory_request": 1 * 2**30,
                        "duration": 86400
                    },
                    86400: {
                        "cpu_request": 20,
                        "memory_request": 1 * 2**30,
                        "duration": 172800
                    }
                }
            },
            "pod3": {
                "namespace": "namespace2",
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 8 * 2**30,
                        "duration": 172800
                    },
                }
            },
            "pod4": {
                "namespace": "namespace2",
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100,
                        "gpu_resource": utils.WHOLE_GPU,
                        "duration": 172700 # little under 48 hours, expect to be rounded up in the output
                    },
                }
            },
            "pod5": {
                "namespace": "namespace2",
                "gpu_type": utils.GPU_A100_SXM4,
                "metrics": {
                    0: {
                        "cpu_request": 24,
                        "memory_request": 8 * 2**30,
                        "gpu_request": 1,
                        "gpu_type": utils.GPU_A100_SXM4,
                        "gpu_resource": utils.WHOLE_GPU,
                        "duration": 172800
                    },
                }
            },
        }

        expected_output = ("Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
                            "2023-01,namespace1,namespace1,PI1,,,,76,1128,OpenShift CPU,0.013,14.66\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,96,OpenShift CPU,0.013,1.25\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100,1.803,86.54\n"
                            "2023-01,namespace2,namespace2,PI2,,,,,48,OpenShift GPUA100SXM4,2.078,99.74\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
            self.assertEqual(tmp.read(), expected_output)


    @mock.patch('openshift_metrics.utils.get_namespace_attributes')
    def test_write_metrics_by_namespace_decimal(self, mock_gna):
        """This tests the inaccurate result we get when using floating
        point instead of decimals.

        If floating points are used then the cost is 0.45499999999999996
        which is then rounded down to 0.45.
        """
        mock_gna.return_value = {
            'namespace1': {
                'cf_pi': 'PI1',
                'cf_project_id': '123',
                'institution_code': '76'
            },
        }

        duration = 35 #hours
        rate = 0.013

        test_metrics_dict = {
            "pod1": {
                "namespace": "namespace1",
                "metrics": {
                    0: {
                        "cpu_request": 1,
                        "memory_request": 4 * 2**30,
                        "duration": 35*3600
                    },
                }
        }}

        cost = round(duration*rate,2)
        self.assertEqual(cost, 0.45)

        expected_output = ("Invoice Month,Project - Allocation,Project - Allocation ID,Manager (PI),Invoice Email,Invoice Address,Institution,Institution - Specific Code,SU Hours (GBhr or SUhr),SU Type,Rate,Cost\n"
                            "2023-01,namespace1,namespace1,PI1,,,,76,35,OpenShift CPU,0.013,0.46\n")

        with tempfile.NamedTemporaryFile(mode="w+") as tmp:
            utils.write_metrics_by_namespace(test_metrics_dict, tmp.name, "2023-01")
            self.assertEqual(tmp.read(), expected_output)


class TestGetServiceUnit(TestCase):

    def test_cpu_only(self):
        su_type, su_count, determining_resource = utils.get_service_unit(4, 16, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 4)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 74, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_A100_SXM4(self):
        su_type, su_count, determining_resource = utils.get_service_unit(32, 245, 1, utils.GPU_A100_SXM4, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_SXM4_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_high_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(50, 96, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 3)
        self.assertEqual(determining_resource, "CPU")

    def test_known_gpu_high_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(24, 100, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 2)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_low_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(2, 4, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 1, "Unknown_GPU_Type", utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_zero_count(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 0, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_known_mig_gpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, utils.GPU_A100_SXM4, utils.MIG_1G_5GB)
        self.assertEqual(su_type, utils.SU_UNKNOWN_MIG_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_known_gpu_unknown_resource(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, utils.GPU_A100, "nvidia.com/mig_20G_500GB")
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_unknown_gpu_known_resource(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 4, 1, "Unknown GPU", utils.MIG_2G_10GB)
        self.assertEqual(su_type, utils.SU_UNKNOWN_GPU)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "GPU")

    def test_zero_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(1, 0, 0, None, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_zero_cpu(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0, 1, 0, None, None)
        self.assertEqual(su_type, utils.SU_UNKNOWN)
        self.assertEqual(su_count, 0)
        self.assertEqual(determining_resource, "CPU")

    def test_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(8, 64, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 16)
        self.assertEqual(determining_resource, "RAM")

    def test_fractional_su_cpu_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.5, 0.5, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.5)
        self.assertEqual(determining_resource, "CPU")

    def test_fractional_su_memory_dominant(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.1, 1, 0, None, None)
        self.assertEqual(su_type, utils.SU_CPU)
        self.assertEqual(su_count, 0.25)
        self.assertEqual(determining_resource, "RAM")

    def test_known_gpu_fractional_cpu_memory(self):
        su_type, su_count, determining_resource = utils.get_service_unit(0.8, 0.8, 1, utils.GPU_A100, utils.WHOLE_GPU)
        self.assertEqual(su_type, utils.SU_A100_GPU)
        self.assertEqual(su_count, 1)
        self.assertEqual(determining_resource, "GPU")

    def test_decimal_return_type(self):
        from decimal import Decimal
        _, su_count, _ = utils.get_service_unit(Decimal("1"), Decimal("8.1"), Decimal("0"), None, None)
        self.assertIsInstance(su_count, Decimal)
        self.assertEqual(su_count, Decimal('2.025'))

    def test_not_decimal_return_type_when_gpu_su_type(self):
        from decimal import Decimal
        su_type, su_count, _ = utils.get_service_unit(Decimal("1"), Decimal("76"), Decimal("1"), utils.GPU_A100, utils.WHOLE_GPU)
        # for GPU SUs, we always round up to the nearest integer
        self.assertIsInstance(su_count, int)
        self.assertEqual(su_count, 2)
        self.assertEqual(su_type, utils.SU_A100_GPU)
