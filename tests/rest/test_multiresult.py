import pytest

from openeo import BatchJob
from openeo.rest._testing import DummyBackend
from openeo.rest.multiresult import MultiResult


class TestMultiResultHandling:
    def test_flat_graph(self, dummy_backend):
        cube = dummy_backend.connection.load_collection("S2")
        save1 = cube.save_result(format="GTiff")
        save2 = cube.save_result(format="netCDF")
        multi_result = MultiResult([save1, save2])
        assert multi_result.flat_graph() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
            },
            "saveresult2": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "netCDF", "options": {}},
                "result": True,
            },
        }

    def test_create_job_method(self, dummy_backend):
        con = dummy_backend.connection
        cube = con.load_collection("S2")
        save1 = cube.save_result(format="GTiff")
        save2 = cube.save_result(format="netCDF")
        multi_result = MultiResult([save1, save2])
        multi_result.create_job(title="multi result test")
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
                    },
                    "saveresult1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                    },
                    "saveresult2": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "loadcollection1"}, "format": "netCDF", "options": {}},
                        "result": True,
                    },
                },
                "status": "created",
                "title": "multi result test",
            }
        }

    def test_create_job_through_connection(self, con120, dummy_backend):
        con = dummy_backend.connection
        cube = con120.load_collection("S2")
        save1 = cube.save_result(format="GTiff")
        save2 = cube.save_result(format="netCDF")
        multi_result = MultiResult([save1, save2])
        con.create_job(multi_result)
        assert dummy_backend.get_batch_pg() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
            },
            "saveresult2": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "netCDF", "options": {}},
                "result": True,
            },
        }

    def test_execute_batch(self, dummy_backend):
        con = dummy_backend.connection
        cube = con.load_collection("S2")
        save1 = cube.save_result(format="GTiff")
        save2 = cube.save_result(format="netCDF")
        multi_result = MultiResult([save1, save2])
        job = multi_result.execute_batch(title="multi result test")
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
                    },
                    "saveresult1": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                    },
                    "saveresult2": {
                        "process_id": "save_result",
                        "arguments": {"data": {"from_node": "loadcollection1"}, "format": "netCDF", "options": {}},
                        "result": True,
                    },
                },
                "status": "finished",
                "title": "multi result test",
            }
        }
