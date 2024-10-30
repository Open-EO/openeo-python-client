import pytest

from openeo.rest._testing import DummyBackend
from openeo.rest.multiresult import MultiResult


class TestMultiResultHandling:

    def test_create_job_method(self, dummy_backend):
        con = dummy_backend.connection
        cube = con.load_collection("S2")
        save1 = cube.save_result(format="GTiff")
        save2 = cube.save_result(format="netCDF")
        multi_result = MultiResult([save1, save2])
        multi_result.create_job()
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

    def test_create_job_on_connection(self, con120, dummy_backend):
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
