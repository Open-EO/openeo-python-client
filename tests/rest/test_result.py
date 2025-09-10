from openeo.rest.datacube import THIS
from openeo.rest.models.general import ValidationResponse


def test_legacy_save_result_export_workspace(dummy_backend):
    """https://github.com/Open-EO/openeo-python-client/issues/742"""

    s2 = dummy_backend.connection.load_collection("S2")
    saved = s2.save_result(format="GTiff")
    result = saved.process("export_workspace", arguments={"data": THIS, "workspace": "foobar"})
    result.download()
    assert dummy_backend.get_pg() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {"data": {"from_node": "saveresult1"}, "workspace": "foobar"},
            "result": True,
        },
    }


def test_save_result_validate(dummy_backend):
    dummy_backend.next_validation_errors = [{"code": "OfflineRequired", "message": "Turn off your smartphone"}]

    s2 = dummy_backend.connection.load_collection("S2")
    saved = s2.save_result(format="GTiff")
    result = saved.validate()

    assert dummy_backend.validation_requests == [
        {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
    ]
    assert isinstance(result, ValidationResponse)
    assert result == [{"code": "OfflineRequired", "message": "Turn off your smartphone"}]
