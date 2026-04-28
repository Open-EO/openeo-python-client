import json
import re
from pathlib import Path

import pytest
import shapely.geometry

import openeo.processes
from openeo.api.process import Parameter
from openeo.rest._testing import DummyBackend, build_capabilities
from openeo.rest.connection import Connection
from openeo.rest.models.general import ValidationResponse
from openeo.rest.vectorcube import VectorCube
from openeo.util import InvalidBBoxException, dict_no_none

API_URL = "https://oeo.test"


@pytest.fixture
def vector_cube(con100) -> VectorCube:
    """Dummy vector cube"""
    return con100.load_geojson({"type": "Point", "coordinates": [1, 2]})


def test_vector_cube_fixture(vector_cube, dummy_backend):
    assert dummy_backend.execute(vector_cube) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
            "result": True,
        }
    }


def test_vector_cube_fixture_process_id(vector_cube, dummy_backend):
    assert dummy_backend.execute(vector_cube, process_id="load_geojson") == {
        "process_id": "load_geojson",
        "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        "result": True,
    }


def test_raster_to_vector(con100):
    img = con100.load_collection("S2")
    vector_cube = img.raster_to_vector()
    vector_cube_tranformed = vector_cube.run_udf(udf="python source code", runtime="Python")

    assert vector_cube_tranformed.flat_graph() == {
        "loadcollection1": {
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            "process_id": "load_collection",
        },
        "rastertovector1": {"arguments": {"data": {"from_node": "loadcollection1"}}, "process_id": "raster_to_vector"},
        "runudf1": {
            "arguments": {"data": {"from_node": "rastertovector1"}, "runtime": "Python", "udf": "python source code"},
            "process_id": "run_udf",
            "result": True,
        },
    }


@pytest.mark.parametrize(
    ["filename", "expected_format"],
    [
        ("result.json", "JSON"),  # TODO possible to allow "GeoJSON" with ".json" extension?
        ("result.geojson", "GeoJSON"),
        ("result.nc", "netCDF"),
    ],
)
@pytest.mark.parametrize("path_class", [str, Path])
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_only_file(
    vector_cube, dummy_backend, tmp_path, filename, expected_format, path_class, exec_mode
):
    output_path = tmp_path / filename
    if exec_mode == "sync":
        vector_cube.download(path_class(output_path))
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=path_class(output_path))
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "format": expected_format,
                "options": {},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize(
    ["filename", "execute_format", "expected_format"],
    [
        ("result.json", "JSON", "JSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.nc", "netCDF", "netCDF"),
        ("result.nc", "NETcDf", "NETcDf"),  # TODO #401 normalize format
        ("result.nc", "inV6l1d!!!", "inV6l1d!!!"),  # TODO #401 this should fail?
        ("result.json", None, "JSON"),
        ("result.geojson", None, "GeoJSON"),
        ("result.nc", None, "netCDF"),
        # TODO #449 more formats to autodetect?
    ],
)
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_with_format(
    vector_cube, dummy_backend, tmp_path, filename, execute_format, expected_format, exec_mode
):
    output_path = tmp_path / filename
    if exec_mode == "sync":
        vector_cube.download(output_path, format=execute_format)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path, out_format=execute_format)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "format": expected_format,
                "options": {},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_download_auto_save_result_with_options(vector_cube, dummy_backend, tmp_path, exec_mode):
    output_path = tmp_path / "result.json"
    format = "GeoJSON"
    options = {"precision": 7}

    if exec_mode == "sync":
        vector_cube.download(output_path, format=format, options=options)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path, out_format=format, **options)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "format": "GeoJSON",
                "options": {"precision": 7},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize(
    ["auto_add_save_result", "process_ids"],
    [
        (True, {"load_geojson", "save_result"}),
        (False, {"load_geojson"}),
    ],
)
def test_download_auto_add_save_result(vector_cube, dummy_backend, auto_add_save_result, process_ids, tmp_path):
    vector_cube.download(tmp_path / "result.geojson", auto_add_save_result=auto_add_save_result)
    assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids


@pytest.mark.parametrize(
    ["output_file", "save_result_format", "expected_format"],
    [
        ("result.geojson", None, "GeoJSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.json", "JSON", "JSON"),
        ("result.nc", "netCDF", "netCDF"),
        ("result.data", "netCDF", "netCDF"),
    ],
)
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_save_result_and_download_filename(
    vector_cube, dummy_backend, tmp_path, output_file, save_result_format, expected_format, exec_mode
):
    """e.g. https://github.com/Open-EO/openeo-geopyspark-driver/issues/477"""
    vector_cube = vector_cube.save_result(format=save_result_format)
    output_path = tmp_path / output_file
    if exec_mode == "sync":
        vector_cube.download(output_path)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path)
    else:
        raise ValueError(exec_mode)

    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadgeojson1"}, "format": expected_format, "options": {}},
            "result": True,
        },
    }
    assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize(
    ["save_result_format", "execute_format", "output_file", "expected"],
    [
        (None, None, None, "GeoJSON"),
        (None, None, "result.geojson", "GeoJSON"),
        ("GeoJSON", None, None, "GeoJSON"),
        (None, "GeoJSON", None, "GeoJSON"),
        (
            "GeoJSON",
            "GeoJSON",
            None,
            TypeError("got an unexpected keyword argument 'format'"),
        ),
        (None, None, "result.nc", "netCDF"),
        ("netCDF", None, None, "netCDF"),
        (None, "netCDF", None, "netCDF"),
        (
            "GeoJson",
            "netCDF",
            None,
            TypeError("got an unexpected keyword argument 'format'"),
        ),
    ],
)
def test_save_result_and_download_with_format(
    vector_cube, dummy_backend, tmp_path, save_result_format, execute_format, output_file, expected
):
    if save_result_format:
        vector_cube = vector_cube.save_result(format=save_result_format)
    output_path = tmp_path / (output_file or "data")

    def do_it():
        if execute_format:
            vector_cube.download(output_path, format=execute_format)
        else:
            vector_cube.download(output_path)

    if isinstance(expected, Exception):
        with pytest.raises(type(expected), match=re.escape(str(expected))):
            do_it()
    else:
        do_it()
        assert dummy_backend.get_pg()["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadgeojson1"}, "format": expected, "options": {}},
            "result": True,
        }
        assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize(
    ["save_result_format", "execute_format", "output_file", "expected"],
    [
        (None, None, None, "GeoJSON"),
        (None, None, "result.geojson", "GeoJSON"),
        ("GeoJSON", None, None, "GeoJSON"),
        (None, "GeoJSON", None, "GeoJSON"),
        (
            "GeoJSON",
            "GeoJSON",
            None,
            TypeError("got an unexpected keyword argument 'out_format'"),
        ),
        (None, None, "result.nc", "netCDF"),
        ("netCDF", None, None, "netCDF"),
        (None, "netCDF", None, "netCDF"),
        (
            "GeoJson",
            "netCDF",
            None,
            TypeError("got an unexpected keyword argument 'out_format'"),
        ),
    ],
)
def test_save_result_and_execute_batch_with_format(
    vector_cube, dummy_backend, tmp_path, save_result_format, execute_format, output_file, expected
):
    if save_result_format:
        vector_cube = vector_cube.save_result(format=save_result_format)
    output_path = tmp_path / (output_file or "data")

    def do_it():
        if execute_format:
            vector_cube.execute_batch(outputfile=output_path, out_format=execute_format)
        else:
            vector_cube.execute_batch(outputfile=output_path)

    if isinstance(expected, Exception):
        with pytest.raises(type(expected), match=re.escape(str(expected))):
            do_it()
    else:
        do_it()
        assert dummy_backend.get_pg()["saveresult1"] == {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadgeojson1"}, "format": expected, "options": {}},
            "result": True,
        }
        assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT


@pytest.mark.parametrize(
    ["auto_add_save_result", "process_ids"],
    [
        (True, {"load_geojson", "save_result"}),
        (False, {"load_geojson"}),
    ],
)
def test_create_job_auto_add_save_result(vector_cube, dummy_backend, auto_add_save_result, process_ids):
    vector_cube.create_job(auto_add_save_result=auto_add_save_result)
    assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids


@pytest.mark.parametrize(
    ["auto_add_save_result", "process_ids"],
    [
        (True, {"load_geojson", "save_result"}),
        (False, {"load_geojson"}),
    ],
)
def test_cexecute_batch_auto_add_save_result(vector_cube, dummy_backend, auto_add_save_result, process_ids):
    vector_cube.execute_batch(auto_add_save_result=auto_add_save_result)
    assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids


@pytest.mark.parametrize(
    "data",
    [
        {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
        """{"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]}""",
        shapely.geometry.Polygon([[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]),
    ],
)
def test_load_geojson_basic(con100, data, dummy_backend):
    vc = VectorCube.load_geojson(connection=con100, data=data)
    assert isinstance(vc, VectorCube)
    assert dummy_backend.execute(vc) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {
                "data": {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
                "properties": [],
            },
            "result": True,
        }
    }


@pytest.mark.parametrize("path_type", [str, Path])
def test_load_geojson_path(con100, dummy_backend, tmp_path, path_type):
    path = tmp_path / "geometry.json"
    path.write_text("""{"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]}""")
    vc = VectorCube.load_geojson(connection=con100, data=path_type(path))
    assert isinstance(vc, VectorCube)
    assert dummy_backend.execute(vc) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {
                "data": {"type": "Polygon", "coordinates": [[[1, 2], [3, 2], [3, 4], [1, 4], [1, 2]]]},
                "properties": [],
            },
            "result": True,
        }
    }


def test_load_geojson_parameter(con100, dummy_backend):
    vc = VectorCube.load_geojson(connection=con100, data=Parameter.datacube())
    assert isinstance(vc, VectorCube)
    assert dummy_backend.execute(vc) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"from_parameter": "data"}, "properties": []},
            "result": True,
        }
    }


def test_load_url(con100, dummy_backend):
    vc = VectorCube.load_url(connection=con100, url="https://example.com/geometry.json", format="GeoJSON")
    assert isinstance(vc, VectorCube)
    assert dummy_backend.execute(vc) == {
        "loadurl1": {
            "process_id": "load_url",
            "arguments": {"url": "https://example.com/geometry.json", "format": "GeoJSON"},
            "result": True,
        }
    }


@pytest.mark.parametrize(
    ["dimension"],
    [
        ("geometry",),
        ("geometries",),
    ],
)
def test_apply_dimension(vector_cube, dummy_backend, dimension, caplog):
    vc = vector_cube.apply_dimension("sort", dimension=dimension)
    assert dummy_backend.execute(vc, process_id="apply_dimension") == {
        "process_id": "apply_dimension",
        "arguments": {
            "data": {"from_node": "loadgeojson1"},
            "dimension": dimension,
            "process": {
                "process_graph": {
                    "sort1": {
                        "process_id": "sort",
                        "arguments": {"data": {"from_parameter": "data"}},
                        "result": True,
                    }
                }
            },
        },
        "result": True,
    }



def test_filter_bands(vector_cube, dummy_backend):
    vc = vector_cube.filter_bands(["B01", "B02"])
    assert dummy_backend.execute(vc, process_id="filter_bands") == {
        "process_id": "filter_bands",
        "arguments": {"data": {"from_node": "loadgeojson1"}, "bands": ["B01", "B02"]},
        "result": True,
    }


def test_filter_bbox_wsen(vector_cube, dummy_backend):
    vc = vector_cube.filter_bbox(west=1, south=2, east=3, north=4)
    assert dummy_backend.execute(vc, process_id="filter_bbox") == {
        "process_id": "filter_bbox",
        "arguments": {"data": {"from_node": "loadgeojson1"}, "extent": {"west": 1, "south": 2, "east": 3, "north": 4}},
        "result": True,
    }


@pytest.mark.parametrize(
    "extent",
    [
        [1, 2, 3, 4],
        (1, 2, 3, 4),
        {"west": 1, "south": 2, "east": 3, "north": 4},
    ],
)
def test_filter_bbox_extent(vector_cube, dummy_backend, extent):
    vc = vector_cube.filter_bbox(extent=extent)
    assert dummy_backend.execute(vc, process_id="filter_bbox") == {
        "process_id": "filter_bbox",
        "arguments": {"data": {"from_node": "loadgeojson1"}, "extent": {"west": 1, "south": 2, "east": 3, "north": 4}},
        "result": True,
    }


def test_filter_bbox_extent_parameter(vector_cube, dummy_backend):
    vc = vector_cube.filter_bbox(extent=Parameter(name="the_extent"))
    assert dummy_backend.execute(vc, process_id="filter_bbox") == {
        "process_id": "filter_bbox",
        "arguments": {"data": {"from_node": "loadgeojson1"}, "extent": {"from_parameter": "the_extent"}},
        "result": True,
    }


@pytest.mark.parametrize(
    ["kwargs", "expected"],
    [
        ({}, "Can not construct BBoxDict from None"),
        ({"west": 123, "south": 456, "east": 789}, "Missing bbox fields ['north']"),
        ({"west": 123, "extent": [1, 2, 3, 4]}, "Don't specify both west/south/east/north and extent"),
        ({"extent": [1, 2, 3]}, "Expected sequence with 4 items, but got 3."),
        ({"extent": {"west": 1, "south": 2, "east": 3, "norht": 4}}, "Missing bbox fields ['north']"),
    ],
)
def test_filter_bbox_invalid(vector_cube, dummy_backend, kwargs, expected):
    with pytest.raises(InvalidBBoxException, match=re.escape(expected)):
        _ = vector_cube.filter_bbox(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"extent": [1, 2, 3, 4], "crs": 4326},
        {"extent": {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}},
        {"extent": {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}, "crs": 4326},
        {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326},
        {"extent": [1, 2, 3, 4], "crs": "EPSG:4326"},
    ],
)
def test_filter_bbox_crs(vector_cube, dummy_backend, kwargs):
    vc = vector_cube.filter_bbox(**kwargs)
    assert dummy_backend.execute(vc, process_id="filter_bbox") == {
        "process_id": "filter_bbox",
        "arguments": {
            "data": {"from_node": "loadgeojson1"},
            "extent": {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326},
        },
        "result": True,
    }


def test_filter_labels_eq(vector_cube, dummy_backend):
    vc = vector_cube.filter_labels(condition=lambda v: v == "B02", dimension="properties")
    assert dummy_backend.execute(vc, process_id="filter_labels") == {
        "process_id": "filter_labels",
        "arguments": {
            "data": {"from_node": "loadgeojson1"},
            "condition": {
                "process_graph": {
                    "eq1": {
                        "arguments": {"x": {"from_parameter": "value"}, "y": "B02"},
                        "process_id": "eq",
                        "result": True,
                    }
                }
            },
            "dimension": "properties",
        },
        "result": True,
    }


def test_filter_labels_contains(vector_cube, dummy_backend):
    vc = vector_cube.filter_labels(
        condition=lambda v: openeo.processes.array_contains(["B02", "B03"], v), dimension="properties"
    )
    assert dummy_backend.execute(vc, process_id="filter_labels") == {
        "process_id": "filter_labels",
        "arguments": {
            "data": {"from_node": "loadgeojson1"},
            "condition": {
                "process_graph": {
                    "arraycontains1": {
                        "arguments": {"data": ["B02", "B03"], "value": {"from_parameter": "value"}},
                        "process_id": "array_contains",
                        "result": True,
                    }
                }
            },
            "dimension": "properties",
        },
        "result": True,
    }


def test_filter_vector_vector_cube(vector_cube, con100, dummy_backend):
    geometries = con100.load_geojson({"type": "Point", "coordinates": [3, 4]})
    vc = vector_cube.filter_vector(geometries=geometries)
    assert dummy_backend.execute(vc) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "loadgeojson2": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [3, 4]}, "properties": []},
        },
        "filtervector1": {
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "geometries": {"from_node": "loadgeojson2"},
                "relation": "intersects",
            },
            "process_id": "filter_vector",
            "result": True,
        },
    }


@pytest.mark.parametrize(
    "geometries",
    [
        shapely.geometry.Point(3, 4),
        {"type": "Point", "coordinates": [3, 4]},
    ],
)
def test_filter_vector_shapely(vector_cube, dummy_backend, geometries):
    vc = vector_cube.filter_vector(geometries=geometries)
    assert dummy_backend.execute(vc) == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "loadgeojson2": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [3, 4]}, "properties": []},
        },
        "filtervector1": {
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "geometries": {"from_node": "loadgeojson2"},
                "relation": "intersects",
            },
            "process_id": "filter_vector",
            "result": True,
        },
    }


class TestVectorCubeValidation:
    """
    Test (auto) validation of vector cube execution with `download`, `execute`, ...
    """

    _PG_GEOJSON = {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
            "result": True,
        },
    }
    _PG_GEOJSON_SAVE = {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadgeojson1"}, "format": "GeoJSON", "options": {}},
            "result": True,
        },
    }

    @pytest.fixture(params=[False, True])
    def auto_validate(self, request) -> bool:
        """Fixture to parametrize auto_validate setting."""
        return request.param

    @pytest.fixture
    def connection(self, api_version, requests_mock, api_capabilities, auto_validate) -> Connection:
        requests_mock.get(API_URL, json=build_capabilities(api_version=api_version, **api_capabilities))
        con = Connection(API_URL, **dict_no_none(auto_validate=auto_validate))
        return con

    @pytest.fixture(autouse=True)
    def dummy_backend_setup(self, dummy_backend):
        dummy_backend.next_result = {"type": "Point", "coordinates": [1, 2]}
        dummy_backend.next_validation_errors = [{"code": "NoPoints", "message": "Don't use points."}]

    # Reusable list of (fixture) parameterization
    # of ["api_capabilities", "auto_validate", "validate", "validation_expected"]
    _VALIDATION_PARAMETER_SETS = [
        # No validation supported by backend: don't attempt to validate
        ({}, None, None, False),
        ({}, True, True, False),
        # Validation supported by backend, default behavior -> validate
        ({"validation": True}, None, None, True),
        # (Validation supported by backend) no explicit validation enabled: follow auto_validate setting
        ({"validation": True}, True, None, True),
        ({"validation": True}, False, None, False),
        # (Validation supported by backend) follow explicit `validate` toggle regardless of auto_validate
        ({"validation": True}, False, True, True),
        ({"validation": True}, True, False, False),
    ]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_vectorcube_download_validation(
        self, dummy_backend, connection, validate, validation_expected, caplog, tmp_path
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        vector_cube = VectorCube.load_geojson(connection=connection, data={"type": "Point", "coordinates": [1, 2]})

        output = tmp_path / "result.geojson"
        vector_cube.download(outputfile=output, **dict_no_none(validate=validate))
        assert json.loads(output.read_text()) == {"type": "Point", "coordinates": [1, 2]}
        assert dummy_backend.get_sync_pg() == self._PG_GEOJSON_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_GEOJSON_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoPoints] Don't use points."]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_vectorcube_execute_validation(self, dummy_backend, connection, validate, validation_expected, caplog):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        vector_cube = VectorCube.load_geojson(connection=connection, data={"type": "Point", "coordinates": [1, 2]})

        res = vector_cube.execute(**dict_no_none(validate=validate))
        assert res == {"type": "Point", "coordinates": [1, 2]}
        assert dummy_backend.get_sync_pg() == self._PG_GEOJSON

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_GEOJSON]
            assert caplog.messages == ["Preflight process graph validation raised: [NoPoints] Don't use points."]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_vectorcube_create_job_validation(self, dummy_backend, connection, validate, validation_expected, caplog):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        vector_cube = VectorCube.load_geojson(connection=connection, data={"type": "Point", "coordinates": [1, 2]})

        job = vector_cube.create_job(**dict_no_none(validate=validate))
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_GEOJSON_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_GEOJSON_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoPoints] Don't use points."]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []

    @pytest.mark.parametrize("api_capabilities", [{"validation": True}])
    def test_vectorcube_create_job_validation_broken(self, dummy_backend, connection, requests_mock, caplog):
        """Test resilience against broken validation response."""
        requests_mock.post(
            connection.build_url("/validation"), status_code=500, json={"code": "Internal", "message": "nope!"}
        )
        vector_cube = VectorCube.load_geojson(connection=connection, data={"type": "Point", "coordinates": [1, 2]})

        job = vector_cube.create_job(validate=True)
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_GEOJSON_SAVE

        assert caplog.messages == ["Preflight process graph validation failed: [500] Internal: nope!"]

    @pytest.mark.parametrize(
        ["api_capabilities", "auto_validate", "validate", "validation_expected"],
        _VALIDATION_PARAMETER_SETS,
    )
    def test_vectorcube_execute_batch_validation(
        self, dummy_backend, connection, validate, validation_expected, caplog
    ):
        """The DataCube should pass through request for the validation to the
        connection and the validation endpoint should only be called when
        validation was requested.
        """
        vector_cube = VectorCube.load_geojson(connection=connection, data={"type": "Point", "coordinates": [1, 2]})

        job = vector_cube.execute_batch(**dict_no_none(validate=validate))
        assert job.job_id == "job-000"
        assert dummy_backend.get_batch_pg() == self._PG_GEOJSON_SAVE

        if validation_expected:
            assert dummy_backend.validation_requests == [self._PG_GEOJSON_SAVE]
            assert caplog.messages == ["Preflight process graph validation raised: [NoPoints] Don't use points."]
        else:
            assert dummy_backend.validation_requests == []
            assert caplog.messages == []


@pytest.mark.parametrize(
    ["target_parameter", "expected_target_parameter"],
    [
        (None, "target"),
        ("target_data_cube", "target_data_cube"),
        ("target", "target"),
    ],
)
def test_vector_to_raster(s2cube, vector_cube, requests_mock, target_parameter, expected_target_parameter):
    if target_parameter:
        requests_mock.get(
            API_URL + "/processes",
            json={
                "processes": [
                    {
                        "id": "vector_to_raster",
                        "parameters": [
                            {"name": "data"},
                            {"name": target_parameter},
                        ],
                    }
                ]
            },
        )
    raster_cube = vector_cube.vector_to_raster(s2cube)
    assert raster_cube.flat_graph() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"type": "Point", "coordinates": [1, 2]}, "properties": []},
        },
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "vectortoraster1": {
            "process_id": "vector_to_raster",
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                expected_target_parameter: {"from_node": "loadcollection1"},
            },
            "result": True,
        },
    }


def test_execute_batch_with_title(vector_cube, dummy_backend):
    """
    Support title/description in execute_batch
    https://github.com/Open-EO/openeo-python-client/issues/652
    """
    vector_cube.execute_batch(title="S2 job", description="Lorem ipsum dolor S2 amet")
    assert dummy_backend.batch_jobs == {
        "job-000": {
            "job_id": "job-000",
            "pg": {
                "loadgeojson1": {
                    "process_id": "load_geojson",
                    "arguments": {"data": {"coordinates": [1, 2], "type": "Point"}, "properties": []},
                },
                "saveresult1": {
                    "process_id": "save_result",
                    "arguments": {"data": {"from_node": "loadgeojson1"}, "format": "GeoJSON", "options": {}},
                    "result": True,
                },
            },
            "status": "finished",
            "title": "S2 job",
            "description": "Lorem ipsum dolor S2 amet",
        }
    }


def test_vector_cube_validate(vector_cube, dummy_backend):
    dummy_backend.next_validation_errors = [{"code": "OfflineRequired", "message": "Turn off your smartphone"}]

    result = vector_cube.validate()

    assert dummy_backend.validation_requests == [
        {
            "loadgeojson1": {
                "process_id": "load_geojson",
                "arguments": {"data": {"coordinates": [1, 2], "type": "Point"}, "properties": []},
                "result": True,
            }
        }
    ]
    assert isinstance(result, ValidationResponse)
    assert result == [{"code": "OfflineRequired", "message": "Turn off your smartphone"}]
