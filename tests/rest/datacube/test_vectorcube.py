import re
from pathlib import Path

import pytest
import shapely.geometry

import openeo.processes
from openeo.api.process import Parameter
from openeo.rest._testing import DummyBackend
from openeo.rest.vectorcube import VectorCube
from openeo.util import InvalidBBoxException


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
    ["filename", "format", "expected_format"],
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
    vector_cube, dummy_backend, tmp_path, filename, format, expected_format, exec_mode
):
    output_path = tmp_path / filename
    if exec_mode == "sync":
        vector_cube.download(output_path, format=format)
    elif exec_mode == "batch":
        vector_cube.execute_batch(outputfile=output_path, out_format=format)
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
    ["output_file", "format", "expected_format"],
    [
        ("result.geojson", None, "GeoJSON"),
        ("result.geojson", "GeoJSON", "GeoJSON"),
        ("result.json", "JSON", "JSON"),
        ("result.nc", "netCDF", "netCDF"),
    ],
)
@pytest.mark.parametrize("exec_mode", ["sync", "batch"])
def test_save_result_and_download(
    vector_cube, dummy_backend, tmp_path, output_file, format, expected_format, exec_mode
):
    """e.g. https://github.com/Open-EO/openeo-geopyspark-driver/issues/477"""
    vector_cube = vector_cube.save_result(format=format)
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
