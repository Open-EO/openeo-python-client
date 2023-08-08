from pathlib import Path

import pytest
import shapely.geometry

from openeo.api.process import Parameter
from openeo.internal.graph_building import PGNode
from openeo.rest._testing import DummyBackend
from openeo.rest.vectorcube import VectorCube


@pytest.fixture
def vector_cube(con100) -> VectorCube:
    pgnode = PGNode(process_id="create_vector_cube")
    return VectorCube(graph=pgnode, connection=con100)


def test_raster_to_vector(con100):
    img = con100.load_collection("S2")
    vector_cube = img.raster_to_vector()
    vector_cube_tranformed = vector_cube.run_udf(udf="python source code", runtime="Python")

    assert vector_cube_tranformed.flat_graph() == {
        'loadcollection1': {
            'arguments': {
                'id': 'S2',
                'spatial_extent': None,
                'temporal_extent': None
            },
            'process_id': 'load_collection'
        },
        'rastertovector1': {
            'arguments': {
                'data': {'from_node': 'loadcollection1'}
            },
            'process_id': 'raster_to_vector'
        },
        'runudf1': {
            'arguments': {
                'data': {'from_node': 'rastertovector1'},
                'runtime': 'Python',
                'udf': 'python source code'
            },
            'process_id': 'run_udf',
            'result': True}
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
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
                "format": expected_format,
                "options": {},
            },
            "result": True,
        },
    }
    assert output_path.read_bytes() == DummyBackend.DEFAULT_RESULT
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
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
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
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "createvectorcube1"},
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
        "createvectorcube1": {"process_id": "create_vector_cube", "arguments": {}},
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "createvectorcube1"}, "format": expected_format, "options": {}},
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
    vc.execute()
    assert dummy_backend.get_pg() == {
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
    vc.execute()
    assert dummy_backend.get_pg() == {
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
    vc.execute()
    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"from_parameter": "data"}, "properties": []},
            "result": True,
        }
    }


def test_load_url(con100, dummy_backend):
    vc = VectorCube.load_url(connection=con100, url="https://example.com/geometry.json", format="GeoJSON")
    assert isinstance(vc, VectorCube)
    vc.execute()
    assert dummy_backend.get_pg() == {
        "loadurl1": {
            "process_id": "load_url",
            "arguments": {"url": "https://example.com/geometry.json", "format": "GeoJSON"},
            "result": True,
        }
    }


def test_apply_dimension(con100, dummy_backend):
    vc = con100.load_geojson({"type": "Point", "coordinates": [1, 2]})
    result = vc.apply_dimension("sort", dimension="geometries")
    result.execute()
    assert dummy_backend.get_pg() == {
        "loadgeojson1": {
            "process_id": "load_geojson",
            "arguments": {"data": {"coordinates": [1, 2], "type": "Point"}, "properties": []},
        },
        "applydimension1": {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "loadgeojson1"},
                "dimension": "geometries",
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
        },
    }
