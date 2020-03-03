import openeo
import pytest
from openeo.rest.connection import Connection

from ... import load_json_resource, get_download_graph

API_URL = "https://oeo.net"


@pytest.fixture
def con100(requests_mock) -> Connection:
    """
    Fixture to have a v1.0.0 connection to a backend
    with a some default image collections
    """
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    s2_properties = {
        "properties": {
            "cube:dimensions": {
                "bands": {"type": "bands", "values": ["B02", "B03", "B04", "B08"]}
            },
            "eo:bands": [
                {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
                {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
                {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351},
            ]
        }
    }
    # Classic Sentinel2 collection
    requests_mock.get(API_URL + "/collections/SENTINEL2_RADIOMETRY_10M", json=s2_properties)
    # Alias for quick tests
    requests_mock.get(API_URL + "/collections/S2", json=s2_properties)
    requests_mock.get(API_URL + "/collections/MASK", json={})
    return openeo.connect(API_URL)


def test_band_basic(con100):
    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/1.0.0/band0.json')
    assert cube.band(0).graph == expected_graph
    assert cube.band("B02").graph == expected_graph
    # TODO graph contains "spectral_band" hardcoded


def test_indexing(con100):
    def check_cube(cube, band_index):
        assert cube.band(band_index).graph == expected_graph
        assert cube.band("B04").graph == expected_graph
        assert cube.band("red").graph == expected_graph

    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/1.0.0/band_red.json')
    check_cube(cube, 2)

    cube2 = cube.filter_bands(['red', 'green'])
    expected_graph = load_json_resource('data/1.0.0/band_red_filtered.json')
    check_cube(cube2, 0)


def test_evi(con100):
    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    B02 = cube.band('B02')
    B04 = cube.band('B04')
    B08 = cube.band('B08')
    evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
    actual_graph = get_download_graph(evi_cube)
    expected_graph = load_json_resource('data/1.0.0/evi_graph.json')
    assert actual_graph == expected_graph


def test_ndvi_udf(con100, requests_mock):
    s2_radio = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.reduce_bands_udf("def myfunction(tile):\n"
                                              "    print(tile)\n"
                                              "    return tile")
    actual_graph = get_download_graph(ndvi_coverage)
    expected_graph = load_json_resource('data/1.0.0/udf_graph.json')["process_graph"]
    assert actual_graph == expected_graph


@pytest.mark.parametrize(["process", "expected"], [
    ((lambda b: b + 3), {
        "sum1": {"process_id": "sum", "arguments": {"data": [{"from_node": "arrayelement1"}, 3]}, "result": True}
    }),
    ((lambda b: 3 + b), {
        "sum1": {"process_id": "sum", "arguments": {"data": [3, {"from_node": "arrayelement1"}]}, "result": True}
    }),
    ((lambda b: 3 + b + 5), {
        "sum1": {"process_id": "sum", "arguments": {"data": [3, {"from_node": "arrayelement1"}]}},
        "sum2": {"process_id": "sum", "arguments": {"data": [{"from_node": "sum1"}, 5]}, "result": True}
    }
     ),
    ((lambda b: b - 3), {
        "subtract1": {"process_id": "subtract", "arguments": {"data": [{"from_node": "arrayelement1"}, 3]},
                      "result": True}
    }),
    ((lambda b: 3 - b), {
        "subtract1": {"process_id": "subtract", "arguments": {"data": [3, {"from_node": "arrayelement1"}]},
                      "result": True}
    }),
    ((lambda b: 2 * b), {
        "product1": {"process_id": "product", "arguments": {"data": [2, {"from_node": "arrayelement1"}]},
                     "result": True}
    }),
    ((lambda b: b * 6), {
        "product1": {"process_id": "product", "arguments": {"data": [{"from_node": "arrayelement1"}, 6]},
                     "result": True}
    }),
    ((lambda b: b / 8), {
        "divide1": {"process_id": "divide", "arguments": {"data": [{"from_node": "arrayelement1"}, 8]}, "result": True}
    }),
])
def test_band_operation(con100, process, expected):
    s2 = con100.load_collection("S2")
    b = s2.band('B04')
    c = process(b)

    callback = {"arrayelement1": {
        "process_id": "array_element", "arguments": {"data": {"from_argument": "data"}, "index": 2}
    }}
    callback.update(expected)
    assert c.graph == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None}
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "reducer": {"callback": callback},
                "dimension": "spectral_bands",
            },
            "result": True,
        }
    }


@pytest.mark.skip("TODO issue #107")
def test_merge_issue107(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/107"""
    s2 = con100.load_collection("S2")
    a = s2.filter_bands(['B02'])
    b = s2.filter_bands(['B04'])
    c = a.merge(b)

    flat = c.graph
    # There should be only one `load_collection` node (but two `filter_band` ones)
    processes = sorted(n["process_id"] for n in flat.values())
    assert processes == ["filter_bands", "filter_bands", "merge_cubes", "load_collection"]


def test_reduce_dimension_binary(con100):
    s2 = con100.load_collection("S2")
    callback = {
        "process_id": "add",
        "arguments": {"x": {"from_argument": "x"}, "y": {"from_argument": "y"}}
    }
    # TODO: use a public version of reduce_dimension_binary?
    x = s2._reduce(dimension="bands", callback=callback, process_id="reduce_dimension_binary")
    assert x.graph == {
        'loadcollection1': {
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
            'process_id': 'load_collection',
        },
        'reducedimensionbinary1': {
            'process_id': 'reduce_dimension_binary',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'dimension': 'bands',
                'reducer': {'callback': {
                    'add1': {
                        'process_id': 'add',
                        'arguments': {'x': {'from_argument': 'x'}, 'y': {'from_argument': 'y'}},
                        'result': True
                    }
                }}
            },
            'result': True
        }}
