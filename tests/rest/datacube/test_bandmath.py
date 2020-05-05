"""

Band math related tests against both
- 0.4.0-style ImageCollectionClient
- 1.0.0-style DataCube

"""

import pytest

from openeo.capabilities import ComparableVersion
from openeo.rest import BandMathException
from .. import get_download_graph
from ..conftest import reset_graphbuilder
from ... import load_json_resource


def test_band_basic(connection, api_version):
    cube = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/%s/band0.json' % api_version)
    assert cube.band(0).graph == expected_graph
    reset_graphbuilder()
    assert cube.band("B02").graph == expected_graph


def test_indexing_040(con040):
    cube = con040.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/0.4.0/band_red.json')
    reset_graphbuilder()
    assert cube.band("B04").graph == expected_graph
    reset_graphbuilder()
    assert cube.band("red").graph == expected_graph
    reset_graphbuilder()
    assert cube.band(2).graph == expected_graph

    cube2 = cube.filter_bands(['B04', 'B03'])
    expected_graph = load_json_resource('data/0.4.0/band_red_filtered.json')
    reset_graphbuilder()
    assert cube2.band("B04").graph == expected_graph
    reset_graphbuilder()
    assert cube2.band("red").graph == expected_graph
    reset_graphbuilder()
    assert cube2.band(0).graph == expected_graph


def test_indexing_100(con100):
    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/1.0.0/band_red.json')
    assert cube.band("B04").graph == expected_graph
    assert cube.band("red").graph == expected_graph
    assert cube.band(2).graph == expected_graph

    cube2 = cube.filter_bands(['red', 'green'])
    expected_graph = load_json_resource('data/1.0.0/band_red_filtered.json')
    assert cube2.band("B04").graph == expected_graph
    assert cube2.band("red").graph == expected_graph
    assert cube2.band(0).graph == expected_graph


def test_evi(connection, api_version):
    cube = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    B02 = cube.band('B02')
    B04 = cube.band('B04')
    B08 = cube.band('B08')
    evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
    actual_graph = get_download_graph(evi_cube)
    expected_graph = load_json_resource('data/%s/evi_graph.json' % api_version)
    assert actual_graph == expected_graph


def test_ndvi_udf(connection, api_version):
    s2_radio = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.apply_tiles("def myfunction(tile):\n"
                                         "    print(tile)\n"
                                         "    return tile")
    actual_graph = get_download_graph(ndvi_coverage)
    expected_graph = load_json_resource('data/%s/udf_graph.json' % api_version)["process_graph"]
    assert actual_graph == expected_graph


def test_ndvi_udf_v100(con100):
    s2_radio = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.reduce_bands_udf("def myfunction(tile):\n"
                                              "    print(tile)\n"
                                              "    return tile")
    actual_graph = get_download_graph(ndvi_coverage)
    expected_graph = load_json_resource('data/1.0.0/udf_graph.json')["process_graph"]
    assert actual_graph == expected_graph


@pytest.mark.parametrize(["process", "expected"], [
    ((lambda b: b + 3), {
        "add1": {"process_id": "add", "arguments": {"x": {"from_node": "arrayelement1"}, "y": 3}, "result": True}
    }),
    ((lambda b: 3 + b), {
        "add1": {"process_id": "add", "arguments": {"x": 3, "y": {"from_node": "arrayelement1"}}, "result": True}
    }),
    ((lambda b: 3 + b + 5), {
        "add1": {"process_id": "add", "arguments": {"x": 3, "y": {"from_node": "arrayelement1"}}},
        "add2": {"process_id": "add", "arguments": {"x": {"from_node": "add1"}, "y": 5}, "result": True}
    }
     ),
    ((lambda b: b - 3), {
        "subtract1": {"process_id": "subtract", "arguments": {"x": {"from_node": "arrayelement1"}, "y": 3},
                      "result": True}
    }),
    ((lambda b: 3 - b), {
        "subtract1": {"process_id": "subtract", "arguments": {"x": 3, "y": {"from_node": "arrayelement1"}},
                      "result": True}
    }),
    ((lambda b: 2 * b), {
        "multiply1": {"process_id": "multiply", "arguments": {"x": 2, "y": {"from_node": "arrayelement1"}},
                      "result": True}
    }),
    ((lambda b: b * 6), {
        "multiply1": {"process_id": "multiply", "arguments": {"x": {"from_node": "arrayelement1"}, "y": 6},
                      "result": True}
    }),
    ((lambda b: -b), {
        "multiply1": {"process_id": "multiply", "arguments": {"x": {"from_node": "arrayelement1"}, "y": -1},
                      "result": True}
    }),
    ((lambda b: b / 8), {
        "divide1": {"process_id": "divide", "arguments": {"x": {"from_node": "arrayelement1"}, "y": 8}, "result": True}
    }),
])
def test_band_operation(con100, process, expected):
    s2 = con100.load_collection("S2")
    b = s2.band('B04')
    c = process(b)

    callback = {"arrayelement1": {
        "process_id": "array_element", "arguments": {"data": {"from_parameter": "data"}, "index": 2}
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
                "reducer": {"process_graph": callback},
                "dimension": "bands",
            },
            "result": True,
        }
    }


def test_merge_issue107(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/107"""
    s2 = con100.load_collection("S2")
    a = s2.filter_bands(['B02'])
    b = s2.filter_bands(['B04'])
    c = a.merge(b)

    flat = c.graph
    # There should be only one `load_collection` node (but two `filter_band` ones)
    processes = sorted(n["process_id"] for n in flat.values())
    assert processes == ["filter_bands", "filter_bands", "load_collection", "merge_cubes"]


def test_invert_band(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (~band)
    assert result.graph == load_json_resource('data/%s/bm_invert_band.json' % api_version)


def test_eq_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (band == 42)
    assert result.graph == load_json_resource('data/%s/bm_eq_scalar.json' % api_version)


def test_gt_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (band > 42)
    assert result.graph == load_json_resource('data/%s/bm_gt_scalar.json' % api_version)


def test_add_sub_mul_div_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (((band + 42) - 10) * 3) / 2
    assert result.graph == load_json_resource('data/%s/bm_add_sub_mul_div_scalar.json' % api_version)


def test_negative(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = -band
    assert result.graph == load_json_resource('data/%s/bm_negative.json' % api_version)


def test_add_bands(connection, api_version):
    cube = connection.load_collection("S2")
    b4 = cube.band("B04")
    b3 = cube.band("B03")
    result = b4 + b3
    assert result.graph == load_json_resource('data/%s/bm_add_bands.json' % api_version)


def test_add_bands_different_collection(connection, api_version):
    if api_version == "0.4.0":
        pytest.skip("0.4.0 generates invalid result")
    b4 = connection.load_collection("S2").band("B04")
    b3 = connection.load_collection("SENTINEL2_RADIOMETRY_10M").band("B02")
    with pytest.raises(BandMathException):
        # TODO #123 implement band math with bands of different collections
        b4 + b3


def test_logical_not_equal(connection, api_version):
    s2 = connection.load_collection("SENTINEL2_SCF")
    scf_band = s2.band("SCENECLASSIFICATION")
    mask = scf_band != 4
    actual = get_download_graph(mask)
    assert actual == load_json_resource('data/%s/notequal.json' % api_version)


def test_logical_or(connection, api_version):
    s2 = connection.load_collection("SENTINEL2_SCF")
    scf_band = s2.band("SCENECLASSIFICATION")
    mask = (scf_band == 2) | (scf_band == 5)
    actual = get_download_graph(mask)
    assert actual == load_json_resource('data/%s/logical_or.json' % api_version)


def test_logical_and(connection, api_version):
    s2 = connection.load_collection("SENTINEL2_SCF")
    b1 = s2.band("SCENECLASSIFICATION")
    b2 = s2.band("MSK")
    mask = (b1 == 2) & (b2 == 5)
    actual = get_download_graph(mask)
    assert actual == load_json_resource('data/%s/logical_and.json' % api_version)


def test_merge_cubes_or(connection, api_version):
    s2 = connection.load_collection("S2")
    b1 = s2.band("B02") > 1
    b2 = s2.band("B03") > 2
    b1 = b1.linear_scale_range(0, 1, 0, 2)
    b2 = b2.linear_scale_range(0, 1, 0, 2)
    combined = b1 | b2
    actual = get_download_graph(combined)
    assert actual == load_json_resource('data/%s/merge_cubes_or.json' % api_version)


def test_merge_cubes_multiple(connection, api_version):
    if api_version == "0.4.0":
        pytest.skip("doesn't work in 0.4.0")
    s2 = connection.load_collection("S2")
    b1 = s2.band("B02")
    b1 = b1.linear_scale_range(0, 1, 0, 2)
    combined = b1 + b1 + b1
    actual = get_download_graph(combined)
    assert sorted(n["process_id"] for n in actual.values()) == [
        "linear_scale_range", "load_collection",
        "merge_cubes", "merge_cubes", "reduce_dimension", "save_result"]
    assert actual == load_json_resource('data/%s/merge_cubes_multiple.json' % api_version)


def test_merge_cubes_no_resolver(connection, api_version):
    s2 = connection.load_collection("S2")
    mask = connection.load_collection("MASK")
    merged = s2.merge(mask)
    assert merged.graph == load_json_resource('data/%s/merge_cubes_no_resolver.json' % api_version)


def test_merge_cubes_max_resolver(connection, api_version):
    s2 = connection.load_collection("S2")
    mask = connection.load_collection("MASK")
    merged = s2.merge(mask, overlap_resolver="max")
    assert merged.graph == load_json_resource('data/%s/merge_cubes_max.json' % api_version)
