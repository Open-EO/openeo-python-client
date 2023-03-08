"""

Band math related tests against both
- 0.4.0-style ImageCollectionClient
- 1.0.0-style DataCube

"""

import numpy as np
import pytest

import openeo
from openeo.rest import BandMathException
from .. import get_download_graph
from ... import load_json_resource
from .test_datacube import _get_leaf_node


def test_band_basic(connection, api_version):
    cube = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/%s/band0.json' % api_version)
    assert cube.band(0).flat_graph() == expected_graph
    assert cube.band("B02").flat_graph() == expected_graph



def test_indexing_100(con100):
    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    expected_graph = load_json_resource('data/1.0.0/band_red.json')
    assert cube.band("B04").flat_graph() == expected_graph
    assert cube.band("red").flat_graph() == expected_graph
    assert cube.band(2).flat_graph() == expected_graph

    cube2 = cube.filter_bands(['red', 'green'])
    expected_graph = load_json_resource('data/1.0.0/band_red_filtered.json')
    assert cube2.band("B04").flat_graph() == expected_graph
    assert cube2.band("red").flat_graph() == expected_graph
    assert cube2.band(0).flat_graph() == expected_graph


def test_evi(connection, api_version):
    cube = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    B02 = cube.band('B02')
    B04 = cube.band('B04')
    B08 = cube.band('B08')
    evi_cube = (2.5 * (B08 - B04)) / ((B08 + 6.0 * B04 - 7.5 * B02) + 1.0)
    actual_graph = get_download_graph(evi_cube)
    expected_graph = load_json_resource('data/%s/evi_graph.json' % api_version)
    assert actual_graph == expected_graph


@pytest.mark.parametrize("process", [
    (lambda b2: b2 ** 3.14),
    (lambda b2: b2.power(3.14)),
])
def test_power(con100, process):
    b2 = con100.load_collection("SENTINEL2_RADIOMETRY_10M").band("B02")
    res = process(b2)
    assert _get_leaf_node(res) == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "bands",
            "reducer": {"process_graph": {
                "arrayelement1": {
                    "process_id": "array_element",
                    "arguments": {"data": {"from_parameter": "data"}, "index": 0},
                },
                "power1": {
                    "process_id": "power",
                    "arguments": {"base": {"from_node": "arrayelement1"}, "p": 3.14},
                    "result": True}
            }}
        },
        "result": True}


@pytest.mark.parametrize("process", [
    (lambda b2: 2 ** b2),
    # TODO: non-operator way to express `2 ** b2` band math?
])
def test_power_reverse(con100, process):
    b2 = con100.load_collection("SENTINEL2_RADIOMETRY_10M").band("B02")
    res = process(b2)
    assert _get_leaf_node(res) == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "bands",
            "reducer": {"process_graph": {
                "arrayelement1": {
                    "process_id": "array_element",
                    "arguments": {"data": {"from_parameter": "data"}, "index": 0},
                },
                "power1": {
                    "process_id": "power",
                    "arguments": {"base": 2, "p": {"from_node": "arrayelement1"}},
                    "result": True}
            }}
        },
        "result": True}


def test_db_to_natural(con100):
    cube = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    B02 = cube.band('B02')
    natural = 10 ** ((B02 * 0.001 - 45) / 10)
    expected_graph = load_json_resource('data/1.0.0/db_to_natural.json')
    assert natural.flat_graph() == expected_graph


def test_ndvi_reduce_bands_udf(connection, api_version):
    # TODO #181 #312 drop this deprecated pattern
    s2_radio = connection.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.reduce_bands_udf("def myfunction(tile):\n    print(tile)\n    return tile")
    actual_graph = get_download_graph(ndvi_coverage)
    expected_graph = load_json_resource('data/%s/udf_graph.json' % api_version)["process_graph"]
    assert actual_graph == expected_graph


def test_ndvi_reduce_bands_udf_legacy_v100(con100):
    # TODO #181 #312 drop this deprecated pattern
    s2_radio = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.reduce_bands_udf("def myfunction(tile):\n    print(tile)\n    return tile")
    actual_graph = get_download_graph(ndvi_coverage)
    expected_graph = load_json_resource('data/1.0.0/udf_graph.json')["process_graph"]
    assert actual_graph == expected_graph


def test_ndvi_reduce_bands_udf_v100(con100):
    s2_radio = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    ndvi_coverage = s2_radio.reduce_bands(openeo.UDF("def myfunction(tile):\n    print(tile)\n    return tile"))
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
    assert c.flat_graph() == {
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


def test_invert_band(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (~band)
    assert result.flat_graph() == load_json_resource('data/%s/bm_invert_band.json' % api_version)


def test_eq_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (band == 42)
    assert result.flat_graph() == load_json_resource('data/%s/bm_eq_scalar.json' % api_version)


def test_gt_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (band > 42)
    assert result.flat_graph() == load_json_resource('data/%s/bm_gt_scalar.json' % api_version)


@pytest.mark.parametrize(["operation", "expected"], (
        (lambda b: b == 42, "eq"),
        (lambda b: b != 42, "neq"),
        (lambda b: b > 42, "gt"),
        (lambda b: b >= 42, "gte"),
        (lambda b: b < 42, "lt"),
        (lambda b: b <= 42, "lte"),
))
def test_comparison(connection, api_version, operation, expected):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = operation(band)
    assert result.flat_graph() == load_json_resource(
        'data/%s/bm_comparison.json' % api_version,
        preprocess=lambda data: data.replace("OPERATOR", expected)
    )


def test_add_sub_mul_div_scalar(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = (((band + 42) - 10) * 3) / 2
    assert result.flat_graph() == load_json_resource('data/%s/bm_add_sub_mul_div_scalar.json' % api_version)


def test_negative(connection, api_version):
    cube = connection.load_collection("S2")
    band = cube.band('B04')
    result = -band
    assert result.flat_graph() == load_json_resource('data/%s/bm_negative.json' % api_version)


def test_add_bands(connection, api_version):
    cube = connection.load_collection("S2")
    b4 = cube.band("B04")
    b3 = cube.band("B03")
    result = b4 + b3
    assert result.flat_graph() == load_json_resource('data/%s/bm_add_bands.json' % api_version)


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
        "apply", "load_collection",
        "merge_cubes", "merge_cubes", "reduce_dimension", "save_result"]
    assert actual == load_json_resource('data/%s/merge_cubes_multiple.json' % api_version)


def test_fuzzy_mask(connection, api_version):
    s2 = connection.load_collection("SENTINEL2_SCF")
    scf_band = s2.band("SCENECLASSIFICATION")
    clouds = scf_band == 4
    fuzzy = clouds.apply_kernel(kernel=0.1 * np.ones((3, 3)))
    mask = fuzzy > 0.3
    assert mask.flat_graph() == load_json_resource('data/%s/fuzzy_mask.json' % api_version)


def test_fuzzy_mask_band_math(con100):
    s2 = con100.load_collection("SENTINEL2_SCF")
    scf_band = s2.band("SCENECLASSIFICATION")
    clouds = scf_band == 4
    fuzzy = clouds.apply_kernel(kernel=0.1 * np.ones((3, 3)))
    mask = fuzzy.add_dimension("bands", "mask", "bands").band("mask") > 0.3
    assert mask.flat_graph() == load_json_resource('data/1.0.0/fuzzy_mask_add_dim.json')


def test_normalized_difference(connection, api_version):
    cube = connection.load_collection("S2")
    nir = cube.band("B08")
    red = cube.band("B04")

    result = nir.normalized_difference(red)

    assert result.flat_graph() == load_json_resource('data/%s/bm_nd_bands.json' % api_version)


def test_ln(con100):
    result = con100.load_collection("S2").band('B04').ln()
    assert result.flat_graph() == load_json_resource('data/1.0.0/bm_ln.json')


def test_log10(con100):
    result = con100.load_collection("S2").band('B04').log10()
    assert result.flat_graph() == load_json_resource('data/1.0.0/bm_log.json')


def test_log2(con100):
    result = con100.load_collection("S2").band('B04').log2()
    assert result.flat_graph() == load_json_resource(
        'data/1.0.0/bm_log.json',
        preprocess=lambda s: s.replace('"base": 10', '"base": 2')
    )


def test_log3(con100):
    result = con100.load_collection("S2").band('B04').logarithm(base=3)
    assert result.flat_graph() == load_json_resource(
        'data/1.0.0/bm_log.json',
        preprocess=lambda s: s.replace('"base": 10', '"base": 3')
    )
