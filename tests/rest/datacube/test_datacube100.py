"""

Unit tests specifically for 1.0.0-style DataCube

"""
import pytest
import shapely.geometry

import openeo.metadata
from openeo.internal.graph_building import PGNode
from openeo.rest.connection import Connection
from .conftest import API_URL
from ... import load_json_resource


def test_mask_polygon(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.mask_polygon(mask=polygon)
    assert sorted(masked.graph.keys()) == ["loadcollection1", "maskpolygon1"]
    assert masked.graph["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            'mask': {
                'coordinates': (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                'crs': {'properties': {'name': 'EPSG:4326'}, 'type': 'name'},
                'type': 'Polygon'}
        },
        "result": True
    }


def test_mask_polygon_path(con100: Connection):
    img = con100.load_collection("S2")
    masked = img.mask_polygon(mask="path/to/polygon.json")
    assert sorted(masked.graph.keys()) == ["loadcollection1", "maskpolygon1", "readvector1"]
    assert masked.graph["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "readvector1"},
        },
        "result": True
    }
    assert masked.graph["readvector1"] == {
        "process_id": "read_vector",
        "arguments": {"filename": "path/to/polygon.json"},
    }


def test_mask_raster(con100: Connection):
    img = con100.load_collection("S2")
    mask = con100.load_collection("MASK")
    masked = img.mask(mask=mask, replacement=102)
    assert masked.graph["mask1"] == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "loadcollection2"},
            "replacement": 102
        },
        "result": True
    }


def test_merge_cubes(con100: Connection):
    a = con100.load_collection("S2")
    b = con100.load_collection("MASK")
    c = a.merge(b)
    assert c.graph["mergecubes1"] == {
        "process_id": "merge_cubes",
        "arguments": {
            "cube1": {"from_node": "loadcollection1"},
            "cube2": {"from_node": "loadcollection2"},
        },
        "result": True
    }

def test_resample_spatial(con100: Connection):
    data = con100.load_collection("S2")
    target = con100.load_collection("MASK")
    im = data.resample_cube_spatial(target,method='spline')
    print(im.graph)
    assert im.graph["resamplecubespatial1"] == {
        'arguments': {
           'data': {'from_node': 'loadcollection1'},
           'method': 'spline',
           'target': {'from_node': 'loadcollection2'}
        },
        'process_id': 'resample_cube_spatial',
        'result': True}

def test_ndvi_simple(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi()
    assert sorted(ndvi.graph.keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.graph["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}},
        "result": True,
    }


def test_ndvi_args(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi(nir="nirr", red="rred", target_band="ndvii")
    assert sorted(ndvi.graph.keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.graph["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}, "nir": "nirr", "red": "rred", "target_band": "ndvii"},
        "result": True,
    }


def test_rename_dimension(con100):
    s2 = con100.load_collection("S2")
    x = s2.rename_dimension(source="bands", target="ThisIsNotTheBandsDimension")
    assert x.graph == {
        'loadcollection1': {
            'arguments': {
                'id': 'S2',
                'spatial_extent': None,
                'temporal_extent': None
            },
            'process_id': 'load_collection'
        },
        'renamedimension1': {
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'source': 'bands',
                'target': 'ThisIsNotTheBandsDimension'
            },
            'process_id': 'rename_dimension',
            'result': True
        }
    }


def test_reduce_dimension(con100):
    s2 = con100.load_collection("S2")
    x = s2.reduce_dimension(dimension="bands", reducer="mean")
    assert x.graph == {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        },
        'reducedimension1': {
            'process_id': 'reduce_dimension',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'dimension': 'bands',
                'reducer': {'process_graph': {
                    'mean1': {
                        'process_id': 'mean',
                        'arguments': {'data': {'from_parameter': 'data'}},
                        'result': True
                    }
                }}
            },
            'result': True
        }}


def test_reduce_dimension_binary(con100):
    s2 = con100.load_collection("S2")
    reducer = PGNode(
        process_id="add",
        arguments={"x": {"from_parameter": "x"}, "y": {"from_parameter": "y"}},
    )
    x = s2.reduce_dimension(dimension="bands", reducer=reducer, process_id="reduce_dimension_binary")
    assert x.graph == {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        },
        'reducedimensionbinary1': {
            'process_id': 'reduce_dimension_binary',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'dimension': 'bands',
                'reducer': {'process_graph': {
                    'add1': {
                        'process_id': 'add',
                        'arguments': {'x': {'from_parameter': 'x'}, 'y': {'from_parameter': 'y'}},
                        'result': True
                    }
                }}
            },
            'result': True
        }}


def test_reduce_dimension_name(con100, requests_mock):
    requests_mock.get(API_URL + "/collections/S22", json={
        "cube:dimensions": {
            "color": {"type": "bands", "values": ["cyan", "magenta", "yellow", "black"]},
            "alpha": {"type": "spatial"},
            "date": {"type": "temporal"}
        }
    })
    s22 = con100.load_collection("S22")

    for dim in ["color", "alpha", "date"]:
        cube = s22.reduce_dimension(dimension=dim, reducer="sum")
        assert cube.graph["reducedimension1"] == {
            "process_id": "reduce_dimension",
            "arguments": {
                'data': {'from_node': 'loadcollection1'},
                'dimension': dim,
                'reducer': {'process_graph': {
                    'sum1': {
                        'process_id': 'sum',
                        'arguments': {'data': {'from_parameter': 'data'}},
                        'result': True
                    }
                }}
            },
            'result': True
        }

    with pytest.raises(ValueError, match="Invalid dimension 'wut'"):
        s22.reduce_dimension(dimension="wut", reducer="sum")


def test_metadata_load_collection_100(con100, requests_mock):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={
        "cube:dimensions": {
            "bands": {"type": "bands", "values": ["B2", "B3"]}
        },
        "summaries": {
            "eo:bands": [
                {"name": "B2", "common_name": "blue"},
                {"name": "B3", "common_name": "green"},
            ]
        }
    })
    im = con100.load_collection('SENTINEL2')
    assert im.metadata.bands == [
        openeo.metadata.Band("B2", "blue", None),
        openeo.metadata.Band("B3", "green", None)
    ]


def test_apply_absolute_pgnode(con100):
    im = con100.load_collection("S2")
    result = im.apply(PGNode(process_id="absolute", arguments={"x": {"from_parameter": "x"}}))
    expected_graph = load_json_resource('data/1.0.0/apply_absolute.json')
    assert result.graph == expected_graph


def test_load_collection_properties(con100):
    # TODO: put this somewhere and expose it to the user?
    def eq(value, case_sensitive=True) -> PGNode:
        return PGNode(
            process_id="eq",
            arguments={"x": {"from_parameter": "value"}, "y": value, "case_sensitive": case_sensitive}
        )

    def between(min, max) -> PGNode:
        return PGNode(process_id="between", arguments={"x": {"from_parameter": "value"}, "min": min, "max": max})

    im = con100.load_collection(
        "S2",
        spatial_extent={"west": 16.1, "east": 16.6, "north": 48.6, "south": 47.2},
        temporal_extent=["2018-01-01", "2019-01-01"],
        properties={
            "eo:cloud_cover": between(min=0, max=50),
            "platform": eq("Sentinel-2B", case_sensitive=False)
        }
    )

    expected = load_json_resource('data/1.0.0/load_collection_properties.json')
    assert im.graph == expected


def test_apply_dimension_temporal_cumsum_with_target(con100):
    cumsum = con100.load_collection("S2").apply_dimension('cumsum', dimension="t", target_dimension="MyNewTime")
    actual_graph = cumsum.graph
    expected_graph = load_json_resource('data/1.0.0/apply_dimension_temporal_cumsum.json')
    expected_graph['applydimension1']['arguments']['target_dimension'] = 'MyNewTime'
    expected_graph['applydimension1']['result'] = True
    del expected_graph['saveresult1']
    assert actual_graph == expected_graph


def test_filter_spatial_callbak(con100):
    """
    Experiment test showing how to introduce a callback for preprocessing process arguments
    https://github.com/Open-EO/openeo-processes/issues/156
    @param con100:
    @return:
    """
    collection = con100.load_collection("S2")

    point_to_bbox_callback = PGNode(process_id="run_udf", arguments={
        "data": {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [125.6, 10.1]
                }
            }]
        },
        "runtime": "Python",
        "udf": "def transform_point_into_bbox(data:UdfData): blabla"
    })

    filtered_collection = collection.process("filter_spatial", {
        "data": collection._pg,
        "geometries": point_to_bbox_callback
    })

    assert filtered_collection.graph == {
        'filterspatial1': {
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'geometries': {'from_node': 'runudf1'}
            },
            'process_id': 'filter_spatial',
            'result': True
        },
        'loadcollection1': {
            'arguments': {
                'id': 'S2',
                'spatial_extent': None,
                'temporal_extent': None
            },
            'process_id': 'load_collection'
        },
        'runudf1': {
            'arguments': {
                'data': {
                    'features': [{'geometry': {'coordinates': [125.6, 10.1], 'type': 'Point'}, 'type': 'Feature'}],
                    'type': 'FeatureCollection'
                },
                'runtime': 'Python',
                'udf': 'def transform_point_into_bbox(data:UdfData): blabla'
            },
            'process_id': 'run_udf'}
    }


def test_custom_process_kwargs_datacube(con100: Connection):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", data=img, bar=123)
    expected = load_json_resource('data/1.0.0/process_foo.json')
    assert res.graph == expected


def test_custom_process_kwargs_datacube_pg(con100: Connection):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", data=img._pg, bar=123)
    expected = load_json_resource('data/1.0.0/process_foo.json')
    assert res.graph == expected


def test_custom_process_arguments_datacube(con100: Connection):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", arguments={"data": img, "bar": 123})
    expected = load_json_resource('data/1.0.0/process_foo.json')
    assert res.graph == expected


def test_custom_process_arguments_datacube_pg(con100: Connection):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", arguments={"data": img._pg, "bar": 123})
    expected = load_json_resource('data/1.0.0/process_foo.json')
    assert res.graph == expected


def test_save_user_defined_process(con100, requests_mock):
    expected_body = load_json_resource("data/1.0.0/save_user_defined_process.json")

    def check_body(request):
        assert request.json()['process_graph'] == expected_body['process_graph']
        return True

    requests_mock.put(API_URL + "/process_graphs/my_udp", additional_matcher=check_body)

    collection = con100.load_collection("S2") \
        .filter_bbox(west=16.1, east=16.6, north=48.6, south=47.2) \
        .filter_temporal(start_date="2018-01-01", end_date="2019-01-01")

    collection.save_user_defined_process(user_defined_process_id='my_udp')
