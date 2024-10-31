"""

Unit tests specifically for 1.0.0-style DataCube

"""
import collections
import copy
import io
import json
import logging
import pathlib
import re
import textwrap
from typing import Optional

import pyproj
import pytest
import requests
import shapely.geometry

import openeo.metadata
import openeo.processes
from openeo import collection_property
from openeo.api.process import Parameter
from openeo.capabilities import ComparableVersion
from openeo.internal.graph_building import PGNode
from openeo.internal.process_graph_visitor import ProcessGraphVisitException
from openeo.internal.warnings import UserDeprecationWarning
from openeo.processes import ProcessBuilder
from openeo.rest import OpenEoClientException
from openeo.rest.connection import Connection
from openeo.rest.datacube import THIS, UDF, DataCube

from .. import get_download_graph
from .conftest import API_URL, DEFAULT_S2_METADATA, setup_collection_metadata

basic_geometry_types = [
    (
        shapely.geometry.box(0, 0, 1, 1),
        {"type": "Polygon", "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]]},
    ),
    (
        {"type": "Polygon", "coordinates": [[[1, 0], [1, 1], [0, 1], [0, 0], [1, 0]]]},
        {"type": "Polygon", "coordinates": [[[1, 0], [1, 1], [0, 1], [0, 0], [1, 0]]]},
    ),
    (
        shapely.geometry.MultiPolygon([shapely.geometry.box(0, 0, 1, 1)]),
        {"type": "MultiPolygon", "coordinates": [[[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]]]},
    ),
    (
        shapely.geometry.GeometryCollection([shapely.geometry.box(0, 0, 1, 1)]),
        {
            "type": "GeometryCollection",
            "geometries": [
                {"type": "Polygon", "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]]}
            ],
        },
    ),
    (
        {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Polygon", "coordinates": [[[1, 0], [1, 1], [0, 1], [0, 0], [1, 0]]]},
        },
        {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Polygon", "coordinates": [[[1, 0], [1, 1], [0, 1], [0, 0], [1, 0]]]},
        },
    ),
]


WKT2_FOR_EPSG23631 = """
PROJCRS["WGS 84 / UTM zone 31N",
    BASEGEOGCRS["WGS 84",
        ENSEMBLE["World Geodetic System 1984 ensemble",
            MEMBER["World Geodetic System 1984 (Transit)"],
            MEMBER["World Geodetic System 1984 (G730)"],
            MEMBER["World Geodetic System 1984 (G873)"],
            MEMBER["World Geodetic System 1984 (G1150)"],
            MEMBER["World Geodetic System 1984 (G1674)"],
            MEMBER["World Geodetic System 1984 (G1762)"],
            MEMBER["World Geodetic System 1984 (G2139)"],
            ELLIPSOID["WGS 84",6378137,298.257223563,
                LENGTHUNIT["metre",1]],
            ENSEMBLEACCURACY[2.0]],
        PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433]],
        ID["EPSG",4326]],
    CONVERSION["UTM zone 31N",
        METHOD["Transverse Mercator",
            ID["EPSG",9807]],
        PARAMETER["Latitude of natural origin",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8801]],
        PARAMETER["Longitude of natural origin",3,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8802]],
        PARAMETER["Scale factor at natural origin",0.9996,
            SCALEUNIT["unity",1],
            ID["EPSG",8805]],
        PARAMETER["False easting",500000,
            LENGTHUNIT["metre",1],
            ID["EPSG",8806]],
        PARAMETER["False northing",0,
            LENGTHUNIT["metre",1],
            ID["EPSG",8807]]],
    CS[Cartesian,2],
        AXIS["(E)",east,
            ORDER[1],
            LENGTHUNIT["metre",1]],
        AXIS["(N)",north,
            ORDER[2],
            LENGTHUNIT["metre",1]],
    USAGE[
        SCOPE["Engineering survey, topographic mapping."],
        AREA["Between 0°E and 6°E, northern hemisphere between equator and 84°N, onshore and offshore. Algeria. Andorra. Belgium. Benin. Burkina Faso. Denmark - North Sea. France. Germany - North Sea. Ghana. Luxembourg. Mali. Netherlands. Niger. Nigeria. Norway. Spain. Togo. United Kingdom (UK) - North Sea."],
        BBOX[0,0,84,6]],
    ID["EPSG",32631]]
"""


PROJJSON_FOR_EPSG23631 = {
    "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
    "type": "ProjectedCRS",
    "name": "WGS 84 / UTM zone 31N",
    "base_crs": {
        "name": "WGS 84",
        "datum_ensemble": {
            "name": "World Geodetic System 1984 ensemble",
            "members": [
                {"name": "World Geodetic System 1984 (Transit)", "id": {"authority": "EPSG", "code": 1166}},
                {"name": "World Geodetic System 1984 (G730)", "id": {"authority": "EPSG", "code": 1152}},
                {"name": "World Geodetic System 1984 (G873)", "id": {"authority": "EPSG", "code": 1153}},
                {"name": "World Geodetic System 1984 (G1150)", "id": {"authority": "EPSG", "code": 1154}},
                {"name": "World Geodetic System 1984 (G1674)", "id": {"authority": "EPSG", "code": 1155}},
                {"name": "World Geodetic System 1984 (G1762)", "id": {"authority": "EPSG", "code": 1156}},
                {"name": "World Geodetic System 1984 (G2139)", "id": {"authority": "EPSG", "code": 1309}},
            ],
            "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
            "accuracy": "2.0",
            "id": {"authority": "EPSG", "code": 6326},
        },
        "coordinate_system": {
            "subtype": "ellipsoidal",
            "axis": [
                {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"},
            ],
        },
        "id": {"authority": "EPSG", "code": 4326},
    },
    "conversion": {
        "name": "UTM zone 31N",
        "method": {"name": "Transverse Mercator", "id": {"authority": "EPSG", "code": 9807}},
        "parameters": [
            {
                "name": "Latitude of natural origin",
                "value": 0,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8801},
            },
            {
                "name": "Longitude of natural origin",
                "value": 3,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8802},
            },
            {
                "name": "Scale factor at natural origin",
                "value": 0.9996,
                "unit": "unity",
                "id": {"authority": "EPSG", "code": 8805},
            },
            {"name": "False easting", "value": 500000, "unit": "metre", "id": {"authority": "EPSG", "code": 8806}},
            {"name": "False northing", "value": 0, "unit": "metre", "id": {"authority": "EPSG", "code": 8807}},
        ],
    },
    "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
            {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
            {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"},
        ],
    },
    "scope": "Engineering survey, topographic mapping.",
    "area": "Between 0°E and 6°E, northern hemisphere between equator and 84°N, onshore and offshore. Algeria. Andorra. Belgium. Benin. Burkina Faso. Denmark - North Sea. France. Germany - North Sea. Ghana. Luxembourg. Mali. Netherlands. Niger. Nigeria. Norway. Spain. Togo. United Kingdom (UK) - North Sea.",
    "bbox": {"south_latitude": 0, "west_longitude": 0, "north_latitude": 84, "east_longitude": 6},
    "id": {"authority": "EPSG", "code": 32631},
}


def _get_normalizable_crs_inputs():
    """
    Dynamic (proj version based) generation of supported CRS inputs (to normalize).
    :return:
    """
    yield "EPSG:32631"
    yield 32631
    yield "32631"
    yield "+proj=utm +zone=31 +datum=WGS84 +units=m +no_defs"  # is also EPSG:32631, in proj format
    yield WKT2_FOR_EPSG23631


def _get_leaf_node(cube: DataCube) -> dict:
    """Get leaf node (node with result=True), supporting old and new style of graph building."""
    # TODO replace this with get_download_graph
    flat_graph = cube.flat_graph()
    (node,) = [n for n in flat_graph.values() if n.get("result")]
    return node


def test_datacube_flat_graph(con100):
    s2cube = con100.load_collection("S2")
    assert s2cube.flat_graph() == {'loadcollection1': {
        'process_id': 'load_collection',
        'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        'result': True
    }}


@pytest.mark.parametrize(["kwargs", "expected"], [
    ({"west": 3, "south": 51, "east": 4, "north": 52}, {"west": 3, "south": 51, "east": 4, "north": 52}),
    (
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326},
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
    ),
    ({"bbox": [3, 51, 4, 52]}, {"west": 3, "south": 51, "east": 4, "north": 52}),
    ({"bbox": (3, 51, 4, 52)}, {"west": 3, "south": 51, "east": 4, "north": 52}),
    ({"bbox": shapely.geometry.box(3, 51, 4, 52)}, {"west": 3, "south": 51, "east": 4, "north": 52}),
    ({"bbox": {"west": 3, "south": 51, "east": 4, "north": 52}}, {"west": 3, "south": 51, "east": 4, "north": 52}),
    (
            {"bbox": {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}},
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
    ),

])
def test_filter_bbox_kwargs(con100: Connection, kwargs, expected):
    cube = con100.load_collection("S2").filter_bbox(**kwargs)
    node = _get_leaf_node(cube)
    assert node["process_id"] == "filter_bbox"
    assert node["arguments"]["extent"] == expected


def test_filter_bbox_parameter(con100: Connection):
    expected = {
        "process_id": "filter_bbox",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "extent": {"from_parameter": "my_bbox"},
        },
        "result": True,
    }
    bbox_param = Parameter(name="my_bbox", schema={"type": "object"})

    cube = con100.load_collection("S2").filter_bbox(bbox_param)
    assert _get_leaf_node(cube) == expected

    cube = con100.load_collection("S2").filter_bbox(bbox=bbox_param)
    assert _get_leaf_node(cube) == expected


def test_filter_bbox_parameter_invalid_schema(con100: Connection):
    expected = {
        "process_id": "filter_bbox",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "extent": {"from_parameter": "my_bbox"},
        },
        "result": True,
    }
    bbox_param = Parameter(name="my_bbox", schema={"type": "string"})

    with pytest.warns(
        UserWarning,
        match="Unexpected parameterized `extent` in `filter_bbox`: expected schema with type 'object' but got {'type': 'string'}.",
    ):
        cube = con100.load_collection("S2").filter_bbox(bbox_param)
    assert _get_leaf_node(cube) == expected

    with pytest.warns(
        UserWarning,
        match="Unexpected parameterized `extent` in `filter_bbox`: expected schema with type 'object' but got {'type': 'string'}.",
    ):
        cube = con100.load_collection("S2").filter_bbox(bbox=bbox_param)
    assert _get_leaf_node(cube) == expected


@pytest.mark.parametrize(["args", "expected"], [
    ((3, 4, 52, 51,), {"west": 3, "south": 51, "east": 4, "north": 52}),
    ((3, 4, 52, 51, 4326,), {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}),
    (([3, 51, 4, 52],), {"west": 3, "south": 51, "east": 4, "north": 52}),
    (((3, 51, 4, 52),), {"west": 3, "south": 51, "east": 4, "north": 52}),
    (({"west": 3, "south": 51, "east": 4, "north": 52},), {"west": 3, "south": 51, "east": 4, "north": 52}),
    (
            ({"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326},),
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
    ),
    ((shapely.geometry.box(3, 51, 4, 52),), {"west": 3, "south": 51, "east": 4, "north": 52}),

])
def test_filter_bbox_positional_args(con100: Connection, args, expected):
    cube = con100.load_collection("S2").filter_bbox(*args)
    node = _get_leaf_node(cube)
    assert node["process_id"] == "filter_bbox"
    assert node["arguments"]["extent"] == expected


def test_filter_bbox_legacy_positional_args(con100: Connection):
    with pytest.warns(UserWarning, match="Deprecated argument order"):
        cube = con100.load_collection("S2").filter_bbox(3, 4, 52, 51)
    node = _get_leaf_node(cube)
    assert node["process_id"] == "filter_bbox"
    assert node["arguments"]["extent"] == {"west": 3, "south": 51, "east": 4, "north": 52}


@pytest.mark.parametrize(["args", "kwargs", "expected"], [
    ((3, 4, 52, 51,), {"crs": 4326}, {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}),
    (([3, 51, 4, 52],), {"crs": 4326}, {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}),
    (((3, 51, 4, 52),), {"crs": 4326}, {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}),
    (
            ({"west": 3, "south": 51, "east": 4, "north": 52},),
            {"crs": 4326},
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
    ),
    (
            (shapely.geometry.box(3, 51, 4, 52),),
            {"crs": 4326},
            {"west": 3, "south": 51, "east": 4, "north": 52, "crs": 4326}
    ),
])
def test_filter_bbox_args_and_kwargs(con100: Connection, args, kwargs, expected):
    cube = con100.load_collection("S2").filter_bbox(*args, **kwargs)
    node = _get_leaf_node(cube)
    assert node["process_id"] == "filter_bbox"
    assert node["arguments"]["extent"] == expected


@pytest.mark.parametrize(["args", "kwargs", "expected"], [
    ((3, 4, 52, 51,), {"west": 2}, "Don't mix positional arguments with keyword arguments"),
    (([3, 51, 4, 52],), {"west": 2}, "Don't mix positional arguments with keyword arguments"),
    ((), {"west": 2, "bbox": [3, 51, 4, 52]}, "Don't mix `bbox` with `west`/`south`/`east`/`north` keyword arguments"),
])
def test_filter_bbox_args_and_kwargs_conflict(con100: Connection, args, kwargs, expected):
    with pytest.raises(ValueError, match=expected):
        con100.load_collection("S2").filter_bbox(*args, **kwargs)


def test_filter_spatial(con100: Connection, recwarn):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.filter_spatial(geometries=polygon)
    assert sorted(masked.flat_graph().keys()) == ["filterspatial1", "loadcollection1"]
    assert masked.flat_graph()["filterspatial1"] == {
        "process_id": "filter_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
            }
        },
        "result": True
    }


def test_aggregate_spatial_basic(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean")
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatial1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatial1"] == {
        "process_id": "aggregate_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
            },
            "reducer": {"process_graph": {
                "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
            }}
        },
        "result": True
    }


@pytest.mark.parametrize(["polygon", "expected_geometries"], [
    (
            shapely.geometry.box(0, 0, 1, 1),
            {"type": "Polygon", "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),)},
    ),
    (
            {"type": "Polygon", "coordinates": (((1, 0), (1, 1), (0, 1), (0, 0), (1, 0)),)},
            {"type": "Polygon", "coordinates": (((1, 0), (1, 1), (0, 1), (0, 0), (1, 0)),)},
    ),
    (
            shapely.geometry.MultiPolygon([shapely.geometry.box(0, 0, 1, 1)]),
            {"type": "MultiPolygon", "coordinates": [(((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),)]},
    ),
    (
            shapely.geometry.GeometryCollection([shapely.geometry.box(0, 0, 1, 1)]),
            {"type": "GeometryCollection", "geometries": [
                {"type": "Polygon", "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),)}
            ]},
    ),
    (
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature", "properties": {},
                        "geometry": {"type": "Polygon", "coordinates": (((1, 0), (1, 1), (0, 1), (0, 0), (1, 0)),)},
                    },

                ]
            },
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature", "properties": {},
                        "geometry": {"type": "Polygon", "coordinates": (((1, 0), (1, 1), (0, 1), (0, 0), (1, 0)),)},
                    },

                ]
            },
    ),
])
def test_aggregate_spatial_types(con100: Connection, polygon, expected_geometries):
    img = con100.load_collection("S2")
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean")
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatial1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatial1"] == {
        "process_id": "aggregate_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": expected_geometries,
            "reducer": {"process_graph": {
                "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
            }}
        },
        "result": True
    }


@pytest.mark.parametrize("crs", _get_normalizable_crs_inputs())
def test_aggregate_spatial_with_crs(con100: Connection, recwarn, crs: str):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean", crs=crs)
    warnings = [str(w.message) for w in recwarn]
    assert f"Geometry with non-Lon-Lat CRS {crs!r} is only supported by specific back-ends." in warnings
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatial1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatial1"] == {
        "process_id": "aggregate_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                "crs": {"properties": {"name": "EPSG:32631"}, "type": "name"},
            },
            "reducer": {"process_graph": {
                "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
            }}
        },
        "result": True
    }


@pytest.mark.skipif(
    pyproj.__version__ < ComparableVersion("3.3.0"),
    reason="PROJJSON format support requires pyproj 3.3.0 or higher",
)
@pytest.mark.parametrize("crs", [PROJJSON_FOR_EPSG23631, json.dumps(PROJJSON_FOR_EPSG23631)])
def test_aggregate_spatial_with_crs_as_projjson(con100: Connection, recwarn, crs):
    """Separate test coverage for PROJJSON, so we can skip it for Python versions below 3.8"""
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean", crs=crs)
    warnings = [str(w.message) for w in recwarn]
    assert f"Geometry with non-Lon-Lat CRS {crs!r} is only supported by specific back-ends." in warnings
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatial1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatial1"] == {
        "process_id": "aggregate_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                "crs": {"properties": {"name": "EPSG:32631"}, "type": "name"},
            },
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
        "result": True,
    }


@pytest.mark.parametrize(
    "crs",
    [
        "does-not-exist-crs",
        "EEPSG:32165",  # Simulate a user typo
        -1,  # negative value can not be valid EPSG code
        "-1",  # string representing negative value can not be valid EPSG code
        1.0,  # floating point: type is not supported
        {1: 1},  # type is not supported
        [1],  # type is not supported
    ],
)
def test_aggregate_spatial_with_invalid_crs(con100: Connection, recwarn, crs: str):
    """Test that it refuses invalid input for the CRS: incorrect types and negative integers,
    i.e. things that can not be a CRS at all, soo it is not just a CRS proj does not know about.
    """
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    with pytest.raises((ValueError, TypeError)):
        img.aggregate_spatial(geometries=polygon, reducer="mean", crs=crs)


def test_aggregate_spatial_target_dimension(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean", target_dimension="agg")
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatial1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatial1"] == {
        "process_id": "aggregate_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
            },
            "reducer": {"process_graph": {
                "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
            }},
            "target_dimension": "agg",
        },
        "result": True
    }


def test_aggregate_spatial_context(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.aggregate_spatial(geometries=polygon, reducer="mean", context={"foo": "bar"})
    assert masked.flat_graph()["aggregatespatial1"]["arguments"] == {
        "data": {"from_node": "loadcollection1"},
        "geometries": {
            "type": "Polygon",
            "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
        },
        "reducer": {"process_graph": {
            "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
        }},
        "context": {"foo": "bar"},
    }


@pytest.mark.parametrize("get_geometries", [
    lambda c: PGNode("load_vector", url="https://geo.test/features.json"),
    lambda c: openeo.processes.process("load_vector", url="https://geo.test/features.json"),
    lambda c: c.datacube_from_process("load_vector", url="https://geo.test/features.json"),
])
def test_aggregate_spatial_geometry_from_node(con100: Connection, get_geometries):
    cube = con100.load_collection("S2")
    geometries = get_geometries(con100)
    result = cube.aggregate_spatial(geometries=geometries, reducer="mean")
    assert result.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "loadvector1": {
            "process_id": "load_vector",
            "arguments": {"url": "https://geo.test/features.json"},
        },
        "aggregatespatial1": {
            "process_id": "aggregate_spatial",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "geometries": {"from_node": "loadvector1"},
                "reducer": {"process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }},
            },
            "result": True,
        },
    }


def test_aggregate_spatial_window(con100: Connection):
    img = con100.load_collection("S2")
    size = [5, 3]
    masked = img.aggregate_spatial_window(size=size, reducer="mean")
    assert sorted(masked.flat_graph().keys()) == ["aggregatespatialwindow1", "loadcollection1"]
    assert masked.flat_graph()["aggregatespatialwindow1"] == {
        "process_id": "aggregate_spatial_window",
        "arguments": {
            "align": "upper-left",
            "boundary": "pad",
            "data": {"from_node": "loadcollection1"},
            "size": [5, 3],
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
            "size": [5, 3],
            "context": None,
        },
        "result": True,
    }
    with pytest.raises(ValueError, match="Provided size not supported. Please provide a list of 2 integer values."):
        img.aggregate_spatial_window(size=[1], reducer="mean")
    with pytest.raises(ValueError, match="Provided size not supported. Please provide a list of 2 integer values."):
        img.aggregate_spatial_window(size=[1, 2, 3], reducer="mean")


def test_aggregate_temporal(con100: Connection):
    cube = con100.load_collection("S2")
    cube = cube.aggregate_temporal(
        intervals=[["2015-01-01", "2016-01-01"], ["2016-01-01", "2017-01-01"]],
        reducer=lambda d: d.median(),
        context={"bla": "bla"},
    )

    assert cube.flat_graph()["aggregatetemporal1"] == {
        "process_id": "aggregate_temporal",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "intervals": [["2015-01-01", "2016-01-01"], ["2016-01-01", "2017-01-01"]],
            "reducer": {"process_graph": {"median1": {
                "arguments": {"data": {"from_parameter": "data"}},
                "process_id": "median",
                "result": True,
            }}},
            "context": {"bla": "bla"},
        },
        "result": True
    }


def test_aggregate_temporal_period(con100: Connection):
    cube = con100.load_collection("S2")
    cube = cube.aggregate_temporal_period(period="dekad", reducer=lambda d: d.median(), context={"bla": "bla"})

    assert cube.flat_graph()["aggregatetemporalperiod1"] == {
        "process_id": "aggregate_temporal_period",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "period": "dekad",
            "reducer": {"process_graph": {"median1": {
                "arguments": {"data": {"from_parameter": "data"}},
                "process_id": "median",
                "result": True,
            }}},
            "context": {"bla": "bla"},
        },
        "result": True
    }


def test_mask_polygon_basic(con100: Connection):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.mask_polygon(mask=polygon)
    assert sorted(masked.flat_graph().keys()) == ["loadcollection1", "maskpolygon1"]
    assert masked.flat_graph()["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
            }
        },
        "result": True
    }


@pytest.mark.parametrize(["polygon", "expected_mask"], basic_geometry_types)
def test_mask_polygon_types(con100: Connection, polygon, expected_mask):
    cube = con100.load_collection("S2")
    masked = cube.mask_polygon(mask=polygon)
    assert get_download_graph(masked, drop_save_result=True, drop_load_collection=True) == {
        "maskpolygon1": {
            "process_id": "mask_polygon",
            "arguments": {"data": {"from_node": "loadcollection1"}, "mask": expected_mask},
        }
    }


@pytest.mark.parametrize("crs", _get_normalizable_crs_inputs())
def test_mask_polygon_with_crs(con100: Connection, recwarn, crs: str):
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.mask_polygon(mask=polygon, srs=crs)
    warnings = [str(w.message) for w in recwarn]
    assert f"Geometry with non-Lon-Lat CRS {crs!r} is only supported by specific back-ends." in warnings
    assert sorted(masked.flat_graph().keys()) == ["loadcollection1", "maskpolygon1"]
    assert masked.flat_graph()["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                # All listed test inputs for crs should be converted to "EPSG:32631"
                "crs": {"type": "name", "properties": {"name": "EPSG:32631"}},
            },
        },
        "result": True,
    }


@pytest.mark.skipif(
    pyproj.__version__ < ComparableVersion("3.3.0"),
    reason="PROJJSON format support requires pyproj 3.3.0 or higher",
)
@pytest.mark.parametrize("crs", [PROJJSON_FOR_EPSG23631, json.dumps(PROJJSON_FOR_EPSG23631)])
def test_mask_polygon_with_crs_as_projjson(con100: Connection, recwarn, crs):
    """Separate test coverage for PROJJSON, so we can skip it for Python versions below 3.8"""
    img = con100.load_collection("S2")
    polygon = shapely.geometry.box(0, 0, 1, 1)
    masked = img.mask_polygon(mask=polygon, srs=crs)
    warnings = [str(w.message) for w in recwarn]
    assert f"Geometry with non-Lon-Lat CRS {crs!r} is only supported by specific back-ends." in warnings
    assert sorted(masked.flat_graph().keys()) == ["loadcollection1", "maskpolygon1"]
    assert masked.flat_graph()["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {
                "type": "Polygon",
                "coordinates": (((1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0)),),
                # All listed test inputs for crs should be converted to "EPSG:32631"
                "crs": {"type": "name", "properties": {"name": "EPSG:32631"}},
            },
        },
        "result": True,
    }


def test_mask_polygon_parameter(con100: Connection):
    img = con100.load_collection("S2")
    polygon = Parameter(name="shape", schema="object")
    masked = img.mask_polygon(mask=polygon)
    assert sorted(masked.flat_graph().keys()) == ["loadcollection1", "maskpolygon1"]
    assert masked.flat_graph()["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_parameter": "shape"},
        },
        "result": True,
    }


def test_mask_polygon_path(con100: Connection):
    img = con100.load_collection("S2")
    masked = img.mask_polygon(mask="path/to/polygon.json")
    assert sorted(masked.flat_graph().keys()) == ["loadcollection1", "maskpolygon1", "readvector1"]
    assert masked.flat_graph()["maskpolygon1"] == {
        "process_id": "mask_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "readvector1"},
        },
        "result": True,
    }
    assert masked.flat_graph()["readvector1"] == {
        "process_id": "read_vector",
        "arguments": {"filename": "path/to/polygon.json"},
    }


@pytest.mark.parametrize("get_geometries", [
    lambda c: PGNode("load_vector", url="https://geo.test/features.json"),
    lambda c: openeo.processes.process("load_vector", url="https://geo.test/features.json"),
    lambda c: c.datacube_from_process("load_vector", url="https://geo.test/features.json"),
])
def test_mask_polygon_from_node(con100: Connection, get_geometries):
    cube = con100.load_collection("S2")
    geometries = get_geometries(con100)
    result = cube.mask_polygon(mask=geometries)
    assert result.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "loadvector1": {
            "process_id": "load_vector",
            "arguments": {"url": "https://geo.test/features.json"},
        },
        "maskpolygon1": {
            "process_id": "mask_polygon",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "mask": {"from_node": "loadvector1"},
            },
            "result": True,
        },
    }


def test_mask_raster(con100: Connection):
    img = con100.load_collection("S2")
    mask = con100.load_collection("MASK")
    masked = img.mask(mask=mask, replacement=102)
    assert masked.flat_graph()["mask1"] == {
        "process_id": "mask",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "mask": {"from_node": "loadcollection2"},
            "replacement": 102,
        },
        "result": True,
    }


def test_merge_cubes(con100: Connection):
    a = con100.load_collection("S2")
    b = con100.load_collection("MASK")
    c = a.merge(b)
    assert c.flat_graph()["mergecubes1"] == {
        "process_id": "merge_cubes",
        "arguments": {
            "cube1": {"from_node": "loadcollection1"},
            "cube2": {"from_node": "loadcollection2"},
        },
        "result": True,
    }


def test_merge_cubes_context(con100: Connection):
    a = con100.load_collection("S2")
    b = con100.load_collection("MASK")
    c = a.merge(b, context={"foo": 867})
    assert c.flat_graph()["mergecubes1"] == {
        "process_id": "merge_cubes",
        "arguments": {
            "cube1": {"from_node": "loadcollection1"},
            "cube2": {"from_node": "loadcollection2"},
            "context": {"foo": 867},
        },
        "result": True,
    }


def test_merge_cubes_issue107(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/107"""
    s2 = con100.load_collection("S2")
    a = s2.filter_bands(["B02"])
    b = s2.filter_bands(["B04"])
    c = a.merge_cubes(b)

    flat = c.flat_graph()
    # There should be only one `load_collection` node (but two `filter_band` ones)
    assert collections.Counter(n["process_id"] for n in flat.values()) == {
        "load_collection": 1,
        "filter_bands": 2,
        "merge_cubes": 1,
    }


def test_merge_cubes_no_resolver(con100, test_data):
    s2 = con100.load_collection("S2")
    mask = con100.load_collection("MASK")
    merged = s2.merge_cubes(mask)
    assert s2.metadata.band_names == ["B02", "B03", "B04", "B08"]
    assert mask.metadata.band_names == ["CLOUDS", "WATER"]
    assert merged.metadata.band_names == ["B02", "B03", "B04", "B08", "CLOUDS", "WATER"]
    assert merged.flat_graph() == test_data.load_json("1.0.0/merge_cubes_no_resolver.json")


def test_merge_cubes_max_resolver(con100, test_data):
    s2 = con100.load_collection("S2")
    mask = con100.load_collection("MASK")
    merged = s2.merge_cubes(mask, overlap_resolver="max")
    assert s2.metadata.band_names == ["B02", "B03", "B04", "B08"]
    assert mask.metadata.band_names == ["CLOUDS", "WATER"]
    assert merged.metadata.band_names == ["B02", "B03", "B04", "B08", "CLOUDS", "WATER"]
    assert merged.flat_graph() == test_data.load_json("1.0.0/merge_cubes_max.json")


@pytest.mark.parametrize("overlap_resolver", [None, "max"])
def test_merge_cubes_band_merging_disjunct(con100, requests_mock, overlap_resolver):
    setup_collection_metadata(requests_mock=requests_mock, cid="S3", bands=["B2", "B3"])
    setup_collection_metadata(requests_mock=requests_mock, cid="S4", bands=["C4", "C6"])

    s3 = con100.load_collection("S3")
    s4 = con100.load_collection("S4")
    s3_m_s4 = s3.merge_cubes(s4, overlap_resolver=overlap_resolver)
    s4_m_s3 = s4.merge_cubes(s3, overlap_resolver=overlap_resolver)
    assert s3.metadata.band_names == ["B2", "B3"]
    assert s4.metadata.band_names == ["C4", "C6"]
    assert s3_m_s4.metadata.band_names == ["B2", "B3", "C4", "C6"]
    assert s4_m_s3.metadata.band_names == ["C4", "C6", "B2", "B3"]

    s3_f = s3.filter_bands(["B2"])
    s4_f = s4.filter_bands(["C6", "C4"])
    s3_f_m_s4_f = s3_f.merge_cubes(s4_f, overlap_resolver=overlap_resolver)
    s4_f_m_s3_f = s4_f.merge_cubes(s3_f, overlap_resolver=overlap_resolver)
    assert s3_f.metadata.band_names == ["B2"]
    assert s4_f.metadata.band_names == ["C6", "C4"]
    assert s3_f_m_s4_f.metadata.band_names == ["B2", "C6", "C4"]
    assert s4_f_m_s3_f.metadata.band_names == ["C6", "C4", "B2"]


@pytest.mark.parametrize("overlap_resolver", [None, "max"])
def test_merge_cubes_band_merging_with_overlap(con100, requests_mock, overlap_resolver):
    # Overlapping bands without overlap resolver will give an error in the backend
    setup_collection_metadata(requests_mock=requests_mock, cid="S3", bands=["B2", "B3", "B5", "B8"])
    setup_collection_metadata(requests_mock=requests_mock, cid="S4", bands=["B4", "B5", "B6"])

    s3 = con100.load_collection("S3")
    s4 = con100.load_collection("S4")
    s3_m_s4 = s3.merge_cubes(s4, overlap_resolver=overlap_resolver)
    s4_m_s3 = s4.merge_cubes(s3, overlap_resolver=overlap_resolver)
    assert s3.metadata.band_names == ["B2", "B3", "B5", "B8"]
    assert s4.metadata.band_names == ["B4", "B5", "B6"]
    assert s3_m_s4.metadata.band_names == ["B2", "B3", "B5", "B8", "B4", "B6"]
    assert s4_m_s3.metadata.band_names == ["B4", "B5", "B6", "B2", "B3", "B8"]

    s3_f = s3.filter_bands(["B5", "B8"])
    s4_f = s4.filter_bands(["B6", "B5"])
    s3_f_m_s4_f = s3_f.merge_cubes(s4_f, overlap_resolver=overlap_resolver)
    s4_f_m_s3_f = s4_f.merge_cubes(s3_f, overlap_resolver=overlap_resolver)
    assert s3_f.metadata.band_names == ["B5", "B8"]
    assert s4_f.metadata.band_names == ["B6", "B5"]
    assert s3_f_m_s4_f.metadata.band_names == ["B5", "B8", "B6"]
    assert s4_f_m_s3_f.metadata.band_names == ["B6", "B5", "B8"]


def test_resample_cube_spatial(con100: Connection):
    data = con100.load_collection("S2")
    target = con100.load_collection("MASK")
    cube = data.resample_cube_spatial(target, method="spline")
    assert _get_leaf_node(cube) == {
        "process_id": "resample_cube_spatial",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "target": {"from_node": "loadcollection2"},
            "method": "spline",
        },
        "result": True,
    }


def test_resample_cube_temporal(con100: Connection):
    data = con100.load_collection("S2")
    target = con100.load_collection("MASK")
    cube = data.resample_cube_temporal(target)
    assert _get_leaf_node(cube) == {
        "process_id": "resample_cube_temporal",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "target": {"from_node": "loadcollection2"},
        },
        "result": True,
    }

    cube = data.resample_cube_temporal(target, dimension="t", valid_within=30)
    assert _get_leaf_node(cube) == {
        "process_id": "resample_cube_temporal",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "target": {"from_node": "loadcollection2"},
            "dimension": "t",
            "valid_within": 30,
        },
        "result": True,
    }


def test_ndvi_simple(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi()
    assert sorted(ndvi.flat_graph().keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.flat_graph()["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}},
        "result": True,
    }
    assert not ndvi.metadata.has_band_dimension()


def test_ndvi_args(con100: Connection):
    ndvi = con100.load_collection("S2").ndvi(nir="nirr", red="rred", target_band="ndvii")
    assert sorted(ndvi.flat_graph().keys()) == ["loadcollection1", "ndvi1"]
    assert ndvi.flat_graph()["ndvi1"] == {
        "process_id": "ndvi",
        "arguments": {"data": {"from_node": "loadcollection1"}, "nir": "nirr", "red": "rred", "target_band": "ndvii"},
        "result": True,
    }
    assert ndvi.metadata.has_band_dimension()
    assert ndvi.metadata.band_dimension.band_names == ["B02", "B03", "B04", "B08", "ndvii"]


def test_rename_dimension(s2cube):
    cube = s2cube.rename_dimension(source="bands", target="ThisIsNotTheBandsDimension")
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "renamedimension1": {
            "process_id": "rename_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "source": "bands",
                "target": "ThisIsNotTheBandsDimension",
            },
            "result": True,
        },
    }


def test_rename_dimension_invalid_dimension_with_metadata(s2cube):
    with pytest.raises(ValueError, match="Invalid dimension 'applepie'."):
        _ = s2cube.rename_dimension(source="applepie", target="icecream")


def test_rename_dimension_invalid_dimension_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.rename_dimension(source="applepie", target="icecream")
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "renamedimension1": {
            "process_id": "rename_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "source": "applepie",
                "target": "icecream",
            },
            "result": True,
        },
    }


def test_add_dimension(con100):
    s2 = con100.load_collection("S2")
    x = s2.add_dimension(name="james_band", label="alpha")
    assert x.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "adddimension1": {
            "process_id": "add_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "name": "james_band",
                "label": "alpha",
            },
            "result": True,
        },
    }


def test_drop_dimension(con100):
    s2 = con100.load_collection("S2")
    x = s2.drop_dimension(name="bands")
    assert x.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "dropdimension1": {
            "process_id": "drop_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "name": "bands",
            },
            "result": True,
        },
    }


def test_reduce_dimension(con100):
    s2 = con100.load_collection("S2")
    x = s2.reduce_dimension(dimension="bands", reducer="mean")
    assert x.flat_graph() == {
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
    assert x.flat_graph() == {
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
        assert cube.flat_graph()["reducedimension1"] == {
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


def test_reduce_dimension_context(con100):
    s2 = con100.load_collection("S2")
    x = s2.reduce_dimension(dimension="bands", reducer="mean", context=123)
    assert x.flat_graph() == {
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
                }},
                "context": 123,
            },
            'result': True
        }}


def test_reduce_dimension_invalid_dimension_with_metadata(s2cube):
    with pytest.raises(ValueError, match="ola"):
        s2cube.reduce_dimension(dimension="olapola", reducer="mean")


def test_reduce_dimension_invalid_dimension_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.reduce_dimension(dimension="olapola", reducer="mean")
    assert get_download_graph(cube)["reducedimension1"] == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "olapola",
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
    }
    cube = cube.reduce_dimension(dimension="jamanee", reducer="max")
    assert get_download_graph(cube)["reducedimension2"] == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "reducedimension1"},
            "dimension": "jamanee",
            "reducer": {
                "process_graph": {
                    "max1": {"process_id": "max", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
    }


def test_reduce_bands(s2cube):
    cube = s2cube.reduce_bands(reducer="mean")
    assert get_download_graph(cube)["reducedimension1"] == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "bands",
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
    }


def test_reduce_bands_no_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.reduce_bands(reducer="mean")
    assert get_download_graph(cube)["reducedimension1"] == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "bands",
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
    }


def test_reduce_bands_udf(con100):
    s2 = con100.load_collection("S2")
    x = s2.reduce_bands(reducer=openeo.UDF("def apply(x):\n    return x"))
    assert x.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "reducer": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "udf": "def apply(x):\n    return x",
                                "runtime": "Python",
                            },
                            "result": True,
                        }
                    }
                },
            },
            "result": True,
        },
    }


def test_reduce_temporal(s2cube):
    cube = s2cube.reduce_temporal(reducer="mean")
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "mean1": {
                            "process_id": "mean",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
            "result": True,
        },
    }


def test_reduce_spatial(s2cube):
    cube = s2cube.reduce_spatial(reducer="mean")
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducespatial1": {
            "process_id": "reduce_spatial",
            "arguments": {
                "context": None,
                "data": {"from_node": "loadcollection1"},
                "reducer": {
                    "process_graph": {
                        "mean1": {
                            "process_id": "mean",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
            "result": True,
        },
    }


def test_reduce_temporal_udf(con100):
    s2 = con100.load_collection("S2")
    x = s2.reduce_temporal(reducer=openeo.UDF("def apply(x):\n    return x"))

    assert x.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "udf": "def apply(x):\n    return x",
                                "runtime": "Python",
                            },
                            "result": True,
                        }
                    }
                },
            },
            'result': True
        }}


def test_reduce_temporal_without_metadata(s2cube_without_metadata):
    cube = s2cube_without_metadata.reduce_temporal(reducer="mean")
    assert get_download_graph(cube)["reducedimension1"] == {
        "process_id": "reduce_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "dimension": "t",
            "reducer": {
                "process_graph": {
                    "mean1": {"process_id": "mean", "arguments": {"data": {"from_parameter": "data"}}, "result": True}
                }
            },
        },
    }


def test_chunk_polygon_basic(con100: Connection):
    cube = con100.load_collection("S2")
    polygon: shapely.geometry.Polygon = shapely.geometry.box(0, 0, 1, 1)
    process = lambda data: data.run_udf(udf="myfancycode", runtime="Python")
    with pytest.warns(UserDeprecationWarning, match="Use `apply_polygon`"):
        result = cube.chunk_polygon(chunks=polygon, process=process)
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "chunkpolygon1": {
            "process_id": "chunk_polygon",
            "arguments": {
                "chunks": {
                    "type": "Polygon",
                    "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]],
                },
                "data": {"from_node": "loadcollection1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
            },
        }
    }


@pytest.mark.parametrize(["polygon", "expected_chunks"], basic_geometry_types)
def test_chunk_polygon_types(con100: Connection, polygon, expected_chunks):
    cube = con100.load_collection("S2")
    process = UDF(code="myfancycode", runtime="Python")
    with pytest.warns(UserDeprecationWarning, match="Use `apply_polygon`"):
        result = cube.chunk_polygon(chunks=polygon, process=process)
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "chunkpolygon1": {
            "process_id": "chunk_polygon",
            "arguments": {
                "chunks": expected_chunks,
                "data": {"from_node": "loadcollection1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
            },
        }
    }


def test_chunk_polygon_parameter(con100: Connection):
    cube = con100.load_collection("S2")
    polygon = Parameter(name="shape", schema="object")
    process = lambda data: data.run_udf(udf="myfancycode", runtime="Python")
    with pytest.warns(UserDeprecationWarning, match="Use `apply_polygon`"):
        result = cube.chunk_polygon(chunks=polygon, process=process)
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "chunkpolygon1": {
            "process_id": "chunk_polygon",
            "arguments": {
                "chunks": {"from_parameter": "shape"},
                "data": {"from_node": "loadcollection1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
            },
        }
    }


def test_chunk_polygon_path(con100: Connection):
    cube = con100.load_collection("S2")
    process = lambda data: data.run_udf(udf="myfancycode", runtime="Python")
    with pytest.warns(UserDeprecationWarning, match="Use `apply_polygon`"):
        result = cube.chunk_polygon(chunks="path/to/polygon.json", process=process)
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "readvector1": {"process_id": "read_vector", "arguments": {"filename": "path/to/polygon.json"}},
        "chunkpolygon1": {
            "process_id": "chunk_polygon",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "chunks": {"from_node": "readvector1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
            },
        },
    }


def test_chunk_polygon_context(con100: Connection):
    cube = con100.load_collection("S2")
    polygon = shapely.geometry.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])
    process = lambda data: data.run_udf(udf="myfancycode", runtime="Python")
    with pytest.warns(UserDeprecationWarning, match="Use `apply_polygon`"):
        result = cube.chunk_polygon(chunks=polygon, process=process, context={"foo": 4})
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "chunkpolygon1": {
            "process_id": "chunk_polygon",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "chunks": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 1], [0, 0]]]},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
                "context": {"foo": 4},
            },
        }
    }


def test_apply_polygon_basic_legacy(con100: Connection):
    cube = con100.load_collection("S2")
    geometries = shapely.geometry.box(0, 0, 1, 1)
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(polygons=geometries, process=process)
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            # TODO #592: For now, stick to legacy "polygons" argument in this case.
            #       But eventually: change argument name to "geometries"
            #       when https://github.com/Open-EO/openeo-python-driver/commit/15b72a77 propagated to all backends
            "polygons": {
                "type": "Polygon",
                "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]],
            },
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
        },
    }


def test_apply_polygon_basic(con100: Connection):
    cube = con100.load_collection("S2")
    geometries = shapely.geometry.box(0, 0, 1, 1)
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(geometries=geometries, process=process)
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]],
            },
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
        },
    }


def test_apply_polygon_basic_positional(con100: Connection):
    """DataCube.apply_polygon() with positional arguments."""
    cube = con100.load_collection("S2")
    geometries = shapely.geometry.box(0, 0, 1, 1)
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(geometries, process)
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "geometries": {
                "type": "Polygon",
                "coordinates": [[[1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0], [1.0, 0.0]]],
            },
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
        },
    }


@pytest.mark.parametrize(
    ["geometries_argument", "geometries_parameter"],
    [
        ("polygons", "polygons"),  # TODO #592: *parameter* "polygons" for now, eventually change to "geometries"
        ("geometries", "geometries"),
    ],
)
@pytest.mark.parametrize(["geometries", "expected_polygons"], basic_geometry_types)
def test_apply_polygon_types(
    con100: Connection, geometries, expected_polygons, geometries_argument, geometries_parameter
):
    if isinstance(geometries, shapely.geometry.GeometryCollection):
        pytest.skip("apply_polygon does not support GeometryCollection")
    cube = con100.load_collection("S2")
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(**{geometries_argument: geometries}, process=process)
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            geometries_parameter: expected_polygons,
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
        },
    }


@pytest.mark.parametrize(
    ["geometries_argument", "geometries_parameter"],
    [
        ("polygons", "polygons"),  # TODO #592: *parameter* "polygons" for now, eventually change to "geometries"
        ("geometries", "geometries"),
    ],
)
def test_apply_polygon_parameter(con100: Connection, geometries_argument, geometries_parameter):
    cube = con100.load_collection("S2")
    geometries = Parameter(name="shapes", schema="object")
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(**{geometries_argument: geometries}, process=process)
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            geometries_parameter: {"from_parameter": "shapes"},
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
        },
    }


@pytest.mark.parametrize(
    ["geometries_argument", "geometries_parameter"],
    [
        ("polygons", "polygons"),  # TODO #592: *parameter* "polygons" for now, eventually change to "geometries"
        ("geometries", "geometries"),
    ],
)
def test_apply_polygon_path(con100: Connection, geometries_argument, geometries_parameter):
    cube = con100.load_collection("S2")
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(**{geometries_argument: "path/to/polygon.json"}, process=process)
    assert get_download_graph(result, drop_save_result=True, drop_load_collection=True) == {
        "readvector1": {
            # TODO #104 #457 get rid of non-standard read_vector
            "process_id": "read_vector",
            "arguments": {"filename": "path/to/polygon.json"},
        },
        "applypolygon1": {
            "process_id": "apply_polygon",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                geometries_parameter: {"from_node": "readvector1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": "myfancycode",
                            },
                            "result": True,
                        }
                    }
                },
            },
        },
    }


@pytest.mark.parametrize(
    ["geometries_argument", "geometries_parameter"],
    [
        ("polygons", "polygons"),  # TODO #592: *parameter* "polygons" for now, eventually change to "geometries"
        ("geometries", "geometries"),
    ],
)
def test_apply_polygon_context(con100: Connection, geometries_argument, geometries_parameter):
    cube = con100.load_collection("S2")
    geometries = shapely.geometry.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])
    process = UDF(code="myfancycode", runtime="Python")
    result = cube.apply_polygon(**{geometries_argument: geometries}, process=process, context={"foo": 4})
    assert get_download_graph(result)["applypolygon1"] == {
        "process_id": "apply_polygon",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            geometries_parameter: {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 1], [0, 0]]]},
            "process": {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": {"from_parameter": "data"}, "runtime": "Python", "udf": "myfancycode"},
                        "result": True,
                    }
                }
            },
            "context": {"foo": 4},
        },
    }


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


def test_apply_absolute_pgnode(con100, test_data):
    im = con100.load_collection("S2")
    result = im.apply(PGNode(process_id="absolute", arguments={"x": {"from_parameter": "x"}}))
    expected_graph = test_data.load_json("1.0.0/apply_absolute.json")
    assert result.flat_graph() == expected_graph


def test_apply_absolute(con100):
    cube = con100.load_collection("S2")
    result = cube.apply("absolute")
    assert result.flat_graph()["apply1"] == {
        "process_id": "apply",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": {
                "absolute1": {
                    "process_id": "absolute",
                    "arguments": {"x": {"from_parameter": "x"}},
                    "result": True
                },
            }},
        },
        "result": True,
    }


def test_apply_absolute_context(con100):
    cube = con100.load_collection("S2")
    result = cube.apply("absolute", context={"foo": 867})
    assert result.flat_graph()["apply1"] == {
        "process_id": "apply",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": {
                "absolute1": {
                    "process_id": "absolute",
                    "arguments": {"x": {"from_parameter": "x"}},
                    "result": True
                },
            }},
            "context": {"foo": 867},
        },
        "result": True,
    }


def test_load_collection_properties_process_builder_function(con100, test_data):
    from openeo.processes import between, eq

    im = con100.load_collection(
        "S2",
        spatial_extent={"west": 16.1, "east": 16.6, "north": 48.6, "south": 47.2},
        temporal_extent=["2018-01-01", "2019-01-01"],
        properties={
            "eo:cloud_cover": lambda x: between(x=x, min=0, max=50),
            "platform": lambda x: eq(x=x, y="Sentinel-2B"),
        },
    )
    expected = test_data.load_json("1.0.0/load_collection_properties.json")
    assert im.flat_graph() == expected


def test_load_collection_properties_process_builder_method_and_math(con100, test_data):
    im = con100.load_collection(
        "S2",
        spatial_extent={"west": 16.1, "east": 16.6, "north": 48.6, "south": 47.2},
        temporal_extent=["2018-01-01", "2019-01-01"],
        properties={
            "eo:cloud_cover": lambda x: x.between(min=0, max=50),
            "platform": lambda x: x == "Sentinel-2B",
        },
    )
    expected = test_data.load_json("1.0.0/load_collection_properties.json")
    assert im.flat_graph() == expected


def test_load_collection_max_cloud_cover(con100):
    im = con100.load_collection(
        "S2",
        max_cloud_cover=75,
    )
    assert im.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        'eo:cloud_cover': {'process_graph': {
            'lte1': {
                'process_id': 'lte',
                'arguments': {'x': {'from_parameter': 'value'}, 'y': 75},
                'result': True,
            }
        }},
    }


def test_load_collection_max_cloud_cover_with_other_properties(con100):
    im = con100.load_collection(
        "S2",
        properties={
            "platform": lambda x: x == "Sentinel-2B",
        },
        max_cloud_cover=75,
    )
    assert im.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        'eo:cloud_cover': {'process_graph': {
            'lte1': {
                'process_id': 'lte',
                'arguments': {'x': {'from_parameter': 'value'}, 'y': 75},
                'result': True,
            }
        }},
        "platform": {"process_graph": {
            "eq1": {
                "process_id": "eq",
                "arguments": {"x": {"from_parameter": "value"}, "y": "Sentinel-2B"},
                "result": True
            }
        }}
    }


@pytest.mark.parametrize(["extra_summaries", "max_cloud_cover", "expect_warning"], [
    ({}, None, False),
    ({}, 75, True),
    ({"eo:cloud_cover": {"min": 0, "max": 100}}, None, False),
    ({"eo:cloud_cover": {"min": 0, "max": 100}}, 75, False),
])
def test_load_collection_max_cloud_cover_summaries_warning(
        con100, requests_mock, recwarn, extra_summaries, max_cloud_cover, expect_warning,
):
    s2_metadata = copy.deepcopy(DEFAULT_S2_METADATA)
    s2_metadata["summaries"].update(extra_summaries)
    requests_mock.get(API_URL + "/collections/S2", json=s2_metadata)

    _ = con100.load_collection("S2", max_cloud_cover=max_cloud_cover)

    if expect_warning:
        assert len(recwarn.list) == 1
        assert re.search(
            "property filtering.*undefined.*collection metadata.*eo:cloud_cover",
            str(recwarn.pop(UserWarning).message),
        )
    else:
        assert len(recwarn.list) == 0


def test_load_collection_with_collection_properties(con100):
    cube = con100.load_collection(
        "S2",
        properties=[
            collection_property("eo:cloud_cover") <= 75,
            collection_property("platform") == "Sentinel-2B",
        ],
    )
    assert cube.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        "eo:cloud_cover": {
            "process_graph": {
                "lte1": {
                    "process_id": "lte",
                    "arguments": {"x": {"from_parameter": "value"}, "y": 75},
                    "result": True,
                }
            }
        },
        "platform": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "Sentinel-2B"},
                    "result": True,
                }
            }
        },
    }


def test_load_collection_with_collection_properties_and_cloud_cover(con100):
    cube = con100.load_collection(
        "S2",
        properties=[
            collection_property("platform") == "Sentinel-2B",
        ],
        max_cloud_cover=66,
    )
    assert cube.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        "eo:cloud_cover": {
            "process_graph": {
                "lte1": {
                    "process_id": "lte",
                    "arguments": {"x": {"from_parameter": "value"}, "y": 66},
                    "result": True,
                }
            }
        },
        "platform": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "Sentinel-2B"},
                    "result": True,
                }
            }
        },
    }


def test_load_collection_with_single_collection_property(con100):
    cube = con100.load_collection(
        "S2",
        properties=collection_property("platform") == "Sentinel-2B",
    )
    assert cube.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        "platform": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "Sentinel-2B"},
                    "result": True,
                }
            }
        },
    }


def test_load_collection_with_single_collection_property_and_cloud_cover(con100):
    cube = con100.load_collection(
        "S2",
        properties=collection_property("platform") == "Sentinel-2B",
        max_cloud_cover=66,
    )
    assert cube.flat_graph()["loadcollection1"]["arguments"]["properties"] == {
        "eo:cloud_cover": {
            "process_graph": {
                "lte1": {
                    "process_id": "lte",
                    "arguments": {"x": {"from_parameter": "value"}, "y": 66},
                    "result": True,
                }
            }
        },
        "platform": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "Sentinel-2B"},
                    "result": True,
                }
            }
        },
    }


def test_load_collection_temporal_extent_process_builder_function(con100):
    from openeo.processes import date_shift

    expected = {
        "dateshift1": {
            "arguments": {
                "date": {"from_parameter": "start_date"},
                "unit": "days",
                "value": -2,
            },
            "process_id": "date_shift",
        },
        "loadcollection1": {
            "arguments": {
                "id": "S2",
                "spatial_extent": None,
                "temporal_extent": [{"from_node": "dateshift1"}, "2019-01-01"],
            },
            "process_id": "load_collection",
            "result": True,
        },
    }

    im = con100.load_collection(
        "S2",
        temporal_extent=[date_shift(Parameter("start_date"), -2, unit="days").pgnode, "2019-01-01"],
    )
    assert im.flat_graph() == expected

    im = con100.load_collection(
        "S2", temporal_extent=[date_shift(Parameter("start_date"), -2, unit="days"), "2019-01-01"],
    )
    assert im.flat_graph() == expected


def test_load_collection_parameterized_collection_id(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/471"""
    collection = Parameter(name="my_collection", schema={"type": "str"})
    cube = con100.load_collection(collection)
    assert get_download_graph(cube, drop_save_result=True) == {
        "loadcollection1": {
            "arguments": {
                "id": {"from_parameter": "my_collection"},
                "spatial_extent": None,
                "temporal_extent": None,
            },
            "process_id": "load_collection",
        },
    }


def test_load_collection_parameterized_bands(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/471"""
    bands = Parameter(name="my_bands", schema={"type": "array", "items": {"type": "string"}})
    cube = con100.load_collection("S2", bands=bands)
    assert get_download_graph(cube, drop_save_result=True) == {
        "loadcollection1": {
            "arguments": {
                "id": "S2",
                "spatial_extent": None,
                "temporal_extent": None,
                "bands": {"from_parameter": "my_bands"},
            },
            "process_id": "load_collection",
        },
    }


def test_apply_dimension_temporal_cumsum_with_target(con100, test_data):
    cumsum = con100.load_collection("S2").apply_dimension('cumsum', dimension="t", target_dimension="MyNewTime")
    actual_graph = cumsum.flat_graph()
    expected_graph = test_data.load_json("1.0.0/apply_dimension_temporal_cumsum.json")
    expected_graph["applydimension1"]["arguments"]["target_dimension"] = "MyNewTime"
    expected_graph["applydimension1"]["result"] = True
    del expected_graph["saveresult1"]
    assert actual_graph == expected_graph


def test_apply_dimension_temporal_cumsum_context(con100):
    cumsum = con100.load_collection("S2").apply_dimension('cumsum', dimension="t", context={"foo": 867})
    actual_graph = cumsum.flat_graph()
    assert actual_graph["applydimension1"]["arguments"] == {
        'data': {'from_node': 'loadcollection1'},
        'process': {'process_graph': {
            'cumsum1': {'process_id': 'cumsum', 'arguments': {'data': {'from_parameter': 'data'}}, 'result': True}
        }},
        'dimension': 't',
        'context': {'foo': 867},
    }


def test_apply_dimension_modify_bands(con100):
    def update_bands(x: ProcessBuilder):
        b01 = x.array_element(0)
        b02 = x.array_element(1)
        diff = b01 - b02
        return x.array_modify(values=diff, index=0)

    cumsum = con100.load_collection("S2").apply_dimension(process=update_bands, dimension="bands")
    actual_graph = cumsum.flat_graph()

    assert actual_graph == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None}
        },
        "applydimension1": {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "process": {"process_graph": {
                    "arrayelement1": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 0},
                    },
                    "arrayelement2": {
                        "process_id": "array_element",
                        "arguments": {"data": {"from_parameter": "data"}, "index": 1},
                    },
                    "subtract1": {
                        "process_id": "subtract",
                        "arguments": {"x": {"from_node": "arrayelement1"}, "y": {"from_node": "arrayelement2"}},
                    },
                    "arraymodify1": {
                        "process_id": "array_modify",
                        "arguments": {
                            "data": {"from_parameter": "data"}, "index": 0,
                            "values": {"from_node": "subtract1"}
                        },
                        "result": True
                    },
                }}
            },
            "result": True
        },
    }



def test_datacube_from_process_apply_dimension(con100, caplog, recwarn):
    """https://github.com/Open-EO/openeo-python-client/issues/442"""
    cube = con100.datacube_from_process("wibble")
    cube = cube.apply_dimension(dimension="t", process="cumsum")
    assert get_download_graph(cube)["applydimension1"] == {
        "process_id": "apply_dimension",
        "arguments": {
            "data": {"from_node": "wibble1"},
            "dimension": "t",
            "process": {
                "process_graph": {
                    "cumsum1": {
                        "arguments": {"data": {"from_parameter": "data"}},
                        "process_id": "cumsum",
                        "result": True,
                    }
                }
            },
        },
    }
    assert caplog.messages == []
    assert recwarn.list == []


def test_apply_neighborhood_context(con100):
    collection = con100.load_collection("S2")
    neighbors = collection.apply_neighborhood(
        process="mean",
        size=[{"dimension": "x", "value": 128, "unit": "px"}, {"dimension": "y", "value": 128, "unit": "px"}],
        overlap=[{"dimension": "t", "value": "P10d"}],
        context={"foo": 867},
    )
    assert neighbors.flat_graph()["applyneighborhood1"] == {
        "process_id": "apply_neighborhood",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": {"mean1": {
                "process_id": "mean",
                "arguments": {"data": {"from_parameter": "data"}},
                "result": True,
            }}},
            "size": [{"dimension": "x", "unit": "px", "value": 128}, {"dimension": "y", "unit": "px", "value": 128}],
            "overlap": [{"dimension": "t", "value": "P10d"}],
            "context": {"foo": 867},
        },
        "result": True}


def test_apply_neighborhood_udf(con100):
    collection = con100.load_collection("S2")
    neighbors = collection.apply_neighborhood(
        process=lambda data: data.run_udf(udf="myfancycode", runtime="Python"),
        size=[{"dimension": "x", "value": 128, "unit": "px"}, {"dimension": "y", "value": 128, "unit": "px"}],
        overlap=[{"dimension": "t", "value": "P10d"}],
    )
    assert neighbors.flat_graph()["applyneighborhood1"] == {
        "process_id": "apply_neighborhood",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": {"runudf1": {
                "process_id": "run_udf",
                "arguments": {"udf": "myfancycode", "data": {"from_parameter": "data"}, "runtime": "Python"},
                "result": True,
            }}},
            "size": [{"dimension": "x", "unit": "px", "value": 128}, {"dimension": "y", "unit": "px", "value": 128}],
            "overlap": [{"dimension": "t", "value": "P10d"}],
        },
        "result": True}


def test_filter_spatial_callback(con100):
    """
    Experiment test showing how to introduce a callback for preprocessing process arguments
    https://github.com/Open-EO/openeo-processes/issues/156
    @param con100:
    @return:
    """
    collection = con100.load_collection("S2")

    feature_collection = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [125.6, 10.1]
            }
        }]
    }
    from openeo.processes import run_udf
    udf_code = "def transform_point_into_bbox(data:UdfData): blabla"
    feature_collection_processed = run_udf(data=feature_collection, udf=udf_code, runtime="Python")

    filtered_collection = collection.process("filter_spatial", {
        "data": THIS,
        "geometries": feature_collection_processed
    })

    assert filtered_collection.flat_graph() == {
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


def test_filter_labels_callback(con100):
    cube = con100.load_collection("S2")
    cube = cube.filter_labels(condition=lambda t: t.text_contains("-02-2"), dimension="t")
    assert get_download_graph(cube, drop_save_result=True, drop_load_collection=True) == {
        "filterlabels1": {
            "process_id": "filter_labels",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "condition": {
                    "process_graph": {
                        "textcontains1": {
                            "process_id": "text_contains",
                            "arguments": {"data": {"from_parameter": "value"}, "pattern": "-02-2"},
                            "result": True,
                        }
                    }
                },
                "dimension": "t",
            },
        },
    }


def test_custom_process_kwargs_datacube(con100: Connection, test_data):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", data=img, bar=123)
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_kwargs_datacube_pg(con100: Connection, test_data):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", data=img._pg, bar=123)
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_kwargs_this(con100: Connection, test_data):
    res = con100.load_collection("S2").process(process_id="foo", data=THIS, bar=123)
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_kwargs_namespaced(con100: Connection, test_data):
    res = con100.load_collection("S2").process(process_id="foo", data=THIS, bar=123, namespace="bar")
    expected = test_data.load_json("1.0.0/process_foo_namespaced.json")
    assert res.flat_graph() == expected


def test_custom_process_arguments_datacube(con100: Connection, test_data):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", arguments={"data": img, "bar": 123})
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_arguments_datacube_pg(con100: Connection, test_data):
    img = con100.load_collection("S2")
    res = img.process(process_id="foo", arguments={"data": img._pg, "bar": 123})
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_arguments_this(con100: Connection, test_data):
    res = con100.load_collection("S2").process(process_id="foo", arguments={"data": THIS, "bar": 123})
    expected = test_data.load_json("1.0.0/process_foo.json")
    assert res.flat_graph() == expected


def test_custom_process_arguments_namespacd(con100: Connection, test_data):
    res = con100.load_collection("S2").process(process_id="foo", arguments={"data": THIS, "bar": 123}, namespace="bar")
    expected = test_data.load_json("1.0.0/process_foo_namespaced.json")
    assert res.flat_graph() == expected



@pytest.mark.parametrize("api_capabilities", [{"udp": True}])
def test_save_user_defined_process(con100, requests_mock, test_data):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})

    expected_body = test_data.load_json("1.0.0/save_user_defined_process.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == expected_body['process_graph']
        assert not body.get('public', False)
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/my_udp", additional_matcher=check_body)

    collection = con100.load_collection("S2") \
        .filter_bbox(west=16.1, east=16.6, north=48.6, south=47.2) \
        .filter_temporal(start_date="2018-01-01", end_date="2019-01-01")

    collection.save_user_defined_process(user_defined_process_id='my_udp')

    assert adapter.called


@pytest.mark.parametrize("api_capabilities", [{"udp": True}])
def test_save_user_defined_process_public(con100, requests_mock, test_data):
    requests_mock.get(API_URL + "/processes", json={"processes": [{"id": "add"}]})

    expected_body = test_data.load_json("1.0.0/save_user_defined_process.json")

    def check_body(request):
        body = request.json()
        assert body['process_graph'] == expected_body['process_graph']
        assert body['public']
        return True

    adapter = requests_mock.put(API_URL + "/process_graphs/my_udp", additional_matcher=check_body)

    collection = con100.load_collection("S2") \
        .filter_bbox(west=16.1, east=16.6, north=48.6, south=47.2) \
        .filter_temporal(start_date="2018-01-01", end_date="2019-01-01")

    collection.save_user_defined_process(user_defined_process_id='my_udp', public=True)

    assert adapter.called


def test_save_result_format(con100, requests_mock):
    requests_mock.get(API_URL + "/file_formats", json={
        "output": {
            "GTiff": {"gis_data_types": ["raster"]},
            "PNG": {"gis_data_types": ["raster"]},
        }
    })

    cube = con100.load_collection("S2")
    with pytest.raises(ValueError):
        cube.save_result(format="hdmi")
    cube.save_result(format="GTiff")
    cube.save_result(format="gtIFF")
    cube.save_result(format="pNg")


EXPECTED_JSON_EXPORT_S2_NDVI = textwrap.dedent('''\
  {
    "process_graph": {
      "loadcollection1": {
        "process_id": "load_collection",
        "arguments": {
          "id": "S2",
          "spatial_extent": null,
          "temporal_extent": null
        }
      },
      "ndvi1": {
        "process_id": "ndvi",
        "arguments": {
          "data": {
            "from_node": "loadcollection1"
          }
        },
        "result": true
      }
    }
  }''')


def test_to_json(con100):
    ndvi = con100.load_collection("S2").ndvi()
    assert ndvi.to_json() == EXPECTED_JSON_EXPORT_S2_NDVI


def test_to_json_compact(con100):
    ndvi = con100.load_collection("S2").ndvi()
    expected = '{"process_graph": {"loadcollection1": {"process_id": "load_collection", "arguments": {"id": "S2", "spatial_extent": null, "temporal_extent": null}}, "ndvi1": {"process_id": "ndvi", "arguments": {"data": {"from_node": "loadcollection1"}}, "result": true}}}'
    assert ndvi.to_json(indent=None) == expected
    expected = '{"process_graph":{"loadcollection1":{"process_id":"load_collection","arguments":{"id":"S2","spatial_extent":null,"temporal_extent":null}},"ndvi1":{"process_id":"ndvi","arguments":{"data":{"from_node":"loadcollection1"}},"result":true}}}'
    assert ndvi.to_json(indent=None, separators=(",", ":")) == expected


def test_print_json_default(con100, capsys):
    ndvi = con100.load_collection("S2").ndvi()
    ndvi.print_json()
    stdout, stderr = capsys.readouterr()
    assert stdout == EXPECTED_JSON_EXPORT_S2_NDVI + "\n"


def test_print_json_file(con100):
    ndvi = con100.load_collection("S2").ndvi()
    f = io.StringIO()
    ndvi.print_json(file=f)
    assert f.getvalue() == EXPECTED_JSON_EXPORT_S2_NDVI + "\n"


@pytest.mark.parametrize("path_factory", [str, pathlib.Path])
def test_print_json_file_path(con100, tmp_path, path_factory):
    ndvi = con100.load_collection("S2").ndvi()
    path = tmp_path / "dump.json"
    assert not path.exists()
    ndvi.print_json(file=path_factory(path))
    assert path.exists()
    assert path.read_text() == EXPECTED_JSON_EXPORT_S2_NDVI + "\n"


def test_sar_backscatter_defaults(con100):
    cube = con100.load_collection("S2").sar_backscatter()
    assert _get_leaf_node(cube) == {
        "process_id": "sar_backscatter",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "coefficient": "gamma0-terrain",
            "elevation_model": None,
            "mask": False,
            "contributing_area": False,
            "local_incidence_angle": False,
            "ellipsoid_incidence_angle": False,
            "noise_removal": True,
        },
        "result": True,
    }


def test_sar_backscatter_custom(con100):
    cube = con100.load_collection("S2")
    cube = cube.sar_backscatter(
        coefficient="sigma0-ellipsoid",
        elevation_model="mapzen",
        options={"speed": "warp42"},
    )
    assert _get_leaf_node(cube) == {
        "process_id": "sar_backscatter",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "coefficient": "sigma0-ellipsoid",
            "elevation_model": "mapzen",
            "mask": False,
            "contributing_area": False,
            "local_incidence_angle": False,
            "ellipsoid_incidence_angle": False,
            "noise_removal": True,
            "options": {"speed": "warp42"},
        },
        "result": True,
    }


def test_sar_backscatter_coefficient_none(con100):
    cube = con100.load_collection("S2")
    cube = cube.sar_backscatter(coefficient=None)
    assert _get_leaf_node(cube)["arguments"]["coefficient"] is None


def test_sar_backscatter_coefficient_invalid(con100):
    cube = con100.load_collection("S2")
    with pytest.raises(OpenEoClientException, match="Invalid.*coef.*unicorn.*Should.*sigma0-ellipsoid.*gamma0-terrain"):
        cube.sar_backscatter(coefficient="unicorn")


def test_datacube_from_process(con100):
    cube = con100.datacube_from_process("colorize", color="red", size=4)
    assert cube.flat_graph() == {
        "colorize1": {"process_id": "colorize", "arguments": {"color": "red", "size": 4}, "result": True}
    }


def test_datacube_from_process_namespace(con100):
    cube = con100.datacube_from_process("colorize", namespace="foo", color="red")
    assert cube.flat_graph() == {
        "colorize1": {"process_id": "colorize", "namespace": "foo", "arguments": {"color": "red"}, "result": True}
    }


def test_datacube_from_process_no_warnings(con100, caplog, recwarn):
    """https://github.com/Open-EO/openeo-python-client/issues/442"""
    caplog.set_level(logging.INFO)
    _ = con100.datacube_from_process("colorize", color="red", size=4)
    assert caplog.messages == []
    assert recwarn.list == []


class TestDataCubeFromFlatGraph:

    def test_datacube_from_flat_graph_minimal(self, con100):
        flat_graph = {"1+2": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}
        cube = con100.datacube_from_flat_graph(flat_graph)
        assert cube.flat_graph() == {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}

    def test_datacube_from_json_minimal_string(self, con100):
        udp_json = '''{"1+2": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": true}}'''
        cube = con100.datacube_from_json(udp_json)
        assert cube.flat_graph() == {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}

    @pytest.mark.parametrize("path_factory", [str, pathlib.Path])
    def test_datacube_from_json_minimal_file(self, con100, tmp_path, path_factory):
        path = tmp_path / "pg.json"
        with path.open("w") as f:
            f.write('''{"1+2": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": true}}''')
        cube = con100.datacube_from_json(path_factory(path))
        assert cube.flat_graph() == {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}

    def test_datacube_from_json_minimal_http(self, con100, requests_mock):
        url = "https://jzon.test/data/add.json"
        requests_mock.get(url, json={"1+2": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}})
        cube = con100.datacube_from_json(url)
        assert cube.flat_graph() == {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}

    def test_process_dict_wrapper(self, con100):
        flat_graph = {
            "id": "one-plus-two",
            "summary": "One plus two as a service",
            "process_graph": {"1+2": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}
        }
        cube = con100.datacube_from_flat_graph(flat_graph)
        assert cube.flat_graph() == {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}}

    def test_parameter_substitution_minimal(self, con100):
        flat_graph = {
            "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}, "result": True},
        }
        cube = con100.datacube_from_flat_graph(flat_graph, parameters={"f": 86})
        assert cube.flat_graph() == {
            "subtract1": {"process_id": "subtract", "arguments": {"x": 86, "y": 32}, "result": True},
        }

    def test_parameter_substitution_cube(self, con100):
        flat_graph = {
            "kernel": {"process_id": "constant", "arguments": {"x": [[1, 2, 1], [2, 5, 2], [1, 2, 1]]}},
            "blur": {
                "process_id": "apply_kernel",
                "arguments": {"data": {"from_parameter": "cube"}, "kernel": {"from_node": "kernel"}},
                "result": True
            },
        }
        input_cube = con100.load_collection("S2")
        cube = con100.datacube_from_flat_graph(flat_graph, parameters={"cube": input_cube})
        assert cube.flat_graph() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None}
            },
            "constant1": {"process_id": "constant", "arguments": {"x": [[1, 2, 1], [2, 5, 2], [1, 2, 1]]}},
            "applykernel1": {
                "process_id": "apply_kernel",
                "arguments": {"data": {"from_node": "loadcollection1"}, "kernel": {"from_node": "constant1"}},
                "result": True
            },
        }

    def test_parameter_substitution_udp(self, con100):
        flat_graph = {
            "id": "fahrenheit_to_celsius",
            "parameters": [{"description": "Degrees Fahrenheit.", "name": "f", "schema": {"type": "number"}}],
            "process_graph": {
                "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}},
                "div1": {"process_id": "divide", "arguments": {"x": {"from_node": "sub1"}, "y": 1.8}, "result": True},
            },
        }
        cube = con100.datacube_from_flat_graph(flat_graph, parameters={"f": 86})
        assert cube.flat_graph() == {
            "subtract1": {"process_id": "subtract", "arguments": {"x": 86, "y": 32}},
            "divide1": {
                "process_id": "divide", "arguments": {"x": {"from_node": "subtract1"}, "y": 1.8}, "result": True
            },
        }

    def test_parameter_substitution_parameter_again(self, con100):
        flat_graph = {
            "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}, "result": True},
        }
        cube = con100.datacube_from_flat_graph(flat_graph, parameters={"f": Parameter("warmth")})
        assert cube.flat_graph() == {
            "subtract1": {"process_id": "subtract", "arguments": {
                "x": {"from_parameter": "warmth"}, "y": 32
            }, "result": True},
        }

    def test_parameter_substitution_no_params(self, con100):
        flat_graph = {
            "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}, "result": True},
        }
        with pytest.raises(ProcessGraphVisitException, match="No substitution value for parameter 'f'"):
            _ = con100.datacube_from_flat_graph(flat_graph)

    def test_parameter_substitution_missing_params(self, con100):
        flat_graph = {
            "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}, "result": True},
        }
        with pytest.raises(ProcessGraphVisitException, match="No substitution value for parameter 'f'"):
            _ = con100.datacube_from_flat_graph(flat_graph, parameters={"something else": 42})

    @pytest.mark.parametrize(["kwargs", "expected"], [
        ({}, 100),
        ({"parameters": {}}, 100),
        ({"parameters": {"f": 86}}, 86),
    ])
    def test_parameter_substitution_default(self, con100, kwargs, expected):
        flat_graph = {
            "id": "fahrenheit_to_celsius",
            "parameters": [{"name": "f", "schema": {"type": "number"}, "default": 100}],
            "process_graph": {
                "sub1": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "f"}, "y": 32}},
                "div1": {"process_id": "divide", "arguments": {"x": {"from_node": "sub1"}, "y": 1.8}, "result": True},
            },
        }
        cube = con100.datacube_from_flat_graph(flat_graph, **kwargs)
        assert cube.flat_graph() == {
            "subtract1": {"process_id": "subtract", "arguments": {"x": expected, "y": 32}},
            "divide1": {
                "process_id": "divide", "arguments": {"x": {"from_node": "subtract1"}, "y": 1.8}, "result": True
            },
        }

    def test_load_collection_properties(self, con100):
        flat_graph = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "SENTINEL2_L1C_SENTINELHUB",
                    "properties": {"eo:cloud_cover": {"process_graph": {
                        "lte1": {
                            "process_id": "lte",
                            "arguments": {"x": {"from_parameter": "value"}, "y": 70},
                            "result": True},
                    }}},
                },
                "result": True,
            },
        }
        cube = con100.datacube_from_flat_graph(flat_graph)
        assert isinstance(cube, DataCube)
        assert cube.flat_graph() == flat_graph

    def test_reduce_callback(self, con100):
        flat_graph = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL2_L1C_SENTINELHUB"},
            },
            "reducedimension1": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "dimension": "t",
                    "reducer": {"process_graph": {
                        "mean1": {
                            "process_id": "mean",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        },
                    }}
                },
                "result": True,
            }
        }
        cube = con100.datacube_from_flat_graph(flat_graph)
        assert isinstance(cube, DataCube)
        assert cube.flat_graph() == flat_graph


def test_send_nan_json(con100, requests_mock):
    """https://github.com/Open-EO/openeo-python-client/issues/185"""
    cube = con100.load_collection("S2")
    cube = cube.mask(cube > 100, replacement=float("nan"))
    with pytest.raises(requests.exceptions.InvalidJSONError, match="not JSON compliant"):
        cube.execute()


def test_dimension_labels(con100):
    cube = con100.load_collection("S2").dimension_labels("bands")
    assert cube.flat_graph() == {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        },
        'dimensionlabels1': {
            'process_id': 'dimension_labels',
            'arguments': {'data': {'from_node': 'loadcollection1'}, 'dimension': 'bands'},
            'result': True
        },
    }


def test_dimension_labels_invalid(con100):
    # Validate dimension name by default
    with pytest.raises(ValueError, match="Invalid dimension name 'unv6lidd'"):
        con100.load_collection("S2").dimension_labels("unv6lidd")

    # Don't validate when no metadata
    cube = con100.load_collection("S2", fetch_metadata=False).dimension_labels("unv6lidd")
    assert cube.flat_graph()["dimensionlabels1"]["arguments"]["dimension"] == "unv6lidd"


def test_rename_labels_bands(con100):
    cube = con100.load_collection("S2").rename_labels("bands", target=["blue", "green"], source=["B02", "B03"])
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "renamelabels1": {
            "process_id": "rename_labels",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "bands",
                "target": ["blue", "green"],
                "source": ["B02", "B03"],
            },
            "result": True,
        },
    }


def test_rename_labels_temporal(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/274"""
    cube = con100.load_collection("S2").rename_labels("t", target=["2019", "2020", "2021"])
    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "renamelabels1": {
            "process_id": "rename_labels",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "target": ["2019", "2020", "2021"],
            },
            "result": True,
        },
    }


def test_fit_curve_callback(con100: Connection):
    from openeo.processes import array_element

    def model(x, parameters):
        return array_element(parameters, 0) + array_element(parameters, 1) * x

    img = con100.load_collection("S2")
    res = img.fit_curve(parameters=[0, 0], function=model, dimension="t")
    expected = {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        },
        'fitcurve1': {
            'process_id': 'fit_curve',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'parameters': [0, 0],
                'function': {
                    'process_graph': {
                        'arrayelement1': {
                            'process_id': 'array_element',
                            'arguments': {'data': {'from_parameter': 'parameters'}, 'index': 0},
                        },
                        'arrayelement2': {
                            'process_id': 'array_element',
                            'arguments': {'data': {'from_parameter': 'parameters'}, 'index': 1},
                        },
                        'multiply1': {
                            'process_id': 'multiply',
                            'arguments': {'x': {'from_node': 'arrayelement2'}, 'y': {'from_parameter': 'x'}},
                        },
                        'add1': {
                            'process_id': 'add',
                            'arguments': {'x': {'from_node': 'arrayelement1'}, 'y': {'from_node': 'multiply1'}},
                            'result': True
                        },
                    }
                },
                'dimension': 't',
            },
            'result': True
        },
    }
    assert res.flat_graph() == expected


def test_predict_curve_callback(con100: Connection):
    from openeo.processes import array_element, cos
    def model(x, parameters):
        return array_element(parameters, 0) * cos(array_element(parameters, 1) * x)

    img = con100.load_collection("S2")
    res = img.predict_curve(parameters=[0, 0], function=model, dimension="t")
    expected = {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
        },
        'predictcurve1': {
            'process_id': 'predict_curve',
            'arguments': {
                'data': {'from_node': 'loadcollection1'},
                'parameters': [0, 0],
                'function': {
                    'process_graph': {
                        'arrayelement1': {
                            'process_id': 'array_element',
                            'arguments': {'data': {'from_parameter': 'parameters'}, 'index': 0},
                        },
                        'arrayelement2': {
                            'process_id': 'array_element',
                            'arguments': {'data': {'from_parameter': 'parameters'}, 'index': 1},
                        },
                        'multiply1': {
                            'process_id': 'multiply',
                            'arguments': {'x': {'from_node': 'arrayelement2'}, 'y': {'from_parameter': 'x'}},
                        },
                        'cos1': {
                            'process_id': 'cos',
                            'arguments': {'x': {'from_node': "multiply1"}},
                        },
                        'multiply2': {
                            'process_id': 'multiply',
                            'arguments': {'x': {'from_node': 'arrayelement1'}, 'y': {'from_node': 'cos1'}},
                            'result': True
                        },
                    }
                },
                'dimension': 't',
                'labels': None,
            },
            'result': True
        },
    }
    assert res.flat_graph() == expected


def test_validation(con100, requests_mock):
    def validation(request, context):
        assert request.json() == {"process_graph": {
            'loadcollection1': {
                'process_id': 'load_collection',
                'arguments': {'id': 'S2', 'spatial_extent': None, 'temporal_extent': None},
                'result': True,
            }
        }}
        return {"errors": [{"code": "Invalid", "message": "Invalid process graph"}]}

    m = requests_mock.post(API_URL + "/validation", json=validation)

    cube = con100.load_collection("S2")
    errors = cube.validate()
    assert errors == [{"code": "Invalid", "message": "Invalid process graph"}]
    assert m.call_count == 1


def test_flatten_dimensions(con100):
    s2 = con100.load_collection("S2")
    cube = s2.flatten_dimensions(dimensions=["t", "bands"], target_dimension="features")
    assert _get_leaf_node(cube) == {
        "process_id": "flatten_dimensions",
        "arguments": {
            "data": {"from_node": "loadcollection1"}, "dimensions": ["t", "bands"], "target_dimension": "features"
        },
        "result": True
    }
    cube = s2.flatten_dimensions(dimensions=["t", "bands"], target_dimension="features", label_separator="+")
    assert _get_leaf_node(cube) == {
        "process_id": "flatten_dimensions",
        "arguments": {
            "data": {"from_node": "loadcollection1"}, "dimensions": ["t", "bands"], "target_dimension": "features",
            "label_separator": "+",
        },
        "result": True
    }


def test_unflatten_dimension(con100):
    s2 = con100.load_collection("S2")
    cube = s2.unflatten_dimension(dimension="features", target_dimensions=["t", "bands"])
    assert _get_leaf_node(cube) == {
        "process_id": "unflatten_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"}, "dimension": "features", "target_dimensions": ["t", "bands"],
        },
        "result": True
    }
    cube = s2.unflatten_dimension(dimension="features", target_dimensions=["t", "bands"], label_separator="+")
    assert _get_leaf_node(cube) == {
        "process_id": "unflatten_dimension",
        "arguments": {
            "data": {"from_node": "loadcollection1"}, "dimension": "features", "target_dimensions": ["t", "bands"],
            "label_separator": "+",
        },
        "result": True
    }


def test_merge_if(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/275"""
    from openeo.processes import eq, if_

    s1 = con100.load_collection("S2")
    s2 = con100.load_collection("SENTINEL2_RADIOMETRY_10M")
    s3 = if_(eq("foo", "bar"), s1, s2)
    cube = s1.merge_cubes(s3)

    assert cube.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "loadcollection2": {
            "process_id": "load_collection",
            "arguments": {"id": "SENTINEL2_RADIOMETRY_10M", "spatial_extent": None, "temporal_extent": None},
        },
        "eq1": {"process_id": "eq", "arguments": {"x": "foo", "y": "bar"}},
        "if1": {
            "process_id": "if",
            "arguments": {
                "value": {"from_node": "eq1"},
                "accept": {"from_node": "loadcollection1"}, "reject": {"from_node": "loadcollection2"}},
        },
        "mergecubes1": {
            "process_id": "merge_cubes",
            "arguments": {
                "cube1": {"from_node": "loadcollection1"},
                "cube2": {"from_node": "if1"},
            },
            "result": True,
        },
    }


def test_update_arguments_basic(con100):
    s2 = con100.load_collection("S2")
    s2.result_node().update_arguments(feature_flags="fluzbaxing")
    assert s2.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "S2",
                "spatial_extent": None,
                "temporal_extent": None,
                "feature_flags": "fluzbaxing",
            },
            "result": True,
        }
    }


def test_update_arguments_priority(con100):
    s2 = con100.load_collection("S2")
    s2.result_node().update_arguments(id="T3")
    assert s2.flat_graph() == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "T3", "spatial_extent": None, "temporal_extent": None},
            "result": True,
        }
    }


@pytest.mark.parametrize(["math", "process", "args"], [
    (lambda c: c + 1, "add", {"x": {"from_parameter": "x"}, "y": 1}),
    (lambda c: 1 + c, "add", {"x": 1, "y": {"from_parameter": "x"}}),
    (lambda c: c - 1.2, "subtract", {"x": {"from_parameter": "x"}, "y": 1.2}),
    (lambda c: 1.2 - c, "subtract", {"x": 1.2, "y": {"from_parameter": "x"}}),
    (lambda c: c * 2.5, "multiply", {"x": {"from_parameter": "x"}, "y": 2.5}),
    (lambda c: 2.5 * c, "multiply", {"x": 2.5, "y": {"from_parameter": "x"}}),
    (lambda c: c / 3, "divide", {"x": {"from_parameter": "x"}, "y": 3}),
    (lambda c: 3 / c, "divide", {"x": 3, "y": {"from_parameter": "x"}}),
    (lambda c: c > 4, "gt", {"x": {"from_parameter": "x"}, "y": 4}),
    (lambda c: 4 > c, "lt", {"x": {"from_parameter": "x"}, "y": 4}),
    (lambda c: c == 4, "eq", {"x": {"from_parameter": "x"}, "y": 4}),
    (lambda c: 4 == c, "eq", {"x": {"from_parameter": "x"}, "y": 4}),
])
def test_apply_math_simple(con100, math, process, args):
    """https://github.com/Open-EO/openeo-python-client/issues/323"""
    cube = con100.load_collection("S2")
    res = math(cube)
    graph = res.flat_graph()
    assert set(graph.keys()) == {"loadcollection1", "apply1"}
    assert graph["apply1"] == {
        "process_id": "apply",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": {
                f"{process}1": {
                    "process_id": process,
                    "arguments": args,
                    "result": True,
                }
            }}
        },
        "result": True,
    }


@pytest.mark.parametrize(
    ["math", "apply_pg"],
    [
        (
            lambda c: c + 1,
            {
                "add1": {
                    "process_id": "add",
                    "arguments": {"x": {"from_parameter": "x"}, "y": 1},
                    "result": True,
                },
            },
        ),
        (
            lambda c: 1 + c + 2,
            {
                "add1": {
                    "process_id": "add",
                    "arguments": {"x": 1, "y": {"from_parameter": "x"}},
                },
                "add2": {
                    "process_id": "add",
                    "arguments": {"x": {"from_node": "add1"}, "y": 2},
                    "result": True,
                },
            },
        ),
        (
            lambda c: 1 - c - 2,
            {
                "subtract1": {
                    "process_id": "subtract",
                    "arguments": {"x": 1, "y": {"from_parameter": "x"}},
                },
                "subtract2": {
                    "process_id": "subtract",
                    "arguments": {"x": {"from_node": "subtract1"}, "y": 2},
                    "result": True,
                },
            },
        ),
        (
            lambda c: 2 * (3 / c) - 1,
            {
                "divide1": {
                    "process_id": "divide",
                    "arguments": {"x": 3, "y": {"from_parameter": "x"}},
                },
                "multiply1": {
                    "process_id": "multiply",
                    "arguments": {"x": 2, "y": {"from_node": "divide1"}},
                },
                "subtract1": {
                    "process_id": "subtract",
                    "arguments": {"x": {"from_node": "multiply1"}, "y": 1},
                    "result": True,
                },
            },
        ),
        (
            lambda c: 0 * c,
            {
                "multiply1": {
                    "process_id": "multiply",
                    "arguments": {"x": 0, "y": {"from_parameter": "x"}},
                    "result": True,
                },
            },
        ),
        (
            lambda c: 10 * c.log10(),
            {
                "log1": {
                    "process_id": "log",
                    "arguments": {"x": {"from_parameter": "x"}, "base": 10},
                },
                "multiply1": {
                    "process_id": "multiply",
                    "arguments": {"x": 10, "y": {"from_node": "log1"}},
                    "result": True,
                },
            },
        ),
        (
            lambda c: ~c,
            {
                "not1": {
                    "process_id": "not",
                    "arguments": {"x": {"from_parameter": "x"}},
                    "result": True,
                },
            },
        ),
        (
            lambda c: ~(c == 5),
            {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "x"}, "y": 5},
                },
                "not1": {
                    "process_id": "not",
                    "arguments": {"x": {"from_node": "eq1"}},
                    "result": True,
                },
            },
        ),
    ],
)
def test_apply_more_math(con100, math, apply_pg):
    """https://github.com/Open-EO/openeo-python-client/issues/123"""
    cube = con100.load_collection("S2")
    res = math(cube)
    graph = res.flat_graph()
    assert set(graph.keys()) == {"loadcollection1", "apply1"}
    assert graph["apply1"] == {
        "process_id": "apply",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {"process_graph": apply_pg},
        },
        "result": True,
    }


def test_apply_append_math_keep_context(con100):
    cube = con100.load_collection("S2")
    cube = cube.apply(lambda x: x + 1, context={"foo": 866})
    cube = cube * 123
    graph = cube.flat_graph()
    assert set(graph.keys()) == {"loadcollection1", "apply1"}
    assert graph["apply1"] == {
        "process_id": "apply",
        "arguments": {
            "data": {"from_node": "loadcollection1"},
            "process": {
                "process_graph": {
                    "add1": {
                        "process_id": "add",
                        "arguments": {"x": {"from_parameter": "x"}, "y": 1},
                    },
                    "multiply1": {
                        "process_id": "multiply",
                        "arguments": {"x": {"from_node": "add1"}, "y": 123},
                        "result": True,
                    },
                }
            },
            "context": {"foo": 866},
        },
        "result": True,
    }


@pytest.mark.parametrize(
    ["save_result_kwargs", "download_filename", "download_kwargs", "expected"],
    [
        ({}, "result.tiff", {}, b"this is GTiff data"),
        ({}, "result.nc", {}, b"this is netCDF data"),
        ({"format": "GTiff"}, "result.tiff", {}, b"this is GTiff data"),
        ({"format": "GTiff"}, "result.tif", {}, b"this is GTiff data"),
        ({"format": "GTiff"}, "result.nc", {}, b"this is GTiff data"),
        ({}, "result.tiff", {"format": "GTiff"}, b"this is GTiff data"),
        ({}, "result.nc", {"format": "netCDF"}, b"this is netCDF data"),
        ({}, "result.meh", {"format": "netCDF"}, b"this is netCDF data"),
        (
            {"format": "GTiff"},
            "result.tiff",
            {"format": "GTiff"},
            OpenEoClientException(
                "DataCube.download() with explicit output format 'GTiff', but the process graph already has `save_result` node(s) which is ambiguous and should not be combined."
            ),
        ),
        (
            {"format": "netCDF"},
            "result.tiff",
            {"format": "NETCDF"},
            OpenEoClientException(
                "DataCube.download() with explicit output format 'NETCDF', but the process graph already has `save_result` node(s) which is ambiguous and should not be combined."
            ),
        ),
        (
            {"format": "netCDF"},
            "result.json",
            {"format": "JSON"},
            OpenEoClientException(
                "DataCube.download() with explicit output format 'JSON', but the process graph already has `save_result` node(s) which is ambiguous and should not be combined."
            ),
        ),
        ({"options": {}}, "result.tiff", {}, b"this is GTiff data"),
        (
            {"options": {"quality": "low"}},
            "result.tiff",
            {"options": {"quality": "low"}},
            OpenEoClientException(
                "DataCube.download() with explicit output options {'quality': 'low'}, but the process graph already has `save_result` node(s) which is ambiguous and should not be combined."
            ),
        ),
        (
            {"options": {"colormap": "jet"}},
            "result.tiff",
            {"options": {"quality": "low"}},
            OpenEoClientException(
                "DataCube.download() with explicit output options {'quality': 'low'}, but the process graph already has `save_result` node(s) which is ambiguous and should not be combined."
            ),
        ),
    ],
)
def test_save_result_and_download(
    con100,
    requests_mock,
    tmp_path,
    save_result_kwargs,
    download_filename,
    download_kwargs,
    expected,
):
    def post_result(request, context):
        pg = request.json()["process"]["process_graph"]
        process_histogram = collections.Counter(p["process_id"] for p in pg.values())
        assert process_histogram["save_result"] == 1
        format = pg["saveresult1"]["arguments"]["format"]
        return f"this is {format} data".encode("utf8")

    post_result_mock = requests_mock.post(API_URL + "/result", content=post_result)

    cube = con100.load_collection("S2")
    if save_result_kwargs:
        cube = cube.save_result(**save_result_kwargs)

    path = tmp_path / download_filename
    if isinstance(expected, Exception):
        with pytest.raises(type(expected), match=re.escape(str(expected))):
            cube.download(str(path), **download_kwargs)
        assert post_result_mock.call_count == 0
    else:
        cube.download(str(path), **download_kwargs)
        assert path.read_bytes() == expected
        assert post_result_mock.call_count == 1


@pytest.mark.parametrize(
    ["auto_add_save_result", "process_ids"],
    [
        (True, {"load_collection", "save_result"}),
        (False, {"load_collection"}),
    ],
)
def test_download_auto_add_save_result(s2cube, dummy_backend, tmp_path, auto_add_save_result, process_ids):
    path = tmp_path / "result.tiff"
    s2cube.download(path, auto_add_save_result=auto_add_save_result)
    assert set(n["process_id"] for n in dummy_backend.get_pg().values()) == process_ids


class TestBatchJob:
    _EXPECTED_SIMPLE_S2_JOB = {"process": {"process_graph": {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None}
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
            "result": True,
        }
    }}}

    def _get_handler_post_jobs(
            self, expected_post_data: Optional[dict] = None, job_id: str = "myj0b1", add_header=True,
    ):
        """Create `POST /jobs` handler"""
        expected_post_data = expected_post_data or self._EXPECTED_SIMPLE_S2_JOB

        def post_jobs(request, context):
            assert request.json() == expected_post_data
            context.status_code = 201
            if add_header:
                context.headers["OpenEO-Identifier"] = job_id

        return post_jobs

    def test_create_job_basic(self, con100, requests_mock):
        requests_mock.post(API_URL + "/jobs", json=self._get_handler_post_jobs())
        cube = con100.load_collection("S2")
        job = cube.create_job(out_format="GTiff")
        assert job.job_id == "myj0b1"

    @pytest.mark.parametrize(["add_header", "job_id"], [
        (True, "  "),
        (False, None),
    ])
    def test_create_job_invalid_header(self, con100, requests_mock, add_header, job_id):
        requests_mock.post(API_URL + "/jobs", json=self._get_handler_post_jobs(job_id=job_id, add_header=add_header))
        cube = con100.load_collection("S2")
        with pytest.raises(OpenEoClientException, match="response did not contain a valid job id"):
            _ = cube.create_job(out_format="GTiff")

    def test_legacy_send_job(self, con100, requests_mock):
        """Legacy `DataCube.send_job` alias for `create_job"""
        requests_mock.post(API_URL + "/jobs", json=self._get_handler_post_jobs())
        cube = con100.load_collection("S2")
        expected_warning = "Call to deprecated method send_job. (Usage of this legacy method is deprecated. Use `.create_job` instead.) -- Deprecated since version 0.10.0."
        with pytest.warns(UserDeprecationWarning, match=re.escape(expected_warning)):
            job = cube.send_job(out_format="GTiff")
        assert job.job_id == "myj0b1"


class TestUDF:

    def test_apply_udf_basic(self, con100):
        udf = UDF("print('hello world')", runtime="Python")
        cube = con100.load_collection("S2")
        res = cube.apply(udf)

        assert res.flat_graph() == {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
            },
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "process": {
                        "process_graph": {"runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "x"},
                                "runtime": "Python",
                                "udf": "print('hello world')",
                            },
                            "result": True,
                        }},
                    },
                },
                "result": True,
            },
        }

    def test_apply_udf_runtime_detection(self, con100, requests_mock):
        udf = UDF("def foo(x):\n    return x\n")
        cube = con100.load_collection("S2")
        res = cube.apply(udf)

        assert res.flat_graph()["apply1"]["arguments"]["process"] == {
            "process_graph": {"runudf1": {
                "process_id": "run_udf",
                "arguments": {
                    "data": {"from_parameter": "x"},
                    "runtime": "Python",
                    "udf": "def foo(x):\n    return x\n",
                },
                "result": True,
            }},
        }

    @pytest.mark.parametrize(["filename", "udf_code", "expected_runtime"], [
        ("udf-code.py", "def foo(x):\n    return x\n", "Python"),
        ("udf-code.py", "# just empty, but at least with `.py` suffix\n", "Python"),
        ("udf-code-py.txt", "def foo(x):\n    return x\n", "Python"),
        ("udf-code.r", "# R code here\n", "R"),
    ])
    def test_apply_udf_load_from_file(self, con100, tmp_path, filename, udf_code, expected_runtime):
        path = tmp_path / filename
        path.write_text(udf_code)

        udf = UDF.from_file(path)
        cube = con100.load_collection("S2")
        res = cube.apply(udf)

        assert res.flat_graph()["apply1"]["arguments"]["process"] == {
            "process_graph": {"runudf1": {
                "process_id": "run_udf",
                "arguments": {
                    "data": {"from_parameter": "x"},
                    "runtime": expected_runtime,
                    "udf": udf_code,
                },
                "result": True,
            }},
        }

    @pytest.mark.parametrize(["url", "udf_code", "expected_runtime"], [
        ("http://example.com/udf-code.py", "def foo(x):\n    return x\n", "Python"),
        ("http://example.com/udf-code.py", "# just empty, but at least with `.py` suffix\n", "Python"),
        ("http://example.com/udf-code.py&ref=test", "# just empty, but at least with `.py` suffix\n", "Python"),
        ("http://example.com/udf-code.py#test", "# just empty, but at least with `.py` suffix\n", "Python"),
        ("http://example.com/udf-code-py.txt", "def foo(x):\n    return x\n", "Python"),
        ("http://example.com/udf-code.r", "# R code here\n", "R"),
    ])
    def test_apply_udf_load_from_url(self, con100, requests_mock, url, udf_code, expected_runtime):
        requests_mock.get(url, text=udf_code)

        udf = UDF.from_url(url)
        cube = con100.load_collection("S2")
        res = cube.apply(udf)
        assert res.flat_graph()["apply1"]["arguments"]["process"] == {
            "process_graph": {"runudf1": {
                "process_id": "run_udf",
                "arguments": {
                    "data": {"from_parameter": "x"},
                    "runtime": expected_runtime,
                    "udf": udf_code,
                },
                "result": True,
            }},
        }

    @pytest.mark.parametrize(["kwargs"], [
        ({"version": "3.8"},),
        ({"context": {"color": "red"}},),
    ])
    def test_apply_udf_version_and_context(self, con100, kwargs):
        udf = UDF("def foo(x):\n    return x\n", **kwargs)
        cube = con100.load_collection("S2")
        res = cube.apply(udf)

        expected_args = {
            "data": {"from_parameter": "x"},
            "runtime": "Python",
            "udf": "def foo(x):\n    return x\n",
        }
        expected_args.update(kwargs)
        assert res.flat_graph()["apply1"]["arguments"]["process"] == {
            "process_graph": {"runudf1": {
                "process_id": "run_udf",
                "arguments": expected_args,
                "result": True,
            }},
        }

    def test_simple_apply_udf(self, con100):
        udf = UDF("def foo(x):\n    return x\n")
        cube = con100.load_collection("S2")
        res = cube.apply(udf)

        assert res.flat_graph()["apply1"] == {
            "process_id": "apply",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "process": {
                    "process_graph": {"runudf1": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "x"},
                            "runtime": "Python",
                            "udf": "def foo(x):\n    return x\n",
                        },
                        "result": True,
                    }},
                },
            },
            "result": True,
        }

    def test_simple_apply_dimension_udf(self, con100):
        udf = UDF("def foo(x):\n    return x\n")
        cube = con100.load_collection("S2")
        res = cube.apply_dimension(process=udf, dimension="t")

        assert res.flat_graph()["applydimension1"] == {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "process": {
                    "process_graph": {"runudf1": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "runtime": "Python",
                            "udf": "def foo(x):\n    return x\n",
                        },
                        "result": True,
                    }},
                },
            },
            "result": True,
        }

    def test_simple_apply_dimension_udf_legacy(self, con100):
        # TODO #137 #181 #312 remove support for code/runtime/version

        udf_code = "def foo(x):\n    return x\n"
        cube = con100.load_collection("S2")
        res = cube.apply_dimension(code=udf_code, runtime="Python", dimension="t")

        assert res.flat_graph()["applydimension1"] == {
            "process_id": "apply_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "process": {
                    "process_graph": {"runudf1": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "runtime": "Python",
                            "udf": "def foo(x):\n    return x\n",
                        },
                        "result": True,
                    }},
                },
            },
            "result": True,
        }

    def test_simple_reduce_dimension_udf(self, con100):
        udf = UDF("def foo(x):\n    return x\n")
        cube = con100.load_collection("S2")
        res = cube.reduce_dimension(reducer=udf, dimension="t")

        assert res.flat_graph()["reducedimension1"] == {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {"runudf1": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "runtime": "Python",
                            "udf": "def foo(x):\n    return x\n",
                        },
                        "result": True,
                    }},
                },
            },
            "result": True,
        }

    def test_simple_apply_neighborhood_udf(self, con100):
        udf = UDF("def foo(x):\n    return x\n")
        cube = con100.load_collection("S2")
        res = cube.apply_neighborhood(process=udf, size=27)

        assert res.flat_graph()["applyneighborhood1"] == {
            "process_id": "apply_neighborhood",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "size": 27,
                "process": {
                    "process_graph": {"runudf1": {
                        "process_id": "run_udf",
                        "arguments": {
                            "data": {"from_parameter": "data"},
                            "runtime": "Python",
                            "udf": "def foo(x):\n    return x\n",
                        },
                        "result": True,
                    }},
                },
            },
            "result": True,
        }

    def test_run_udf_on_vector_data_cube_generic_datacube_process(self, con100, test_data):
        """
        https://github.com/Open-EO/openeo-python-client/issues/385 with usage pattern:

            res = aggregated.process("run_udf", data=aggregated, udf="...", ...)`
        """
        cube = con100.load_collection("S2")
        geometries = test_data.load_json("geojson/polygon01.json")
        aggregated = cube.aggregate_spatial(geometries=geometries, reducer="mean")

        udf = "def foo(x):\n    return x\n"
        post_processed = aggregated.process(
            "run_udf", data=aggregated, udf=udf, runtime="Python"
        )

        expected = test_data.load_json("1.0.0/run_udf_on_vector_data_cube.json")
        assert post_processed.flat_graph() == expected

    def test_run_udf_on_vector_data_cube_processes_builder(self, con100, test_data):
        """
        https://github.com/Open-EO/openeo-python-client/issues/385 with usage pattern:

            res = openeo.processes.run_udf(data=aggregated, udf="...", ...)`
        """
        cube = con100.load_collection("S2")
        geometries = test_data.load_json("geojson/polygon01.json")
        aggregated = cube.aggregate_spatial(geometries=geometries, reducer="mean")

        udf = "def foo(x):\n    return x\n"
        post_processed = openeo.processes.run_udf(
            data=aggregated, udf=udf, runtime="Python"
        )

        expected = test_data.load_json("1.0.0/run_udf_on_vector_data_cube.json")
        assert post_processed.flat_graph() == expected

    def test_run_udf_on_vector_data_cube_udf_helper(self, con100, test_data):
        """
        https://github.com/Open-EO/openeo-python-client/issues/385 with usage pattern:

            udf = UDF("...")
            res = aggregated.run_udf(udf)
        """
        cube = con100.load_collection("S2")
        geometries = test_data.load_json("geojson/polygon01.json")
        aggregated = cube.aggregate_spatial(geometries=geometries, reducer="mean")

        udf = UDF("def foo(x):\n    return x\n")
        post_processed = aggregated.run_udf(udf)

        expected = test_data.load_json("1.0.0/run_udf_on_vector_data_cube.json")
        assert post_processed.flat_graph() == expected

    def test_run_udf_on_vector_data_cube_udf_helper_with_overrides(self, con100, test_data):
        """
        https://github.com/Open-EO/openeo-python-client/issues/385 with usage pattern:

            udf = UDF("...")
            res = aggregated.run_udf(udf, version="custom")
        """
        cube = con100.load_collection("S2")
        geometries = test_data.load_json("geojson/polygon01.json")
        aggregated = cube.aggregate_spatial(geometries=geometries, reducer="mean")

        udf = UDF("def foo(x):\n    return x\n")
        post_processed = aggregated.run_udf(udf, runtime="Py", version="v4")

        expected = test_data.load_json("1.0.0/run_udf_on_vector_data_cube.json")
        expected["runudf1"]["arguments"]["runtime"] = "Py"
        expected["runudf1"]["arguments"]["version"] = "v4"
        assert post_processed.flat_graph() == expected


def test_to_json_with_if_and_udf(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/470"""
    cube = con100.load_collection("S2")
    cube = openeo.processes.if_(True, cube, cube)
    cube = cube.apply(openeo.UDF("def foo(): pass"))

    flat = cube.flat_graph()
    assert flat == {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {"id": "S2", "spatial_extent": None, "temporal_extent": None},
        },
        "if1": {
            "process_id": "if",
            "arguments": {
                "accept": {"from_node": "loadcollection1"},
                "reject": {"from_node": "loadcollection1"},
                "value": True,
            },
        },
        "apply1": {
            "process_id": "apply",
            "arguments": {
                "data": {"from_node": "if1"},
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "arguments": {
                                "data": {"from_parameter": "x"},
                                "runtime": "Python",
                                "udf": "def " "foo(): " "pass",
                            },
                            "process_id": "run_udf",
                            "result": True,
                        }
                    }
                },
            },
            "result": True,
        },
    }

    assert 'process_id": "run_udf"' in cube.to_json()


def test_aggregate_spatial_band_metadata(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/612"""
    cube = con100.load_collection("S2", bands=["B02", "B03"])
    geometry = shapely.geometry.box(0, 0, 1, 1)
    aggregated = cube.aggregate_spatial(geometries=geometry, reducer="mean")
    assert aggregated.metadata.band_names == ["B02", "B03"]


def test_aggregate_spatial_and_merge_again(con100):
    """https://github.com/Open-EO/openeo-python-client/issues/612"""
    cube = con100.load_collection("S2", bands=["B02", "B03"])
    geometry = shapely.geometry.box(0, 0, 1, 1)
    aggregated = cube.aggregate_spatial(geometries=geometry, reducer="mean")
    rasterized = aggregated.vector_to_raster(target=cube).rename_labels(
        dimension="bands", target=["B02-mean", "B03-mean"]
    )
    merged = cube.merge_cubes(rasterized)
    assert merged.metadata.band_names == ["B02", "B03", "B02-mean", "B03-mean"]
