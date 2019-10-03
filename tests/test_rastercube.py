from datetime import date, datetime
from unittest import TestCase

import numpy as np
import pytest
from mock import MagicMock, patch

from openeo.capabilities import Capabilities
from openeo.rest.connection import Connection
from openeo.graphbuilder import GraphBuilder
from openeo.rest.imagecollectionclient import ImageCollectionClient


@pytest.fixture
def image_collection():
    builder = GraphBuilder()
    id = builder.process("get_collection", {'name': 'S1'})

    connection = MagicMock(spec=Connection)
    capabilities = MagicMock(spec=Capabilities)

    connection.capabilities.return_value = capabilities
    capabilities.version.return_value = "0.4.0"
    return ImageCollectionClient(id, builder, connection)


def test_date_range_filter(image_collection: ImageCollectionClient):
    im = image_collection.date_range_filter("2016-01-01", "2016-03-10")
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_daterange(image_collection: ImageCollectionClient):
    im = image_collection.filter_daterange(extent=("2016-01-01", "2016-03-10"))
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal(image_collection: ImageCollectionClient):
    im = image_collection.filter_temporal("2016-01-01", "2016-03-10")
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_start_end(image_collection: ImageCollectionClient):
    im = image_collection.filter_temporal(start_date="2016-01-01", end_date="2016-03-10")
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


def test_filter_temporal_extent(image_collection: ImageCollectionClient):
    im = image_collection.filter_temporal(extent=("2016-01-01", "2016-03-10"))
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == ["2016-01-01", "2016-03-10"]


@pytest.mark.parametrize("args,kwargs,extent", [
    ((), {}, [None, None]),
    (("2016-01-01",), {}, ["2016-01-01", None]),
    (("2016-01-01", "2016-03-10"), {}, ["2016-01-01", "2016-03-10"]),
    ((date(2016, 1, 1), date(2016, 3, 10)), {}, ["2016-01-01", "2016-03-10"]),
    ((datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45)), {},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
    ((), {"start_date": "2016-01-01", "end_date": "2016-03-10"}, ["2016-01-01", "2016-03-10"]),
    ((), {"start_date": "2016-01-01"}, ["2016-01-01", None]),
    ((), {"end_date": "2016-03-10"}, [None, "2016-03-10"]),
    ((), {"start_date": date(2016, 1, 1), "end_date": date(2016, 3, 10)}, ["2016-01-01", "2016-03-10"]),
    ((), {"start_date": datetime(2016, 1, 1, 12, 34), "end_date": datetime(2016, 3, 10, 23, 45)},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
    ((), {"extent": ("2016-01-01", "2016-03-10")}, ["2016-01-01", "2016-03-10"]),
    ((), {"extent": ("2016-01-01", None)}, ["2016-01-01", None]),
    ((), {"extent": (None, "2016-03-10")}, [None, "2016-03-10"]),
    ((), {"extent": (date(2016, 1, 1), date(2016, 3, 10))}, ["2016-01-01", "2016-03-10"]),
    ((), {"extent": (datetime(2016, 1, 1, 12, 34), datetime(2016, 3, 10, 23, 45))},
     ["2016-01-01T12:34:00Z", "2016-03-10T23:45:00Z"]),
])
def test_filter_temporal_generic(image_collection: ImageCollectionClient, args, kwargs, extent):
    im = image_collection.filter_temporal(*args, **kwargs)
    graph = im.graph[im.node_id]
    assert graph['process_id'] == 'filter_temporal'
    assert graph['arguments']['extent'] == extent


class TestRasterCube(TestCase):

    def setUp(self):
        builder = GraphBuilder()
        id = builder.process("get_collection", {'name': 'S1'})

        connection = MagicMock(spec=Connection)
        capabilities = MagicMock(spec=Capabilities)

        connection.capabilities.return_value = capabilities
        capabilities.version.return_value = "0.4.0"
        self.imagery = ImageCollectionClient(id, builder, connection)

        builder = GraphBuilder()
        mask_id = builder.process("get_collection", {'name': 'S1_Mask'})
        self.mask = ImageCollectionClient(mask_id, builder, connection)

    def test_filter_bbox(self):
        im = self.imagery.filter_bbox(
            west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632"
        )
        graph = im.graph[im.node_id]
        assert graph["process_id"] == "filter_bbox"
        assert graph["arguments"]["extent"] == {
            "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
        }

    def test_filter_bbox_base_height(self):
        im = self.imagery.filter_bbox(
            west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632",
            base=100, height=200,
        )
        graph = im.graph[im.node_id]
        assert graph["process_id"] == "filter_bbox"
        assert graph["arguments"]["extent"] == {
            "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632",
            "base": 100, "height": 200,
        }

    def test_bbox_filter_nsew(self):
        im = self.imagery.bbox_filter(
            west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632"
        )
        graph = im.graph[im.node_id]
        assert graph["process_id"] == "filter_bbox"
        assert graph["arguments"]["extent"] == {
            "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
        }

    def test_bbox_filter_tblr(self):
        im = self.imagery.bbox_filter(
            left=652000, right=672000, top=5161000, bottom=5181000, srs="EPSG:32632"
        )
        graph = im.graph[im.node_id]
        assert graph["process_id"] == "filter_bbox"
        assert graph["arguments"]["extent"] == {
            "west": 652000, "east": 672000, "north": 5161000, "south": 5181000, "crs": "EPSG:32632"
        }

    def test_bbox_filter_nsew_zero(self):
        im = self.imagery.bbox_filter(
            west=0, east=0, north=0, south=0, crs="EPSG:32632"
        )
        graph = im.graph[im.node_id]
        assert graph["process_id"] == "filter_bbox"
        assert graph["arguments"]["extent"] == {
            "west": 0, "east": 0, "north": 0, "south": 0, "crs": "EPSG:32632"
        }

    def test_min_time(self):
        new_imagery = self.imagery.min_time()

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "reduce")
        self.assertIn("data", graph['arguments'])

    def test_max_time(self):
        new_imagery = self.imagery.max_time()

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "reduce")
        self.assertIn("data", graph['arguments'])

    def test_reduce_time_udf(self):
        new_imagery = self.imagery.reduce_tiles_over_time("my custom code")

        graph = new_imagery.graph[new_imagery.node_id]

        import json
        print(json.dumps(graph,indent=2))

        self.assertEqual(graph["process_id"], "reduce")
        self.assertIn("data", graph['arguments'])

    def test_ndvi(self):
        new_imagery = self.imagery.ndvi()

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "ndvi")
        self.assertIn("data", graph['arguments'])

    def test_mask(self):
        from shapely import geometry
        polygon = geometry.Polygon([[0, 0], [1.9, 0], [1.9, 1.9], [0, 1.9]])
        new_imagery = self.imagery.mask(polygon)

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "mask")
        self.assertEqual(graph["arguments"]["mask"],
                         {'coordinates': (((0.0, 0.0), (1.9, 0.0), (1.9, 1.9), (0.0, 1.9), (0.0, 0.0)),),
                          'crs': {'properties': {'name': 'EPSG:4326'}, 'type': 'name'},
                          'type': 'Polygon'})

    def test_mask_raster(self):
        new_imagery = self.imagery.mask(rastermask=self.mask,replacement=102)

        graph = new_imagery.graph[new_imagery.node_id]
        import json
        print(json.dumps(new_imagery.graph,indent=4))

        expected_mask_node = {
            "process_id": "mask",
            "arguments": {
                "data": {
                    "from_node": "getcollection1"
                },
                "mask": {
                    "from_node": "getcollection2"
                },
                "replacement": 102
            },
            "result": False
        }

        self.assertDictEqual(expected_mask_node,graph)

    def test_strech_colors(self):
        new_imagery = self.imagery.stretch_colors(-1, 1)

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "stretch_colors")
        self.assertIn("data", graph['arguments'])
        self.assertEqual(graph["arguments"]["min"], -1)
        self.assertEqual(graph["arguments"]["max"], 1)

    def test_apply_kernel(self):

        kernel = [[0, 1, 0], [1, 1, 1], [0, 1, 0]]
        new_imagery = self.imagery.apply_kernel(np.asarray(kernel), 3)

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "apply_kernel")
        self.assertIn("data", graph["arguments"])
        self.assertEqual(graph["arguments"]["factor"], 3)
        self.assertEqual(graph["arguments"]["kernel"], kernel)
