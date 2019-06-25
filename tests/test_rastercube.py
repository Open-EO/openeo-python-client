from unittest import TestCase

from openeo.capabilities import Capabilities
from openeo.connection import Connection
from openeo.rest.imagecollectionclient import ImageCollectionClient
from openeo.graphbuilder import GraphBuilder
from mock import MagicMock, patch


class TestRasterCube(TestCase):

    @patch.multiple(Connection, __abstractmethods__=set())
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

    def test_date_range_filter(self):
        new_imagery = self.imagery.date_range_filter("2016-01-01", "2016-03-10")

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "filter_temporal")
        self.assertIn("data", graph['arguments'])
        self.assertEqual(graph["arguments"]["from"], "2016-01-01")
        self.assertEqual(graph["arguments"]["to"], "2016-03-10")

    def test_bbox_filter_nsew(self):
        new_imagery = self.imagery.bbox_filter(
            west=652000, east=672000, north=5161000, south=5181000, crs="EPSG:32632"
        )
        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "filter_bbox")
        self.assertIn("data", graph['arguments'])
        self.assertEqual(graph["arguments"]["west"], 652000)
        self.assertEqual(graph["arguments"]["east"], 672000)
        self.assertEqual(graph["arguments"]["north"], 5161000)
        self.assertEqual(graph["arguments"]["south"], 5181000)
        self.assertEqual(graph["arguments"]["crs"], "EPSG:32632")

    def test_bbox_filter_tblr(self):
        new_imagery = self.imagery.bbox_filter(
            left=652000, right=672000, top=5161000, bottom=5181000, srs="EPSG:32632"
        )
        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "filter_bbox")
        self.assertIn("data", graph['arguments'])
        self.assertEqual(graph["arguments"]["west"], 652000)
        self.assertEqual(graph["arguments"]["east"], 672000)
        self.assertEqual(graph["arguments"]["north"], 5161000)
        self.assertEqual(graph["arguments"]["south"], 5181000)
        self.assertEqual(graph["arguments"]["crs"], "EPSG:32632")

    def test_bbox_filter_nsew_zero(self):
        new_imagery = self.imagery.bbox_filter(
            west=0, east=0, north=0, south=0, crs="EPSG:32632"
        )
        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "filter_bbox")
        self.assertIn("data", graph['arguments'])
        self.assertEqual(graph["arguments"]["west"], 0)
        self.assertEqual(graph["arguments"]["east"], 0)
        self.assertEqual(graph["arguments"]["north"], 0)
        self.assertEqual(graph["arguments"]["south"], 0)
        self.assertEqual(graph["arguments"]["crs"], "EPSG:32632")

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
        new_imagery = self.imagery.ndvi("B04", "B8A")

        graph = new_imagery.graph[new_imagery.node_id]

        self.assertEqual(graph["process_id"], "NDVI")
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
        from shapely import geometry

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
