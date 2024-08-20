from openeo.testing.stac import DummyStacDictBuilder


class TestDummyStacDictBuilder:
    def test_item_default(self):
        assert DummyStacDictBuilder.item() == {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "item123",
            "geometry": None,
            "properties": {"datetime": "2024-03-08"},
        }

    def test_item_cube_dimensions(self):
        assert DummyStacDictBuilder.item(
            cube_dimensions={"t": {"type": "temporal", "extent": ["2024-01-01", "2024-04-04"]}}
        ) == {
            "type": "Feature",
            "stac_version": "1.0.0",
            "stac_extensions": ["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
            "id": "item123",
            "geometry": None,
            "properties": {
                "cube:dimensions": {"t": {"extent": ["2024-01-01", "2024-04-04"], "type": "temporal"}},
                "datetime": "2024-03-08",
            },
        }

    def test_collection_default(self):
        assert DummyStacDictBuilder.collection() == {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "collection123",
            "description": "Collection 123",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[3, 4, 5, 6]]},
                "temporal": {"interval": [["2024-01-01", "2024-05-05"]]},
            },
            "links": [],
        }

    def test_collection_cube_dimensions(self):
        assert DummyStacDictBuilder.collection(
            cube_dimensions={"t": {"type": "temporal", "extent": ["2024-01-01", "2024-04-04"]}}
        ) == {
            "type": "Collection",
            "stac_version": "1.0.0",
            "stac_extensions": ["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
            "id": "collection123",
            "description": "Collection 123",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[3, 4, 5, 6]]},
                "temporal": {"interval": [["2024-01-01", "2024-05-05"]]},
            },
            "cube:dimensions": {"t": {"extent": ["2024-01-01", "2024-04-04"], "type": "temporal"}},
            "links": [],
        }

    def test_catalog_default(self):
        assert DummyStacDictBuilder.catalog() == {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "catalog123",
            "description": "Catalog 123",
            "links": [],
        }
