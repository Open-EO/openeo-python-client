import pytest
import shapely

from openeo.extra.job_management.job_splitting import (
    BoundingBox,
    JobSplittingFailure,
    SizeBasedTileGrid,
    reproject_bounding_box,
    split_area,
)


@pytest.fixture
def mock_polygon_wgs():
    return shapely.geometry.box(0.0, 0.0, 1.0, 1.0)


@pytest.fixture
def mock_polygon_utm():
    return shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0)


@pytest.fixture
def mock_dict_no_crs():
    return {
        "west": 0.0,
        "south": 0.0,
        "east": 1.0,
        "north": 1.0,
    }


@pytest.fixture
def mock_dict_with_crs_utm():
    return {
        "west": 0.0,
        "south": 0.0,
        "east": 100_000.0,
        "north": 100_000.0,
        "crs": "EPSG:3857",
    }


@pytest.mark.parametrize(
    ["crs", "bbox"],
    [
        (
            "EPSG:32631",
            {"west": 640800, "south": 5676000, "east": 642200, "north": 5677000},
        ),
        ("EPSG:4326", {"west": 5.01, "south": 51.2, "east": 5.1, "north": 51.5}),
    ],
)
def test_reproject_bounding_box_same(crs, bbox):
    reprojected = reproject_bounding_box(bbox, from_crs=crs, to_crs=crs)
    assert reprojected == dict(crs=crs, **bbox)


def test_reproject_bounding_box():
    bbox = {"west": 640800, "south": 5676000, "east": 642200.0, "north": 5677000.0}
    reprojected = reproject_bounding_box(bbox, from_crs="EPSG:32631", to_crs="EPSG:4326")
    assert reprojected == {
        "west": pytest.approx(5.016118467277098),
        "south": pytest.approx(51.217660146353246),
        "east": pytest.approx(5.036548264535997),
        "north": pytest.approx(51.22699369149726),
        "crs": "EPSG:4326",
    }


class TestBoundingBox:
    def test_basic(self):
        bbox = BoundingBox(1, 2, 3, 4)
        assert bbox.west == 1
        assert bbox.south == 2
        assert bbox.east == 3
        assert bbox.north == 4
        assert bbox.crs == "EPSG:4326"

    def test_from_dict(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "epsg:32633"})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "epsg:32633"

    def test_from_dict_defaults(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "EPSG:4326"

    def test_from_dict_underspecified(self):
        with pytest.raises(KeyError):
            _ = BoundingBox.from_dict({"west": 1, "south": 2, "color": "red"})

    def test_from_dict_overspecified(self):
        bbox = BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326", "color": "red"})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "EPSG:4326"

    def test_from_polygon(self):
        polygon = shapely.geometry.box(1, 2, 3, 4)
        bbox = BoundingBox.from_polygon(polygon, projection="EPSG:4326")
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == "EPSG:4326"

    def test_as_dict(self):
        bbox = BoundingBox(1, 2, 3, 4)
        assert bbox.as_dict() == {"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326"}

    def test_as_polygon(self):
        bbox = BoundingBox(1, 2, 3, 4)
        polygon = bbox.as_polygon()
        assert isinstance(polygon, shapely.geometry.Polygon)
        assert set(polygon.exterior.coords) == {(1, 2), (3, 2), (3, 4), (1, 4)}


class TestSizeBasedTileGrid:

    def test_from_size_projection(self):
        splitter = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        assert splitter.epsg == "epsg:4326"
        assert splitter.size == 0.1

    def test_get_tiles_raises_exception(self):
        """test get_tiles when the input geometry is not a dict or shapely.geometry.Polygon"""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        with pytest.raises(JobSplittingFailure):
            tile_grid.get_tiles("invalid_geometry")

    def test_get_tiles_dict_returns_dict(self, mock_dict_no_crs):
        """test get_tiles when the input geometry dict returns a list of dicts"""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        tiles = tile_grid.get_tiles(mock_dict_no_crs)
        assert isinstance(tiles, list)
        assert all(isinstance(tile, dict) for tile in tiles)

    def test_get_tiles_polygon_returns_polygon(self, mock_polygon_wgs):
        """test get_tiles when the input geometry is a polygon and the tile grid is in wgs"""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        tiles = tile_grid.get_tiles(mock_polygon_wgs)
        assert isinstance(tiles, list)
        assert all(isinstance(tile, shapely.geometry.Polygon) for tile in tiles)

    def test_get_tiles_dict_no_crs_utm(self, mock_dict_no_crs):
        """test get_tiles when the input geometry dict has no crs and the tile grid is in utm"""
        tile_grid = SizeBasedTileGrid.from_size_projection(20.0, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_no_crs)
        assert tiles[0].get("crs") == "EPSG:4326"
        assert len(tiles) == 36

    def test_get_tiles_dict_no_crs_wgs(self, mock_dict_no_crs):
        """test get_tiles when the input geometry dict has no crs and the tile grid is in wgs"""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        tiles = tile_grid.get_tiles(mock_dict_no_crs)
        assert tiles[0].get("crs") == "EPSG:4326"
        assert len(tiles) == 100

    def test_get_tiles_dict_with_crs_same(self, mock_dict_with_crs_utm):
        """test get_tiles when the input geometry dict and the tile grid have the same crs"""
        tile_grid = SizeBasedTileGrid.from_size_projection(20.0, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert tiles[0].get("crs") == "EPSG:3857"
        assert len(tiles) == 25

    def test_get_tiles_dict_with_crs_different(self, mock_dict_with_crs_utm):
        """test get_tiles when the input geometry dict and the tile grid have different crs. The original crs from the geometry should be preserved."""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert tiles[0].get("crs") == "EPSG:3857"
        assert len(tiles) == 81

    def test_simple_get_tiles_dict(self, mock_dict_with_crs_utm):
        """test get_tiles when the the tile grid size is equal to the size of the input geometry. The original geometry should be returned."""
        tile_grid = SizeBasedTileGrid.from_size_projection(100, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_dict_with_crs_utm

    def test_multiple_get_tile_dict(self, mock_dict_with_crs_utm):
        """test get_tiles when the the tile grid size is smaller than the size of the input geometry. The input geometry should be split into multiple tiles."""
        tile_grid = SizeBasedTileGrid.from_size_projection(20, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 25
        assert tiles[0].get("crs") == "EPSG:3857"
        assert tiles[0].get("west") == 0
        assert tiles[0].get("south") == 0
        assert tiles[0].get("east") == 20_000
        assert tiles[0].get("north") == 20_000

    def test_larger_get_tile_dict(self, mock_dict_with_crs_utm):
        """test get_tiles when the the tile grid size is larger than the size of the input geometry. The original geometry should be returned."""
        tile_grid = SizeBasedTileGrid.from_size_projection(200, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_dict_with_crs_utm

    def test_get_tiles_polygon_utm(self, mock_polygon_utm):
        """test get_tiles when the input geometry is a polygon in wgs and the tile grid is in utm"""
        tile_grid = SizeBasedTileGrid.from_size_projection(20.0, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_polygon_utm)
        assert isinstance(tiles, list)
        assert all(isinstance(tile, shapely.geometry.Polygon) for tile in tiles)
        assert len(tiles) == 25
        assert tiles[0] == shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0)

    def test_get_tiles_polygon_wgs(self, mock_polygon_wgs):
        """test get_tiles when the input geometry is a polygon in wgs and the tile grid is in wgs"""
        tile_grid = SizeBasedTileGrid.from_size_projection(0.1, "EPSG:4326")
        tiles = tile_grid.get_tiles(mock_polygon_wgs)
        assert isinstance(tiles, list)
        assert all(isinstance(tile, shapely.geometry.Polygon) for tile in tiles)
        assert len(tiles) == 100
        assert tiles[0] == shapely.geometry.box(0.0, 0.0, 0.1, 0.1)

    def test_simple_get_tiles_polygon(self, mock_polygon_utm):
        """test get_tiles when the the tile grid size is equal to the size of the input geometry. The original geometry should be returned."""
        tile_grid = SizeBasedTileGrid.from_size_projection(100.0, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_polygon_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm

    def test_larger_get_tiles_polygon(self, mock_polygon_utm):
        """test get_tiles when the the tile grid size is larger than the size of the input geometry. The original geometry should be returned."""
        tile_grid = SizeBasedTileGrid.from_size_projection(200.0, "EPSG:3857")
        tiles = tile_grid.get_tiles(mock_polygon_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm


def test_split_area_default():
    """test split_area with default parameters"""
    aoi = {"west": 0.0, "south": 0.0, "east": 20_000.0, "north": 20_000.0, "crs": "EPSG:3857"}
    tiles = split_area(aoi)
    assert len(tiles) == 1
    assert tiles[0] == aoi


def test_split_area_custom():
    """test split_area with wgs projection"""
    aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
    tiles = split_area(aoi, "EPSG:4326", 1.0)
    assert len(tiles) == 1
    assert tiles[0] == aoi
