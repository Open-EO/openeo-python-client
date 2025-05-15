import pytest
import shapely

from openeo.extra.job_management._job_splitting import (
    JobSplittingFailure,
    _BoundingBox,
    _SizeBasedTileGrid,
    split_area,
)

# TODO: using fixtures for these simple objects is a bit overkill, makes the test harder to follow, and undermines opportunity to parameterize

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


class TestBoundingBox:
    def test_basic(self):
        bbox = _BoundingBox(1, 2, 3, 4)
        assert bbox.west == 1
        assert bbox.south == 2
        assert bbox.east == 3
        assert bbox.north == 4
        assert bbox.crs == 4326

    def test_from_dict(self):
        bbox = _BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4, "crs": "epsg:32633"})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == 32633

    def test_from_dict_defaults(self):
        bbox = _BoundingBox.from_dict({"west": 1, "south": 2, "east": 3, "north": 4})
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == 4326

    def test_from_dict_underspecified(self):
        with pytest.raises(KeyError):
            _ = _BoundingBox.from_dict({"west": 1, "south": 2, "color": "red"})

    def test_from_dict_overspecified(self):
        bbox = _BoundingBox.from_dict(
            {"west": 1, "south": 2, "east": 3, "north": 4, "crs": "EPSG:4326", "color": "red"}
        )
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == 4326

    def test_from_polygon(self):
        polygon = shapely.geometry.box(1, 2, 3, 4)
        bbox = _BoundingBox.from_polygon(polygon)
        assert (bbox.west, bbox.south, bbox.east, bbox.north) == (1, 2, 3, 4)
        assert bbox.crs == 4326

    def test_as_dict(self):
        bbox = _BoundingBox(1, 2, 3, 4)
        assert bbox.as_dict() == {"west": 1, "south": 2, "east": 3, "north": 4, "crs": 4326}

    def test_as_polygon(self):
        bbox = _BoundingBox(1, 2, 3, 4)
        polygon = bbox.as_polygon()
        assert isinstance(polygon, shapely.geometry.Polygon)
        assert set(polygon.exterior.coords) == {(1, 2), (3, 2), (3, 4), (1, 4)}


class TestSizeBasedTileGrid:
    def test_from_size_projection(self):
        splitter = _SizeBasedTileGrid.from_size_projection(size=0.1, projection="EPSG:4326")
        assert splitter.epsg == 4326
        assert splitter.size == 0.1

    def test_get_tiles_raises_exception(self):
        """test get_tiles when the input geometry is not a dict or shapely.geometry.Polygon"""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=0.1, projection="EPSG:4326")
        with pytest.raises(JobSplittingFailure):
            tile_grid.get_tiles("invalid_geometry")

    def test_simple_get_tiles_dict(self, mock_dict_with_crs_utm, mock_polygon_utm):
        """test get_tiles when the the tile grid size is equal to the size of the input geometry. The original geometry should be returned as polygon."""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=100_000, projection="EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm

    def test_multiple_get_tile_dict(self, mock_dict_with_crs_utm):
        """test get_tiles when the the tile grid size is smaller than the size of the input geometry. The input geometry should be split into multiple tiles."""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=20_000, projection="EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 25
        assert tiles[0] == shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0)

    def test_larger_get_tile_dict(self, mock_dict_with_crs_utm, mock_polygon_utm):
        """test get_tiles when the the tile grid size is larger than the size of the input geometry. The original geometry should be returned."""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=200_000, projection="EPSG:3857")
        tiles = tile_grid.get_tiles(mock_dict_with_crs_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm

    def test_get_tiles_polygon_wgs(self, mock_polygon_wgs):
        """test get_tiles when the input geometry is a polygon in wgs and the tile grid is in wgs"""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=0.1, projection="EPSG:4326")
        tiles = tile_grid.get_tiles(mock_polygon_wgs)
        assert len(tiles) == 100
        assert tiles[0] == shapely.geometry.box(0.0, 0.0, 0.1, 0.1)

    def test_simple_get_tiles_polygon(self, mock_polygon_utm):
        """test get_tiles when the the tile grid size is equal to the size of the input geometry. The original geometry should be returned."""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=100_000.0, projection="EPSG:3857")
        tiles = tile_grid.get_tiles(mock_polygon_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm

    def test_larger_get_tiles_polygon(self, mock_polygon_utm):
        """test get_tiles when the the tile grid size is larger than the size of the input geometry. The original geometry should be returned."""
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=200_000.0, projection="EPSG:3857")
        tiles = tile_grid.get_tiles(mock_polygon_utm)
        assert len(tiles) == 1
        assert tiles[0] == mock_polygon_utm


def test_split_area_default():
    """test split_area with default parameters"""
    aoi = {"west": 0.0, "south": 0.0, "east": 20_000.0, "north": 20_000.0, "crs": "EPSG:3857"}
    tiles = split_area(aoi)
    assert len(tiles) == 1
    assert tiles[0] == shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0)


def test_split_area_custom():
    """test split_area with wgs projection"""
    aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
    tiles = split_area(aoi, "EPSG:4326", 1.0)
    assert len(tiles) == 1
    assert tiles[0] == shapely.geometry.box(0.0, 0.0, 1.0, 1.0)


def test_split_area_custom_no_crs_specified():
    """test split_area with crs in dict, but not in split_area. The crs in the dict should be used."""
    aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
    tiles = split_area(aoi=aoi, tile_size=1.0)
    assert len(tiles) == 1
    assert tiles[0] == shapely.geometry.box(0.0, 0.0, 1.0, 1.0)
