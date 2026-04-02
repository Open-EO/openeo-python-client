import geopandas as gpd
import pytest
import shapely

from openeo.extra.job_management._job_splitting import (
    JobSplittingFailure,
    PredefinedTileGrid,
    SizeBasedTileGrid,
    TileGridInterface,
    split_area,
)
from openeo.util import BBoxDict


class TestSizeBasedTileGrid:
    def test_from_size_projection(self):
        splitter = SizeBasedTileGrid.from_size_projection(size=0.1, projection="EPSG:4326")
        assert splitter.epsg == 4326
        assert splitter.size == 0.1

    def test_constructor_rejects_unparseable_epsg(self):
        """An invalid CRS string should be caught and raised as JobSplittingFailure."""
        with pytest.raises(JobSplittingFailure, match="Failed to normalize EPSG code"):
            SizeBasedTileGrid(epsg="not_a_crs", size=1.0)

    def test_unsupported_epsg_raises(self):
        grid = SizeBasedTileGrid(epsg=2154, size=1000)
        with pytest.raises(JobSplittingFailure, match="Unsupported EPSG code 2154"):
            grid.get_tiles({"west": 0, "south": 0, "east": 1, "north": 1})

    def test_get_tiles_raises_exception(self):
        """test get_tiles when the input geometry is not a dict or shapely.geometry.Polygon"""
        tile_grid = SizeBasedTileGrid(epsg=4326, size=0.1)
        with pytest.raises(JobSplittingFailure):
            tile_grid.get_tiles("invalid_geometry")

    def test_get_tiles_returns_geodataframe(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        tile_grid = SizeBasedTileGrid(epsg=4326, size=1.0)
        result = tile_grid.get_tiles(aoi)
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.crs is not None
        assert result.crs.to_epsg() == 4326

    def test_simple_get_tiles_dict(self):
        """tile grid size equals input geometry size -> single tile returned."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = SizeBasedTileGrid(epsg=3857, size=100_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0))

    def test_multiple_get_tile_dict(self):
        """tile grid smaller than input -> multiple tiles."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = SizeBasedTileGrid(epsg=3857, size=20_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 25
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0))

    def test_larger_get_tile_dict(self):
        """tile grid larger than input -> single tile clipped to input."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = SizeBasedTileGrid(epsg=3857, size=200_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0))

    def test_get_tiles_polygon_wgs(self):
        """polygon in WGS84 with WGS84 tile grid."""
        polygon = shapely.geometry.box(0.0, 0.0, 1.0, 1.0)
        tile_grid = SizeBasedTileGrid(epsg=4326, size=0.1)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 100
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 0.1, 0.1))

    def test_simple_get_tiles_polygon(self):
        """tile grid size equals polygon size -> single tile."""
        polygon = shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0)
        tile_grid = SizeBasedTileGrid(epsg=3857, size=100_000.0)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 1
        assert result.geometry[0].equals(polygon)

    def test_larger_get_tiles_polygon(self):
        """tile grid larger than polygon -> single tile clipped to polygon."""
        polygon = shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0)
        tile_grid = SizeBasedTileGrid(epsg=3857, size=200_000.0)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 1
        assert result.geometry[0].equals(polygon)

    def test_utm_x_offset(self):
        """UTM zones use a 500 000 m false easting offset."""
        grid = SizeBasedTileGrid(epsg=32631, size=100_000)
        assert grid._get_x_offset() == 500_000.0

    def test_3857_no_x_offset(self):
        """EPSG:3857 has no x offset."""
        grid = SizeBasedTileGrid(epsg=3857, size=100_000)
        assert grid._get_x_offset() == 0.0

    def test_4326_no_x_offset(self):
        """EPSG:4326 has no x offset."""
        grid = SizeBasedTileGrid(epsg=4326, size=1.0)
        assert grid._get_x_offset() == 0.0


class TestPredefinedTileGrid:
    def test_basic_from_list(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)]
        grid = PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2

    def test_from_geodataframe(self):
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)],
            crs="EPSG:4326",
        )
        grid = PredefinedTileGrid(tiles=gdf)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2
        assert result.crs.to_epsg() == 4326

    def test_from_geoseries(self):
        gs = gpd.GeoSeries(
            [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)],
            crs="EPSG:4326",
        )
        grid = PredefinedTileGrid(tiles=gs)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2

    def test_multipolygon_tiles(self):
        """Tiles may be MultiPolygons."""
        mp = shapely.geometry.MultiPolygon([shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(2, 2, 3, 3)])
        grid = PredefinedTileGrid(tiles=[mp], crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 0.8, 0.8))
        assert len(result) == 1

    def test_preserves_extra_columns(self):
        """Extra columns on the input GeoDataFrame are preserved in the output."""
        gdf = gpd.GeoDataFrame(
            {"name": ["A", "B"], "geometry": [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(5, 5, 6, 6)]},
            crs="EPSG:4326",
        )
        grid = PredefinedTileGrid(tiles=gdf)
        result = grid.get_tiles(shapely.geometry.box(0, 0, 2, 2))
        assert len(result) == 1
        assert result["name"].iloc[0] == "A"

    def test_filters_non_intersecting(self):
        tiles = [
            shapely.geometry.box(0, 0, 1, 1),
            shapely.geometry.box(1, 0, 2, 1),
            shapely.geometry.box(5, 5, 6, 6),
        ]
        grid = PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2

    def test_dict_geometry(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(2, 2, 3, 3)]
        grid = PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles({"west": 0.5, "south": 0.5, "east": 0.8, "north": 0.8})
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0, 0, 1, 1))

    def test_empty_list_raises(self):
        with pytest.raises(JobSplittingFailure, match="At least one tile"):
            PredefinedTileGrid(tiles=[], crs=4326)

    def test_empty_geodataframe_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="at least one row"):
            PredefinedTileGrid(tiles=gdf)

    def test_invalid_tile_type_raises(self):
        with pytest.raises(JobSplittingFailure, match="All tiles must be"):
            PredefinedTileGrid(tiles=["not_a_polygon"], crs=4326)

    def test_list_without_crs_raises(self):
        with pytest.raises(JobSplittingFailure, match="'crs' is required"):
            PredefinedTileGrid(tiles=[shapely.geometry.box(0, 0, 1, 1)])

    def test_geodataframe_without_crs_and_no_crs_arg_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        with pytest.raises(JobSplittingFailure, match="no CRS set"):
            PredefinedTileGrid(tiles=gdf)

    def test_geodataframe_without_crs_uses_crs_arg(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        grid = PredefinedTileGrid(tiles=gdf, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0, 0, 1, 1))
        assert result.crs.to_epsg() == 4326

    def test_invalid_geometry_type_raises(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1)]
        grid = PredefinedTileGrid(tiles=tiles, crs=4326)
        with pytest.raises(JobSplittingFailure, match="Expected a bounding-box dict"):
            grid.get_tiles("invalid")


class TestSplitArea:
    def test_returns_geodataframe(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        result = split_area(aoi, projection="EPSG:4326", tile_size=1.0)
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.crs.to_epsg() == 4326

    def test_with_tile_size_and_projection(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        result = split_area(aoi, projection="EPSG:4326", tile_size=1.0)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 1.0, 1.0))

    def test_projection_inferred_from_aoi(self):
        """When projection is omitted, it is inferred from the dict's crs field."""
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        result = split_area(aoi, tile_size=1.0)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 1.0, 1.0))

    def test_no_tile_size_raises(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0}
        with pytest.raises(JobSplittingFailure, match="tile_size"):
            split_area(aoi)

    def test_no_projection_no_crs_raises(self):
        aoi = shapely.geometry.box(0, 0, 1, 1)
        with pytest.raises(JobSplittingFailure, match="projection"):
            split_area(aoi, tile_size=1.0)

    def test_tile_grid_mutually_exclusive(self):
        grid = PredefinedTileGrid(tiles=[shapely.geometry.box(0, 0, 1, 1)], crs=4326)
        with pytest.raises(JobSplittingFailure, match="Cannot combine"):
            split_area(shapely.geometry.box(0, 0, 1, 1), projection="EPSG:4326", tile_grid=grid)

    def test_with_predefined_tile_grid(self):
        grid_tiles = [shapely.geometry.box(0, 0, 0.5, 0.5), shapely.geometry.box(0.5, 0, 1, 0.5)]
        grid = PredefinedTileGrid(tiles=grid_tiles, crs=4326)
        aoi = shapely.geometry.box(0, 0, 1, 0.5)
        result = split_area(aoi, tile_grid=grid)
        assert len(result) == 2

    def test_with_geodataframe_as_tile_grid(self):
        """A GeoDataFrame can be passed directly as tile_grid without wrapping in PredefinedTileGrid."""
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 0.5, 0.5), shapely.geometry.box(0.5, 0, 1, 0.5)],
            crs="EPSG:4326",
        )
        aoi = shapely.geometry.box(0, 0, 1, 0.5)
        result = split_area(aoi, tile_grid=gdf)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2
        assert result.crs.to_epsg() == 4326

    def test_split_area_3857(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 20_000.0, "north": 20_000.0, "crs": "EPSG:3857"}
        result = split_area(aoi, projection="EPSG:3857", tile_size=20_000.0)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0))
        assert result.crs.to_epsg() == 3857
