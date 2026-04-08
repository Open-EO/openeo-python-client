import geopandas as gpd
import pytest
import shapely

from openeo.extra.job_management._job_splitting import (
    JobSplittingFailure,
    _PredefinedTileGrid,
    _SizeBasedTileGrid,
    _TileGridInterface,
    split_area,
)
from openeo.util import BBoxDict


class TestSizeBasedTileGrid:
    def test_from_size_projection(self):
        splitter = _SizeBasedTileGrid.from_size_projection(size=0.1, projection="EPSG:4326")
        assert splitter.crs == 4326
        assert splitter.size == 0.1

    def test_constructor_rejects_unparseable_epsg(self):
        """An invalid CRS string should be caught and raised as JobSplittingFailure."""
        with pytest.raises(JobSplittingFailure, match="Failed to normalize EPSG code"):
            _SizeBasedTileGrid(epsg="not_a_crs", size=1.0)

    def test_constructor_rejects_unknown_epsg(self):
        """An EPSG code unknown to pyproj is caught at construction time."""
        with pytest.raises(JobSplittingFailure, match="Failed to normalize EPSG code"):
            _SizeBasedTileGrid(epsg=999999, size=1.0)

    @pytest.mark.parametrize("size", [0, -1, -0.5])
    def test_constructor_rejects_non_positive_size(self, size):
        """Tile size must be strictly positive."""
        with pytest.raises(JobSplittingFailure, match="Tile size must be positive"):
            _SizeBasedTileGrid(epsg=4326, size=size)
    def test_get_tiles_raises_exception(self):
        """get_tiles rejects input that is not a dict, Polygon, or MultiPolygon."""
        tile_grid = _SizeBasedTileGrid(epsg=4326, size=0.1)
        with pytest.raises(JobSplittingFailure, match="Expected a bounding-box dict"):
            tile_grid.get_tiles("invalid_geometry")

    def test_get_tiles_returns_geodataframe(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        tile_grid = _SizeBasedTileGrid(epsg=4326, size=1.0)
        result = tile_grid.get_tiles(aoi)
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.crs is not None
        assert result.crs.to_epsg() == 4326

    def test_simple_get_tiles_dict(self):
        """tile grid size equals input geometry size -> single tile returned."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = _SizeBasedTileGrid(epsg=3857, size=100_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0))

    def test_multiple_get_tile_dict(self):
        """tile grid smaller than input -> multiple tiles."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = _SizeBasedTileGrid(epsg=3857, size=20_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 25
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 20_000.0, 20_000.0))

    def test_larger_get_tile_dict(self):
        """tile grid larger than input -> single tile clipped to input."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        tile_grid = _SizeBasedTileGrid(epsg=3857, size=200_000)
        result = tile_grid.get_tiles(aoi)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0))

    def test_get_tiles_polygon_wgs(self):
        """polygon in WGS84 with WGS84 tile grid."""
        polygon = shapely.geometry.box(0.0, 0.0, 1.0, 1.0)
        tile_grid = _SizeBasedTileGrid(epsg=4326, size=0.1)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 100
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 0.1, 0.1))

    def test_simple_get_tiles_polygon(self):
        """tile grid size equals polygon size -> single tile."""
        polygon = shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0)
        tile_grid = _SizeBasedTileGrid(epsg=3857, size=100_000.0)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 1
        assert result.geometry[0].equals(polygon)

    def test_larger_get_tiles_polygon(self):
        """tile grid larger than polygon -> single tile clipped to polygon."""
        polygon = shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0)
        tile_grid = _SizeBasedTileGrid(epsg=3857, size=200_000.0)
        result = tile_grid.get_tiles(polygon)
        assert len(result) == 1
        assert result.geometry[0].equals(polygon)

    def test_edge_tiles_clipped_to_aoi(self):
        """When the AOI is not a multiple of tile_size, edge tiles are smaller."""
        aoi = {"west": 0.0, "south": 0.0, "east": 150_000.0, "north": 150_000.0, "crs": "EPSG:3857"}
        grid = _SizeBasedTileGrid(epsg=3857, size=100_000)
        result = grid.get_tiles(aoi)
        assert len(result) == 4  # 2x2 grid
        # The right/top edge tiles should be 50k wide/tall, not 100k
        bounds = [g.bounds for g in result.geometry]
        east_edges = {b[2] for b in bounds}
        north_edges = {b[3] for b in bounds}
        assert 150_000.0 in east_edges
        assert 150_000.0 in north_edges

    def test_reprojects_bbox_when_crs_differs(self):
        """AOI in EPSG:4326 should be reprojected to the tile grid's CRS (EPSG:3857) before splitting."""
        # A small bbox around lon=0, lat=0 in WGS84
        aoi = {"west": -1.0, "south": -1.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        grid = _SizeBasedTileGrid(epsg=3857, size=500_000)
        result = grid.get_tiles(aoi)
        assert result.crs.to_epsg() == 3857
        # AOI-relative tiling: ~222 km bbox fits in one 500 km tile
        assert len(result) == 1
        # Tile coordinates should be in meters (3857), not degrees
        bounds = result.geometry[0].bounds
        assert abs(bounds[0]) > 100  # west in meters, not ~-1 degree
        assert abs(bounds[2]) > 100  # east in meters, not ~1 degree

    def test_no_reprojection_when_crs_matches(self):
        """When AOI CRS matches the tile grid CRS, no reprojection should happen."""
        aoi = {"west": 0.0, "south": 0.0, "east": 100_000.0, "north": 100_000.0, "crs": "EPSG:3857"}
        grid = _SizeBasedTileGrid(epsg=3857, size=100_000)
        result = grid.get_tiles(aoi)
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0.0, 0.0, 100_000.0, 100_000.0))


class TestPredefinedTileGrid:
    def test_basic_from_list(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)]
        grid = _PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 2

    def test_from_geodataframe(self):
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)],
            crs="EPSG:4326",
        )
        grid = _PredefinedTileGrid(tiles=gdf)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2
        assert result.crs.to_epsg() == 4326

    def test_from_geoseries(self):
        gs = gpd.GeoSeries(
            [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)],
            crs="EPSG:4326",
        )
        grid = _PredefinedTileGrid(tiles=gs)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2

    def test_multipolygon_tiles(self):
        """Tiles may be MultiPolygons."""
        mp = shapely.geometry.MultiPolygon([shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(2, 2, 3, 3)])
        grid = _PredefinedTileGrid(tiles=[mp], crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 0.8, 0.8))
        assert len(result) == 1

    def test_preserves_extra_columns(self):
        """Extra columns on the input GeoDataFrame are preserved in the output."""
        gdf = gpd.GeoDataFrame(
            {"name": ["A", "B"], "geometry": [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(5, 5, 6, 6)]},
            crs="EPSG:4326",
        )
        grid = _PredefinedTileGrid(tiles=gdf)
        result = grid.get_tiles(shapely.geometry.box(0, 0, 2, 2))
        assert len(result) == 1
        assert result["name"].iloc[0] == "A"

    def test_filters_non_intersecting(self):
        tiles = [
            shapely.geometry.box(0, 0, 1, 1),
            shapely.geometry.box(1, 0, 2, 1),
            shapely.geometry.box(5, 5, 6, 6),
        ]
        grid = _PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0.5, 0.5, 1.5, 0.75))
        assert len(result) == 2

    def test_reprojects_query_geometry_when_crs_differs(self):
        """AOI dict in EPSG:4326 with tile grid in EPSG:3857 should reproject before intersection."""
        # Tile covering roughly lon [-1, 1], lat [-1, 1] in 3857 meters
        tile_3857 = shapely.geometry.box(-111_320, -111_325, 111_320, 111_325)
        grid = _PredefinedTileGrid(tiles=[tile_3857], crs=3857)
        # Query in WGS84 degrees — overlaps the tile after reprojection
        result = grid.get_tiles({"west": -0.5, "south": -0.5, "east": 0.5, "north": 0.5, "crs": "EPSG:4326"})
        assert len(result) == 1

    def test_no_match_after_reprojection(self):
        """AOI that doesn't overlap tiles after reprojection should return empty."""
        tile_3857 = shapely.geometry.box(-111_320, -111_325, 111_320, 111_325)
        grid = _PredefinedTileGrid(tiles=[tile_3857], crs=3857)
        # Query far away from the tile
        result = grid.get_tiles({"west": 50, "south": 50, "east": 51, "north": 51, "crs": "EPSG:4326"})
        assert len(result) == 0

    def test_dict_geometry(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(2, 2, 3, 3)]
        grid = _PredefinedTileGrid(tiles=tiles, crs=4326)
        result = grid.get_tiles({"west": 0.5, "south": 0.5, "east": 0.8, "north": 0.8})
        assert len(result) == 1
        assert result.geometry[0].equals(shapely.geometry.box(0, 0, 1, 1))

    def test_empty_list_raises(self):
        with pytest.raises(JobSplittingFailure, match="At least one tile"):
            _PredefinedTileGrid(tiles=[], crs=4326)

    def test_empty_geodataframe_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="at least one row"):
            _PredefinedTileGrid(tiles=gdf)

    def test_invalid_tile_type_raises(self):
        with pytest.raises(JobSplittingFailure, match="All tiles must be"):
            _PredefinedTileGrid(tiles=["not_a_polygon"], crs=4326)

    def test_list_without_crs_raises(self):
        with pytest.raises(JobSplittingFailure, match="'crs' is required"):
            _PredefinedTileGrid(tiles=[shapely.geometry.box(0, 0, 1, 1)])

    def test_geodataframe_without_crs_and_no_crs_arg_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        with pytest.raises(JobSplittingFailure, match="no CRS set"):
            _PredefinedTileGrid(tiles=gdf)

    def test_geodataframe_without_crs_uses_crs_arg(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        grid = _PredefinedTileGrid(tiles=gdf, crs=4326)
        result = grid.get_tiles(shapely.geometry.box(0, 0, 1, 1))
        assert result.crs.to_epsg() == 4326

    def test_invalid_geometry_type_raises(self):
        tiles = [shapely.geometry.box(0, 0, 1, 1)]
        grid = _PredefinedTileGrid(tiles=tiles, crs=4326)
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

    def test_projection_required_for_size_based(self):
        """projection is mandatory for size-based tiling, even when the AOI has a crs field."""
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0, "crs": "EPSG:4326"}
        with pytest.raises(JobSplittingFailure, match="projection.*required"):
            split_area(aoi, tile_size=1.0)

    def test_no_tile_size_raises(self):
        aoi = {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0}
        with pytest.raises(JobSplittingFailure, match="tile_size"):
            split_area(aoi)

    def test_no_projection_no_crs_raises(self):
        aoi = shapely.geometry.box(0, 0, 1, 1)
        with pytest.raises(JobSplittingFailure, match="projection"):
            split_area(aoi, tile_size=1.0)

    def test_tile_grid_mutually_exclusive(self):
        grid = _PredefinedTileGrid(tiles=[shapely.geometry.box(0, 0, 1, 1)], crs=4326)
        with pytest.raises(JobSplittingFailure, match="Cannot combine"):
            split_area(shapely.geometry.box(0, 0, 1, 1), projection="EPSG:4326", tile_grid=grid)

    def test_with_predefined_tile_grid(self):
        grid_tiles = [shapely.geometry.box(0, 0, 0.5, 0.5), shapely.geometry.box(0.5, 0, 1, 0.5)]
        grid = _PredefinedTileGrid(tiles=grid_tiles, crs=4326)
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

    def test_invalid_aoi_type_raises(self):
        """split_area rejects AOI types that are not dict, Polygon, or MultiPolygon."""
        with pytest.raises(JobSplittingFailure, match="Expected a bounding-box dict"):
            split_area("not_a_geometry", projection="EPSG:4326", tile_size=1.0)

    def test_antimeridian_crossing_bbox_raises(self):
        """A bounding box where west >= east (antimeridian crossing) is rejected."""
        aoi = {"west": 170.0, "south": -10.0, "east": -170.0, "north": 10.0, "crs": "EPSG:4326"}
        with pytest.raises(JobSplittingFailure, match="Antimeridian-crossing bounding boxes are not supported"):
            split_area(aoi, projection="EPSG:4326", tile_size=1.0)
