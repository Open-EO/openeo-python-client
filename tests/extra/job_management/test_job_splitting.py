import warnings

import geopandas as gpd
import pandas as pd
import pytest
import shapely

from openeo.extra.job_management._job_splitting import (
    JobSplittingFailure,
    _PredefinedTileGrid,
    _SizeBasedTileGrid,
    _TileGridInterface,
    group_geometries,
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

    def test_concave_polygon_skips_non_intersecting_tiles(self):
        """Tiles from the bounding box that don't intersect the actual geometry are dropped."""
        # L-shaped polygon: bottom strip spans full width up to y=0.9, left column extends to y=2.
        # The horizontal edge at y=0.9 avoids the y=1 tile boundary.
        l_shape = shapely.geometry.Polygon([(0, 0), (2, 0), (2, 0.9), (0.5, 0.9), (0.5, 2), (0, 2), (0, 0)])
        grid = _SizeBasedTileGrid(epsg=4326, size=1.0)
        result = grid.get_tiles(l_shape)
        # Bounding box is 2x2 -> 4 candidate tiles, but upper-right (1,1)->(2,2) doesn't intersect
        assert len(result) == 3

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

    def test_antimeridian_crossing_tiles_raises(self):
        """Predefined tiles with coordinates beyond ±180° longitude in a geographic CRS are rejected at construction."""
        tiles = [shapely.geometry.box(170, -10, 190, 10)]
        with pytest.raises(JobSplittingFailure, match="antimeridian"):
            _PredefinedTileGrid(tiles=tiles, crs=4326)

    def test_antimeridian_not_checked_for_projected_tiles(self):
        """Predefined tiles in a projected CRS with large coordinates should not trigger the antimeridian check."""
        tiles = [shapely.geometry.box(18_000_000, 0, 19_000_000, 1_000_000)]
        grid = _PredefinedTileGrid(tiles=tiles, crs=3857)
        assert grid.crs == 3857

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

    def test_split_large_area(self):
        aoi = {"west": 3.36, "south": 50.75, "east": 7.23, "north": 53.56, "crs": 4326}
        result = split_area(aoi, projection="EPSG:3857", tile_size=20_000.0)
        assert len(result) == 572
        assert result.crs.to_epsg() == 3857

    def test_invalid_aoi_type_raises(self):
        """split_area rejects AOI types that are not dict, Polygon, or MultiPolygon."""
        with pytest.raises(JobSplittingFailure, match="Expected a bounding-box dict"):
            split_area("not_a_geometry", projection="EPSG:4326", tile_size=1.0)

    def test_antimeridian_crossing_bbox_is_supported(self):
        """A bounding box where west > east in a geographic CRS is split at the antimeridian."""
        aoi = {"west": 170.0, "south": -10.0, "east": -170.0, "north": 10.0, "crs": "EPSG:4326"}
        result = split_area(aoi, projection="EPSG:4326", tile_size=10.0)
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.crs.to_epsg() == 4326
        # 10° x 20° on the west side + 10° x 20° on the east side → 2 + 2 = 4 tiles
        assert len(result) == 4
        # All tile coordinates must be within [-180, 180]
        for geom in result.geometry:
            min_x, _, max_x, _ = geom.bounds
            assert min_x >= -180.0
            assert max_x <= 180.0

    def test_antimeridian_crossing_bbox_small_tiles(self):
        """Antimeridian-crossing bbox with smaller tiles produces correct count."""
        # 5° west of antimeridian + 5° east → 10° total, 1° tiles, 20° latitude
        aoi = {"west": 175.0, "south": -10.0, "east": -175.0, "north": 10.0, "crs": "EPSG:4326"}
        result = split_area(aoi, projection="EPSG:4326", tile_size=1.0)
        # 5 cols × 20 rows on each side = 100 + 100 = 200
        assert len(result) == 200

    def test_antimeridian_crossing_predefined_grid(self):
        """Predefined tile grid works correctly with an antimeridian-crossing query bbox."""
        tiles = [
            shapely.geometry.box(170, -10, 180, 10),
            shapely.geometry.box(-180, -10, -170, 10),
            shapely.geometry.box(0, 0, 10, 10),  # should NOT match
        ]
        grid = _PredefinedTileGrid(tiles=tiles, crs=4326)
        aoi = {"west": 175.0, "south": -5.0, "east": -175.0, "north": 5.0, "crs": "EPSG:4326"}
        result = split_area(aoi, tile_grid=grid)
        assert len(result) == 2

    def test_antimeridian_crossing_polygon_raises(self):
        """A polygon with coordinates beyond ±180° longitude in a geographic CRS is rejected."""
        # Polygon wrapping past 180° (e.g. 170° to 190° instead of -170°)
        polygon = shapely.geometry.box(170.0, -10.0, 190.0, 10.0)
        with pytest.raises(JobSplittingFailure, match="antimeridian"):
            split_area(polygon, projection="EPSG:4326", tile_size=1.0)

    def test_antimeridian_not_checked_for_projected_crs(self):
        """Large coordinates in a projected CRS (e.g. EPSG:3857) should not trigger the antimeridian check."""
        # EPSG:3857 legitimately has huge coordinate values
        aoi = {"west": 18_000_000.0, "south": 0.0, "east": 19_000_000.0, "north": 1_000_000.0, "crs": "EPSG:3857"}
        result = split_area(aoi, projection="EPSG:3857", tile_size=1_000_000.0)
        assert len(result) >= 1


class TestGroupGeometries:
    def _grid_points(self, n_side: int) -> gpd.GeoDataFrame:
        points = [shapely.geometry.Point(x, y) for x in range(n_side) for y in range(n_side)]
        return gpd.GeoDataFrame(geometry=points, crs="EPSG:4326")

    def test_single_group_when_under_limit(self):
        gdf = self._grid_points(5)  # 25 points
        result = group_geometries(gdf, max_geometries=100)
        assert len(result) == 1
        assert len(result[0]) == 25

    def test_splits_into_groups_of_max_geometries(self):
        gdf = self._grid_points(10)  # 100 points
        result = group_geometries(gdf, max_geometries=30)
        assert sum(len(g) for g in result) == 100
        assert all(len(g) <= 30 for g in result)
        assert len(result) > 1

    def test_groups_are_spatially_clustered(self):
        """Points on the left half and right half should end up in different groups."""
        left = [shapely.geometry.Point(x, 0) for x in range(10)]
        right = [shapely.geometry.Point(x + 100, 0) for x in range(10)]
        gdf = gpd.GeoDataFrame(geometry=left + right, crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=10)
        assert len(result) == 2
        for group in result:
            xs = [geom.x for geom in group.geometry]
            assert max(xs) - min(xs) < 50

    def test_default_max_geometries_is_unlimited(self):
        gdf = self._grid_points(30)  # 900 points
        result = group_geometries(gdf)
        assert len(result) == 1
        assert len(result[0]) == 900

    def test_default_max_geometries_groups_per_tile(self):
        gdf = self._grid_points(30)
        result = group_geometries(gdf, tile_size=15, projection="EPSG:4326")
        assert sorted(len(g) for g in result) == [225, 225, 225, 225]

    def test_empty_geodataframe(self):
        gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        assert group_geometries(gdf) == []

    def test_coincident_points_are_split_by_row_order(self):
        """Points that all share the same location are still split in groups of at most max_geometries."""
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0)] * 25, crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=10)
        assert sum(len(g) for g in result) == 25
        assert all(len(g) <= 10 for g in result)

    def test_missing_geometry_column_raises(self):
        gdf = gpd.GeoDataFrame({"foo": [1, 2]})
        with pytest.raises(JobSplittingFailure, match="geometry"):
            group_geometries(gdf)

    def test_missing_crs_without_grid(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0)])
        result = group_geometries(gdf)
        assert len(result) == 1
        assert result[0].crs is None

    def test_non_positive_max_geometries_raises(self):
        gdf = self._grid_points(2)
        with pytest.raises(JobSplittingFailure, match="max_geometries"):
            group_geometries(gdf, max_geometries=0)

    def test_preserves_extra_columns(self):
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2, 3], "geometry": [shapely.geometry.Point(x, 0) for x in range(3)]},
            crs="EPSG:4326",
        )
        result = group_geometries(gdf, max_geometries=10)
        assert len(result) == 1
        assert list(result[0]["id"]) == [1, 2, 3]

    def test_multipoint_rows_treated_as_single_feature(self):
        """A row with a MultiPoint geometry counts as one feature towards max_geometries, not one per coordinate."""
        gdf = gpd.GeoDataFrame(
            geometry=[
                shapely.geometry.MultiPoint([(0, 0), (0.1, 0.1), (0.2, 0.2)]),  # 3 coordinates, 1 feature
                shapely.geometry.Point(0.5, 0.5),
            ],
            crs="EPSG:4326",
        )
        result = group_geometries(gdf, max_geometries=10)
        assert len(result) == 1
        assert len(result[0]) == 2  # 2 rows/features, regardless of coordinate count

    def test_tile_grid_points_never_span_multiple_tiles(self):
        tile_grid = gpd.GeoDataFrame(
            {"tile": ["A", "B"]},
            geometry=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)],
            crs="EPSG:4326",
        )
        points = [shapely.geometry.Point(0.1 * i, 0.5) for i in range(5)]  # tile A
        # Start at x=1.1 to avoid the x=1.0 shared-edge ambiguity.
        points += [shapely.geometry.Point(1.1 + 0.1 * i, 0.5) for i in range(5)]  # tile B
        gdf = gpd.GeoDataFrame(geometry=points, crs="EPSG:4326")

        result = group_geometries(gdf, max_geometries=100, tile_grid=tile_grid)
        assert len(result) == 2
        for group in result:
            xs = [geom.x for geom in group.geometry]
            assert (max(xs) < 1) or (min(xs) >= 1)

    def test_tile_grid_oversized_tile_is_further_split(self):
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 10, 10)], crs="EPSG:4326")
        points = [shapely.geometry.Point(x, y) for x in range(10) for y in range(10)]  # 100 points, 1 tile
        gdf = gpd.GeoDataFrame(geometry=points, crs="EPSG:4326")

        result = group_geometries(gdf, max_geometries=30, tile_grid=tile_grid)
        assert sum(len(g) for g in result) == 100
        assert all(len(g) <= 30 for g in result)
        assert len(result) > 1

    def test_tile_grid_unmatched_points_grouped_separately(self):
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)], crs="EPSG:4326")
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.Point(0.5, 0.5), shapely.geometry.Point(50, 50)],
            crs="EPSG:4326",
        )
        result = group_geometries(gdf, max_geometries=10, tile_grid=tile_grid)
        assert len(result) == 2
        assert sum(len(g) for g in result) == 2

    def test_tile_grid_reprojects_when_crs_differs(self):
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(-111_320, -111_325, 111_320, 111_325)], crs="EPSG:3857"
        )
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0.1, 0.1)], crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=10, tile_grid=tile_grid)
        assert len(result) == 1
        assert len(result[0]) == 1

    def test_tile_grid_missing_crs_raises(self):
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        gdf = self._grid_points(2)
        with pytest.raises(JobSplittingFailure, match="CRS"):
            group_geometries(gdf, tile_grid=tile_grid)

    @pytest.mark.parametrize(
        "geometries",
        [
            [shapely.geometry.Point(x, 0) for x in range(5)],
            shapely.geometry.MultiPoint([(x, 0) for x in range(5)]),
        ],
    )
    def test_non_geodataframe_raises(self, geometries):
        with pytest.raises(JobSplittingFailure, match="Expected a GeoDataFrame"):
            group_geometries(geometries, max_geometries=10)

    def test_points_without_crs_assume_grid_crs(self, caplog):
        points = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(100_000 * x, 0) for x in range(3)])
        result = group_geometries(points, max_geometries=10, tile_size=150_000, projection="EPSG:3857")
        assert [len(g) for g in result] == [2, 1]
        assert all(g.crs.to_epsg() == 3857 for g in result)
        assert "assuming they are already in the tile grid's CRS (EPSG:3857)" in caplog.text

    def test_tile_grid_interface_instance(self):
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.Point(0.5, 0.5), shapely.geometry.Point(1.5, 0.5)], crs="EPSG:4326"
        )
        tile_grid = _PredefinedTileGrid(
            tiles=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)], crs=4326
        )
        assert len(group_geometries(gdf, max_geometries=10, tile_grid=tile_grid)) == 2
        tile_grid = _SizeBasedTileGrid(epsg=4326, size=1.0)
        assert len(group_geometries(gdf, max_geometries=10, tile_grid=tile_grid)) == 2

    def test_null_geometry_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0), None], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="1 null or empty"):
            group_geometries(gdf)

    def test_empty_geometry_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0), shapely.geometry.Point()], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="1 null or empty"):
            group_geometries(gdf)

    def test_duplicated_index_keeps_all_rows_once(self):
        gdf = self._grid_points(4)
        gdf.index = [0, 0, 1, 1, 2, 2, 3, 3] * 2
        result = group_geometries(gdf, max_geometries=3)
        assert sum(len(g) for g in result) == 16
        coords = sorted((g.x, g.y) for group in result for g in group.geometry)
        assert coords == sorted((g.x, g.y) for g in gdf.geometry)
        assert sorted(i for group in result for i in group.index) == sorted(gdf.index)

    def test_original_index_preserved(self):
        gdf = self._grid_points(3)
        gdf.index = list("abcdefghi")
        result = group_geometries(gdf, max_geometries=4)
        assert sorted(i for group in result for i in group.index) == list("abcdefghi")
        for group in result:
            assert (gdf.loc[group.index].geometry.values == group.geometry.values).all()

    def test_polygons_in_geographic_crs_emit_no_warning(self):
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(x, 0, x + 1, 1) for x in range(10)],
            crs="EPSG:4326",
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            result = group_geometries(gdf, max_geometries=4)
        assert sum(len(g) for g in result) == 10

    def test_concave_polygon_located_by_representative_point(self):
        # U shape: its centroid (4.4, 3.7) lies in the notch, outside the polygon.
        u_shape = shapely.geometry.box(0, 0, 10, 10).difference(shapely.geometry.box(2, 2, 9, 10))
        assert not u_shape.contains(u_shape.centroid)
        gdf = gpd.GeoDataFrame(
            geometry=[u_shape, shapely.geometry.Point(1, 6), shapely.geometry.Point(7, 0.5)], crs="EPSG:3857"
        )
        result = group_geometries(gdf, tile_size=5, projection="EPSG:3857")
        assert sorted(sorted(g.index) for g in result) == [[0, 1], [2]]

    def test_collinear_points_are_split_balanced(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(3, y) for y in range(100)], crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=30)
        assert [len(g) for g in result] == [25, 25, 25, 25]

    def test_balanced_bisection(self):
        gdf = self._grid_points(10)
        result = group_geometries(gdf, max_geometries=30)
        assert sorted(len(g) for g in result) == [25, 25, 25, 25]

    def test_weight_area_respects_max_weight(self):
        big = [shapely.geometry.box(2000 * i, 0, 2000 * i + 1000, 1000) for i in range(3)]  # 1e6 m² each
        small = [shapely.geometry.box(10_000 + 100 * i, 0, 10_010 + 100 * i, 10) for i in range(3)]  # 100 m² each
        gdf = gpd.GeoDataFrame(geometry=big + small, crs="EPSG:6933")
        result = group_geometries(gdf, weight="area", max_weight=2.5e6)
        assert len(result) > 1
        for group in result:
            assert (group.geometry.area.clip(lower=100)).sum() <= 2.5e6
        assert sum(len(g) for g in result) == 6

    def test_weight_area_floor_applies_to_points(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(i, 0) for i in range(10)], crs="EPSG:6933")
        result = group_geometries(gdf, weight="area", max_weight=250)
        assert all(len(g) <= 2 for g in result)

    def test_weight_area_without_crs_raises(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        with pytest.raises(JobSplittingFailure, match="CRS"):
            group_geometries(gdf, weight="area", max_weight=10)

    def test_weight_column(self):
        gdf = self._grid_points(3)
        gdf["w"] = [1, 2, 3, 1, 2, 3, 1, 2, 3]
        result = group_geometries(gdf, weight="w", max_weight=6)
        assert sum(len(g) for g in result) == 9
        assert all(g["w"].sum() <= 6 for g in result)
        assert len(result) > 1

    def test_weight_series(self):
        gdf = self._grid_points(3)
        weights = pd.Series([1, 2, 3, 1, 2, 3, 1, 2, 3], index=gdf.index)
        result = group_geometries(gdf, weight=weights, max_weight=6)
        assert sum(len(g) for g in result) == 9
        assert all(weights.loc[g.index].sum() <= 6 for g in result)

    def test_unknown_weight_column_raises(self):
        with pytest.raises(JobSplittingFailure, match="not found"):
            group_geometries(self._grid_points(2), weight="nope", max_weight=1)

    def test_oversize_feature_gets_own_group_with_warning(self, caplog):
        gdf = self._grid_points(2)
        gdf["w"] = [10, 1, 1, 1]
        result = group_geometries(gdf, weight="w", max_weight=5)
        heavy = [g for g in result if 0 in g.index]
        assert len(heavy) == 1 and len(heavy[0]) == 1
        assert "exceeding 'max_weight'" in caplog.text

    def test_max_weight_without_weight_raises(self):
        with pytest.raises(JobSplittingFailure, match="requires 'weight'"):
            group_geometries(self._grid_points(2), max_weight=5)

    @pytest.mark.parametrize("max_weight", [0, -1])
    def test_non_positive_max_weight_raises(self, max_weight):
        gdf = self._grid_points(2)
        gdf["w"] = 1
        with pytest.raises(JobSplittingFailure, match="max_weight"):
            group_geometries(gdf, weight="w", max_weight=max_weight)

    @pytest.mark.parametrize("bad", [-1.0, float("nan")])
    def test_invalid_weight_values_raise(self, bad):
        gdf = self._grid_points(2)
        gdf["w"] = [1.0, 1.0, 1.0, bad]
        with pytest.raises(JobSplittingFailure, match="finite and non-negative"):
            group_geometries(gdf, weight="w", max_weight=5)

    def test_max_extent(self):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(x, 0) for x in range(100)], crs="EPSG:3857")
        result = group_geometries(gdf, max_extent=30)
        assert len(result) > 1
        for group in result:
            minx, _, maxx, _ = group.total_bounds
            assert maxx - minx <= 30

    def test_max_extent_uses_geometry_bounds(self):
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 10, 1), shapely.geometry.box(0, 2, 10, 3)], crs="EPSG:3857"
        )
        assert len(group_geometries(gdf, max_extent=10)) == 1
        assert len(group_geometries(gdf, max_extent=5)) == 2  # single features exceeding the limit stay alone

    @pytest.mark.parametrize("max_extent", [0, -5])
    def test_non_positive_max_extent_raises(self, max_extent):
        with pytest.raises(JobSplittingFailure, match="max_extent"):
            group_geometries(self._grid_points(2), max_extent=max_extent)

    def test_size_based_grid_is_globally_anchored(self):
        def grouping(gdf):
            result = group_geometries(gdf, tile_size=5, projection="EPSG:3857")
            return {frozenset(g.index) for g in result}

        base = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(x, 1) for x in (1, 3, 6, 8, 11)], crs="EPSG:3857")
        extended = gpd.GeoDataFrame(
            geometry=list(base.geometry) + [shapely.geometry.Point(-1000, 1)], crs="EPSG:3857"
        )
        assert grouping(extended) - {frozenset({5})} == grouping(base)

    def test_predefined_grid_polygon_assigned_to_largest_overlap_tile(self):
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 10, 10), shapely.geometry.box(10, 0, 20, 10)], crs="EPSG:3857"
        )
        gdf = gpd.GeoDataFrame(
            geometry=[
                shapely.geometry.Point(5, 5),
                shapely.geometry.Point(15, 5),
                shapely.geometry.box(8, 2, 13, 4),  # 2 wide in tile A, 3 wide in tile B
            ],
            crs="EPSG:3857",
        )
        result = group_geometries(gdf, tile_grid=tile_grid)
        assert sorted(sorted(g.index) for g in result) == [[0], [1, 2]]

    def test_polygons_grouped_by_centroid(self):
        polygons = [shapely.geometry.box(x, 0, x + 1, 1) for x in (0, 1, 100, 101)]
        gdf = gpd.GeoDataFrame(geometry=polygons, crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=2)
        assert len(result) == 2
        for group in result:
            xs = [geom.centroid.x for geom in group.geometry]
            assert max(xs) - min(xs) < 50

    def test_tile_size_projection_groups_by_regular_grid(self):
        points = [shapely.geometry.Point(0.1 * i, 0.5) for i in range(5)]
        points += [shapely.geometry.Point(1.1 + 0.1 * i, 0.5) for i in range(5)]
        gdf = gpd.GeoDataFrame(geometry=points, crs="EPSG:4326")
        result = group_geometries(gdf, max_geometries=100, tile_size=1.0, projection="EPSG:4326")
        assert len(result) == 2
        for group in result:
            xs = [geom.x for geom in group.geometry]
            assert (max(xs) < 1) or (min(xs) >= 1)

    def test_tile_size_projection_reprojects(self):
        # ~0.1 degree apart near the equator is ~11 km in EPSG:3857: 50 km tiles keep them together
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.Point(0.0, 0.0), shapely.geometry.Point(0.1, 0.0)], crs="EPSG:4326"
        )
        result = group_geometries(gdf, max_geometries=10, tile_size=50_000, projection="EPSG:3857")
        assert len(result) == 1
        result = group_geometries(gdf, max_geometries=10, tile_size=5_000, projection="EPSG:3857")
        assert len(result) == 2

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"tile_size": 1.0},
            {"projection": "EPSG:4326"},
            {"tile_size": 1.0, "projection": "EPSG:4326", "tile_grid": "grid"},
        ],
    )
    def test_invalid_grid_argument_combinations_raise(self, kwargs):
        if kwargs.get("tile_grid") == "grid":
            kwargs["tile_grid"] = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure):
            group_geometries(self._grid_points(2), **kwargs)

    def test_antimeridian_crossing_points_raises(self):
        """Geographic points with longitude outside [-180, 180] are rejected to avoid antimeridian issues."""
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(190.0, 0.0)], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="antimeridian"):
            group_geometries(gdf, max_geometries=10)
