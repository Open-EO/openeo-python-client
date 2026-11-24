import warnings

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from geopandas.testing import assert_geodataframe_equal

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


def _points(coords, crs="EPSG:4326") -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[shapely.geometry.Point(x, y) for x, y in coords], crs=crs)


def _index_groups(groups) -> list:
    """Index labels per group, in canonical order (the order of the groups is not part of the contract)."""
    return sorted(sorted(g.index) for g in groups)


class TestGroupGeometries:
    # --- Grouping without tile grid ---

    def test_all_geometries_in_one_group_without_limit_or_grid(self):
        # Given
        gdf = _points([(0, 0), (100, 0), (0, 50)])
        # When
        actual = group_geometries(gdf)
        # Then
        assert _index_groups(actual) == [[0, 1, 2]]

    def test_one_group_when_count_equals_max_geometries(self):
        # Given
        gdf = _points([(0, 0), (1, 0), (2, 0)])
        # When
        actual = group_geometries(gdf, max_geometries=3)
        # Then
        assert _index_groups(actual) == [[0, 1, 2]]

    def test_groups_are_halved_until_within_max_geometries(self):
        # Given: 8 points on a line
        gdf = _points([(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0)])
        # When
        actual = group_geometries(gdf, max_geometries=3)
        # Then: 8 -> 4 + 4 -> 2 + 2 + 2 + 2 (balanced, not 3 + 3 + 2)
        assert _index_groups(actual) == [[0, 1], [2, 3], [4, 5], [6, 7]]

    def test_groups_follow_location_not_row_order(self):
        # Given: two clusters, interleaved in row order
        gdf = _points([(100, 0), (0, 0), (101, 0), (1, 0)])
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then
        assert _index_groups(actual) == [[0, 2], [1, 3]]

    def test_groups_are_cut_along_the_longest_spread(self):
        # Given: points spread 30 units in y, but only 1 unit in x
        gdf = _points([(0, 0), (1, 10), (0, 20), (1, 30)])
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then: cut between y=10 and y=20 (a cut in x would give [[0, 2], [1, 3]])
        assert _index_groups(actual) == [[0, 1], [2, 3]]

    def test_coincident_points_still_respect_max_geometries(self):
        # Given: all points at the same location
        gdf = _points([(0, 0), (0, 0), (0, 0), (0, 0), (0, 0)])
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then: every row once, in groups of at most 2 (which rows go together is unspecified)
        assert sorted(pd.concat(actual).index) == [0, 1, 2, 3, 4]
        assert sorted(len(g) for g in actual) == [1, 2, 2]

    def test_polygons_are_grouped_by_location(self):
        # Given: two clusters of polygons, interleaved in row order
        gdf = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(x, 0, x + 1, 1) for x in (0, 100, 1, 101)],
            crs="EPSG:4326",
        )
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then
        assert _index_groups(actual) == [[0, 2], [1, 3]]

    def test_multipoint_row_counts_as_one_geometry(self):
        # Given: a MultiPoint with 3 coordinates, and a Point
        gdf = gpd.GeoDataFrame(
            geometry=[
                shapely.geometry.MultiPoint([(0, 0), (0.1, 0.1), (0.2, 0.2)]),
                shapely.geometry.Point(0.5, 0.5),
            ],
            crs="EPSG:4326",
        )
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then: 2 rows fit in one group, regardless of the 4 coordinates
        assert _index_groups(actual) == [[0, 1]]

    def test_groups_keep_original_rows_index_and_columns(self):
        # Given
        gdf = gpd.GeoDataFrame(
            {"id": [10, 11, 12, 13]},
            geometry=[shapely.geometry.Point(x, 0) for x in (0, 100, 1, 101)],
            index=["a", "b", "c", "d"],
            crs="EPSG:4326",
        )
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then: every input row ends up unchanged in exactly one group
        assert _index_groups(actual) == [["a", "c"], ["b", "d"]]
        assert_geodataframe_equal(pd.concat(actual).sort_index(), gdf)

    def test_duplicated_index_labels_keep_each_row_once(self):
        # Given: all rows share the same index label (e.g. after `explode(index_parts=False)`)
        gdf = _points([(0, 0), (1, 0), (100, 0), (101, 0)])
        gdf.index = [7, 7, 7, 7]
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then
        assert _index_groups(actual) == [[7, 7], [7, 7]]
        assert sorted(sorted(g.geometry.x) for g in actual) == [[0, 1], [100, 101]]

    def test_empty_geodataframe_gives_no_groups(self):
        # Given
        gdf = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        # When
        actual = group_geometries(gdf, max_geometries=2)
        # Then
        assert actual == []

    def test_geometries_without_crs_are_grouped_without_crs(self):
        # Given
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0), shapely.geometry.Point(1, 0)])
        # When
        actual = group_geometries(gdf)
        # Then
        assert _index_groups(actual) == [[0, 1]]
        assert actual[0].crs is None

    # --- Grouping with a predefined tile grid ---

    def test_tile_grid_groups_per_tile(self):
        # Given
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)], crs="EPSG:4326"
        )
        gdf = _points([(0.5, 0.5), (1.5, 0.5), (0.2, 0.5), (1.8, 0.5)])
        # When
        actual = group_geometries(gdf, tile_grid=tile_grid)
        # Then
        assert _index_groups(actual) == [[0, 2], [1, 3]]

    def test_tile_grid_point_on_shared_edge_goes_to_first_tile(self):
        # Given: tiles A [0, 1] and B [1, 2], and a point exactly on their shared edge x=1
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)], crs="EPSG:4326"
        )
        gdf = _points([(1.0, 0.5), (0.5, 0.5), (1.5, 0.5)])
        # When
        actual = group_geometries(gdf, tile_grid=tile_grid)
        # Then: the edge point goes with tile A
        assert _index_groups(actual) == [[0, 1], [2]]

    def test_tile_grid_point_in_overlap_goes_to_first_tile_in_grid_order(self):
        # Given: overlapping tiles A [1, 3] (first in grid) and B [0, 2], and a point in the overlap
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(1, 0, 3, 1), shapely.geometry.box(0, 0, 2, 1)], crs="EPSG:4326"
        )
        gdf = _points([(1.5, 0.5), (0.5, 0.5), (2.5, 0.5)])
        # When
        actual = group_geometries(gdf, tile_grid=tile_grid)
        # Then: the overlap point goes with tile A
        assert _index_groups(actual) == [[0, 2], [1]]

    def test_tile_grid_tile_exceeding_max_geometries_is_split(self):
        # Given: 4 points in a single tile
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 10, 10)], crs="EPSG:4326")
        gdf = _points([(1, 1), (2, 1), (3, 1), (4, 1)])
        # When
        actual = group_geometries(gdf, max_geometries=2, tile_grid=tile_grid)
        # Then
        assert _index_groups(actual) == [[0, 1], [2, 3]]

    def test_tile_grid_geometries_outside_all_tiles_are_grouped_separately(self):
        # Given: 3 points inside the only tile, 2 points just outside it
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)], crs="EPSG:4326")
        gdf = _points([(0.2, 0.5), (0.8, 0.5), (0.9, 0.5), (1.5, 0.5), (1.6, 0.5)])
        # When
        actual = group_geometries(gdf, max_geometries=3, tile_grid=tile_grid)
        # Then: without the grid, this would be [[0, 1], [2, 3, 4]]
        assert _index_groups(actual) == [[0, 1, 2], [3, 4]]

    def test_tile_grid_polygon_assigned_by_representative_point(self):
        # Given: a polygon overlapping tiles A and B, with its representative point (10.5, 3) in tile B
        tile_grid = gpd.GeoDataFrame(
            geometry=[shapely.geometry.box(0, 0, 10, 10), shapely.geometry.box(10, 0, 20, 10)], crs="EPSG:3857"
        )
        gdf = gpd.GeoDataFrame(
            geometry=[
                shapely.geometry.Point(5, 5),
                shapely.geometry.Point(15, 5),
                shapely.geometry.box(8, 2, 13, 4),
            ],
            crs="EPSG:3857",
        )
        # When
        actual = group_geometries(gdf, tile_grid=tile_grid)
        # Then
        assert _index_groups(actual) == [[0], [1, 2]]

    def test_tile_grid_in_other_crs_groups_reprojected_but_returns_input_crs(self):
        # Given: tiles west/east of the prime meridian in EPSG:3857, points in EPSG:4326
        tile_grid = gpd.GeoDataFrame(
            geometry=[
                shapely.geometry.box(-200_000, -100_000, 0, 100_000),
                shapely.geometry.box(0, -100_000, 200_000, 100_000),
            ],
            crs="EPSG:3857",
        )
        gdf = _points([(-0.5, 0), (0.5, 0), (-1, 0)])
        # When
        actual = group_geometries(gdf, tile_grid=tile_grid)
        # Then
        assert _index_groups(actual) == [[0, 2], [1]]
        assert [g.crs.to_epsg() for g in actual] == [4326, 4326]

    def test_tile_grid_instances_are_supported(self):
        # Given
        gdf = _points([(0.5, 0.5), (1.5, 0.5)])
        predefined = _PredefinedTileGrid(
            tiles=[shapely.geometry.box(0, 0, 1, 1), shapely.geometry.box(1, 0, 2, 1)], crs=4326
        )
        size_based = _SizeBasedTileGrid(epsg=4326, size=1.0)
        # When / Then
        assert _index_groups(group_geometries(gdf, tile_grid=predefined)) == [[0], [1]]
        assert _index_groups(group_geometries(gdf, tile_grid=size_based)) == [[0], [1]]

    # --- Grouping with a regular grid (tile_size + projection) ---

    def test_tile_size_grid_is_anchored_at_crs_origin(self):
        # Given: points at x=3, 6, 8
        gdf = _points([(3, 0), (6, 0), (8, 0)], crs="EPSG:3857")
        # When
        actual = group_geometries(gdf, tile_size=5, projection="EPSG:3857")
        # Then: tiles [0, 5) and [5, 10), not anchored at the first point ([3, 8) would give [[0, 1], [2]])
        assert _index_groups(actual) == [[0], [1, 2]]

    def test_tile_size_grouping_unchanged_when_outlier_added(self):
        # Given
        base = [(1, 1), (3, 1), (6, 1), (8, 1), (11, 1)]
        # When
        actual_base = group_geometries(_points(base, crs="EPSG:3857"), tile_size=5, projection="EPSG:3857")
        actual_extended = group_geometries(
            _points(base + [(-1000, 1)], crs="EPSG:3857"), tile_size=5, projection="EPSG:3857"
        )
        # Then
        assert _index_groups(actual_base) == [[0, 1], [2, 3], [4]]
        assert _index_groups(actual_extended) == [[0, 1], [2, 3], [4], [5]]

    def test_tile_size_is_in_units_of_projection(self):
        # Given: points 0.1 degree (~11 km) apart at the equator
        gdf = _points([(0.0, 0.0), (0.1, 0.0)])
        # When
        actual_50km = group_geometries(gdf, tile_size=50_000, projection="EPSG:3857")
        actual_5km = group_geometries(gdf, tile_size=5_000, projection="EPSG:3857")
        # Then
        assert _index_groups(actual_50km) == [[0, 1]]
        assert _index_groups(actual_5km) == [[0], [1]]

    def test_tile_size_concave_polygon_located_by_representative_point(self):
        # Given: a U shape whose centroid (4.4, 3.7) lies in the notch, in another tile than the U itself
        u_shape = shapely.geometry.box(0, 0, 10, 10).difference(shapely.geometry.box(2, 2, 9, 10))
        gdf = gpd.GeoDataFrame(
            geometry=[u_shape, shapely.geometry.Point(1, 6), shapely.geometry.Point(7, 0.5)], crs="EPSG:3857"
        )
        # When
        actual = group_geometries(gdf, tile_size=5, projection="EPSG:3857")
        # Then
        assert _index_groups(actual) == [[0, 1], [2]]

    def test_geometries_without_crs_are_assumed_in_grid_crs(self, caplog):
        # Given
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(x, 0) for x in (0, 100_000, 200_000)])
        # When
        actual = group_geometries(gdf, tile_size=150_000, projection="EPSG:3857")
        # Then
        assert _index_groups(actual) == [[0, 1], [2]]
        assert [g.crs.to_epsg() for g in actual] == [3857, 3857]
        assert "assuming they are already in the tile grid's CRS (EPSG:3857)" in caplog.text

    # --- Invalid input ---

    @pytest.mark.parametrize(
        "geometries",
        [
            [shapely.geometry.Point(0, 0), shapely.geometry.Point(1, 0)],
            shapely.geometry.MultiPoint([(0, 0), (1, 0)]),
        ],
    )
    def test_non_geodataframe_raises(self, geometries):
        with pytest.raises(JobSplittingFailure, match="Expected a GeoDataFrame"):
            group_geometries(geometries)

    def test_missing_geometry_column_raises(self):
        gdf = gpd.GeoDataFrame({"foo": [1, 2]})
        with pytest.raises(JobSplittingFailure, match="must contain a 'geometry' column"):
            group_geometries(gdf)

    @pytest.mark.parametrize("max_geometries", [0, -1, 2.5, 2.0, "2", True])
    def test_max_geometries_not_a_positive_integer_raises(self, max_geometries):
        gdf = _points([(0, 0)])
        with pytest.raises(JobSplittingFailure, match="'max_geometries' must be a positive integer"):
            group_geometries(gdf, max_geometries=max_geometries)

    @pytest.mark.parametrize(
        "geometry",
        [None, shapely.geometry.Point()],
    )
    def test_null_or_empty_geometry_raises(self, geometry):
        gdf = gpd.GeoDataFrame(geometry=[shapely.geometry.Point(0, 0), geometry], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="Found 1 null or empty geometries"):
            group_geometries(gdf)

    def test_antimeridian_crossing_geometries_raise(self):
        gdf = _points([(190.0, 0.0)])
        with pytest.raises(JobSplittingFailure, match="antimeridian"):
            group_geometries(gdf)

    @pytest.mark.parametrize("kwargs", [{"tile_size": 1.0}, {"projection": "EPSG:4326"}])
    def test_tile_size_without_projection_or_vice_versa_raises(self, kwargs):
        with pytest.raises(JobSplittingFailure, match="must be provided together"):
            group_geometries(_points([(0, 0)]), **kwargs)

    def test_tile_grid_combined_with_tile_size_raises(self):
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)], crs="EPSG:4326")
        with pytest.raises(JobSplittingFailure, match="Cannot combine 'tile_grid'"):
            group_geometries(_points([(0, 0)]), tile_grid=tile_grid, tile_size=1.0, projection="EPSG:4326")

    def test_tile_grid_without_crs_raises(self):
        tile_grid = gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)])
        with pytest.raises(JobSplittingFailure, match="has no CRS set"):
            group_geometries(_points([(0, 0)]), tile_grid=tile_grid)

    def test_custom_tile_grid_implementation_raises(self):
        class CustomTileGrid(_TileGridInterface):
            @property
            def crs(self) -> int:
                return 4326

            def get_tiles(self, geometry):
                return gpd.GeoDataFrame(geometry=[shapely.geometry.box(0, 0, 1, 1)], crs="EPSG:4326")

        with pytest.raises(JobSplittingFailure, match="Unsupported tile_grid type: CustomTileGrid"):
            group_geometries(_points([(0.5, 0.5)]), tile_grid=CustomTileGrid())
