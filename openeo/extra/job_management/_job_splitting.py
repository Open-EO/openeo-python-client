import abc
import logging
import math
from typing import Dict, List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import shapely.geometry.base
from shapely.geometry import MultiPolygon, Polygon

from openeo.util import BBoxDict, normalize_crs

_log = logging.getLogger(__name__)


class JobSplittingFailure(Exception):
    pass


class _TileGridInterface(metaclass=abc.ABCMeta):
    """
    Interface for tile grid classes that split a geometry into tiles.

    Implementations must define :meth:`get_tiles` and the :attr:`crs` property.

    Shared helpers :meth:`_parse_input_geometry` and
    :meth:`_reproject_to_grid_crs` handle input normalisation and CRS
    reprojection so that subclasses don't need to duplicate that logic.
    """

    @property
    @abc.abstractmethod
    def crs(self) -> int:
        """EPSG code of the tile grid's coordinate reference system."""
        ...

    @abc.abstractmethod
    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        """
        Calculate tiles to cover the given geometry.

        :param geometry: area of interest as a bounding-box dict
            (with keys ``west``, ``south``, ``east``, ``north``, and optionally ``crs``),
            a :class:`~shapely.geometry.Polygon`, or a :class:`~shapely.geometry.MultiPolygon`.
        :return: GeoDataFrame with one row per tile and CRS set.
        """
        ...

    @staticmethod
    def _parse_input_geometry(
        geometry: Union[Dict, Polygon, MultiPolygon],
    ) -> Tuple[Union[Polygon, MultiPolygon], Optional[int]]:
        """
        Normalise the user-supplied *geometry* into a shapely geometry and an
        optional EPSG code.

        :return: ``(shapely_geom, source_epsg)`` where *source_epsg* is
            ``None`` when the input carries no CRS information (bare Polygon).
        :raises JobSplittingFailure: on unsupported input types.
        """
        if isinstance(geometry, dict):
            bbox = BBoxDict.from_dict(geometry)
            raw_crs = bbox.get("crs")
            source_epsg = normalize_crs(raw_crs) if raw_crs is not None else None

            if bbox["south"] >= bbox["north"]:
                raise JobSplittingFailure(
                    "Invalid bounding box: south must be less than north."
                )

            if bbox["west"] == bbox["east"]:
                raise JobSplittingFailure(
                    "Invalid bounding box: west must not equal east (zero-width bounding box)."
                )

            return bbox.as_geometry(), source_epsg
        elif isinstance(geometry, (Polygon, MultiPolygon)):
            return geometry, None
        else:
            raise JobSplittingFailure(
                f"Expected a bounding-box dict, Polygon, or MultiPolygon, got {type(geometry).__name__}."
            )

    def _reproject_to_grid_crs(
        self,
        geom: Union[Polygon, MultiPolygon],
        source_epsg: Optional[int],
    ) -> Union[Polygon, MultiPolygon]:
        """
        Reproject *geom* from *source_epsg* to the tile grid's :attr:`crs`.

        - If *source_epsg* is ``None`` the geometry is returned unchanged
          (assumed to already be in the grid's CRS).
        - If *source_epsg* equals the grid's CRS, no work is done.
        - Otherwise a geopandas reprojection is performed.

        After reprojection the geometry is checked for antimeridian crossing.
        """
        grid_epsg = self.crs
        if source_epsg is None:
            _log.warning(
                "Input geometry has no CRS information; assuming it is already in the tile grid's CRS (EPSG:%d).",
                grid_epsg,
            )
        elif source_epsg != grid_epsg:
            _log.info("Reprojecting input geometry from EPSG:%d to EPSG:%d.", source_epsg, grid_epsg)
            src = gpd.GeoDataFrame(geometry=[geom], crs=f"EPSG:{source_epsg}")
            geom = src.to_crs(f"EPSG:{grid_epsg}").geometry[0]

        self._check_antimeridian_crossing(geom.bounds, grid_epsg)
        return geom

    @staticmethod
    def _check_antimeridian_crossing(bounds: Tuple[float, float, float, float], epsg: int) -> None:
        """
        Raise :exc:`JobSplittingFailure` when *bounds* suggest that a geometry
        crosses or extends beyond the antimeridian in a geographic CRS.

        The check only applies to geographic coordinate reference systems
        (e.g. EPSG:4326).  For projected CRSs the longitude range is
        meaningless, so the check is silently skipped.

        :param bounds: ``(min_x, min_y, max_x, max_y)`` of the geometry.
        :param epsg: EPSG code of the CRS the geometry is expressed in.
        :raises JobSplittingFailure: if longitude falls outside [-180, 180].
        """
        try:
            import pyproj

            crs_obj = pyproj.CRS.from_epsg(epsg)
            if not crs_obj.is_geographic:
                return
        except Exception:
            return

        min_lon, _, max_lon, _ = bounds
        if min_lon < -180 or max_lon > 180:
            raise JobSplittingFailure(
                "Geometry appears to cross or extend beyond the antimeridian "
                "(longitude coordinates outside the [-180, 180] range). "
                "Antimeridian-crossing geometries are not supported."
            )


class _SizeBasedTileGrid(_TileGridInterface):
    """
    Tile grid that splits a geometry into regular tiles of a given size.

    Tiles are anchored at the AOI's own lower-left corner and never extend
    beyond the AOI boundary.  Edge tiles may therefore be smaller than
    *size* x *size*.

    The *size* is interpreted in the unit of the projection (meters for most
    projected CRSs, degrees for EPSG:4326).

    :param epsg: EPSG code of the projection to use for tiling.
    :param size: maximum tile edge length in the unit of the projection.
    """

    def __init__(self, *, epsg: int, size: float):
        try:
            epsg = normalize_crs(epsg)
        except (ValueError, TypeError) as e:
            raise JobSplittingFailure(f"Failed to normalize EPSG code for tile grid splitting: {epsg!r}.") from e
        if not isinstance(epsg, int):
            raise JobSplittingFailure(f"Only integer EPSG codes are supported for tile grid splitting, got {epsg!r}.")
        self._epsg = epsg
        if size <= 0:
            raise JobSplittingFailure(f"Tile size must be positive, got {size!r}.")
        self.size = size

    @property
    def crs(self) -> int:
        return self._epsg

    @classmethod
    def from_size_projection(cls, *, size: float, projection: str) -> "_SizeBasedTileGrid":
        """Create a tile grid from size and projection"""
        # TODO: the constructor also does normalize_crs, so this factory looks like overkill at the moment
        return cls(epsg=normalize_crs(projection), size=size)

    @staticmethod
    def _split_bounding_box(to_cover: BBoxDict, tile_size: float) -> List[Polygon]:
        """
        Subdivide a bounding box into tiles of at most *tile_size*.

        Tiles are anchored at the AOI's lower-left corner (``west``, ``south``)
        and clipped to the AOI boundary, so edge tiles can be smaller than
        *tile_size*.

        :param to_cover: bounding box to subdivide.
        :param tile_size: maximum tile edge length.
        :return: list of tile polygons.
        """
        west, south = to_cover["west"], to_cover["south"]
        east, north = to_cover["east"], to_cover["north"]

        n_cols = math.ceil(round((east - west) / tile_size, 10))
        n_rows = math.ceil(round((north - south) / tile_size, 10))

        if n_cols > 500 or n_rows > 500:
            _log.warning(
                "Attempting to split AOI into %d columns and %d rows of tiles. "
                "This may take a while and consume a lot of memory. Consider increasing the tile size.",
                n_cols,
                n_rows,
            )

        tiles = []
        for col in range(n_cols):
            for row in range(n_rows):
                tiles.append(
                    BBoxDict(
                        west=west + col * tile_size,
                        south=south + row * tile_size,
                        east=min(west + (col + 1) * tile_size, east),
                        north=min(south + (row + 1) * tile_size, north),
                    ).as_geometry()
                )
        return tiles

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        geom, source_epsg = self._parse_input_geometry(geometry)
        geom = self._reproject_to_grid_crs(geom, source_epsg)

        # Process each constituent polygon independently so that a
        # MultiPolygon (e.g. from an antimeridian-crossing bbox) does not
        # produce tiles for the full combined bounding box.
        parts = list(geom.geoms) if isinstance(geom, MultiPolygon) else [geom]
        all_polygons: List[Polygon] = []
        for part in parts:
            bbox = BBoxDict.from_any(part, crs=self._epsg)
            all_polygons.extend(self._split_bounding_box(to_cover=bbox, tile_size=self.size))

        gdf = gpd.GeoDataFrame(geometry=all_polygons, crs=f"EPSG:{self._epsg}")

        # Drop tiles that don't actually intersect the original geometry.
        # This matters for concave or complex shapes whose bounding box is
        # significantly larger than the shape itself.
        mask = gdf.intersects(geom)
        return gdf.loc[mask].reset_index(drop=True)


class _PredefinedTileGrid(_TileGridInterface):
    """
    Tile grid based on a user-supplied collection of geometries.

    Only geometries that intersect the given area of interest are returned by
    :meth:`get_tiles`.

    Geometries can be any shapely geometry type (Polygon, MultiPolygon, …).

    :param tiles: pre-defined geometries as a :class:`~geopandas.GeoDataFrame`,
        a :class:`~geopandas.GeoSeries`, or a plain list of shapely geometries.
    :param crs: EPSG code of the geometry coordinate system.
        Required when *tiles* is a plain list (geometry objects carry no CRS).
        Ignored when *tiles* is a GeoDataFrame/GeoSeries that already has a CRS.
    """

    def __init__(
        self,
        *,
        tiles: Union[gpd.GeoDataFrame, gpd.GeoSeries, List[shapely.geometry.base.BaseGeometry]],
        crs: Optional[int] = None,
    ):
        if isinstance(tiles, gpd.GeoDataFrame):
            self._gdf = tiles.copy()
        elif isinstance(tiles, gpd.GeoSeries):
            self._gdf = gpd.GeoDataFrame(geometry=tiles)
        elif isinstance(tiles, list):
            if not tiles:
                raise JobSplittingFailure("At least one tile geometry must be provided.")
            if not all(isinstance(t, shapely.geometry.base.BaseGeometry) for t in tiles):
                raise JobSplittingFailure("All tiles must be shapely geometry instances.")
            if crs is None:
                raise JobSplittingFailure("'crs' is required when tiles are provided as a plain list of geometries.")
            self._gdf = gpd.GeoDataFrame(geometry=tiles, crs=f"EPSG:{normalize_crs(crs)}")
        else:
            raise JobSplittingFailure(
                f"Expected a GeoDataFrame, GeoSeries, or list of geometries, got {type(tiles).__name__}."
            )

        if self._gdf.empty:
            raise JobSplittingFailure("The tile GeoDataFrame must contain at least one row.")

        if self._gdf.crs is None:
            if crs is None:
                raise JobSplittingFailure(
                    "The tile GeoDataFrame has no CRS set. " "Either set the CRS on the GeoDataFrame or pass 'crs'."
                )
            self._gdf = self._gdf.set_crs(f"EPSG:{normalize_crs(crs)}")

        self._check_antimeridian_crossing(self._gdf.total_bounds, self.crs)

    @property
    def crs(self) -> int:
        return self._gdf.crs.to_epsg()

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        geom, source_epsg = self._parse_input_geometry(geometry)
        geom = self._reproject_to_grid_crs(geom, source_epsg)

        mask = self._gdf.intersects(geom)
        return self._gdf.loc[mask].copy().reset_index(drop=True)


def split_area(
    spatial_extent: Union[Dict, MultiPolygon, Polygon],
    *,
    projection: Optional[str] = None,
    tile_size: Optional[float] = None,
    tile_grid: Optional[Union[_TileGridInterface, gpd.GeoDataFrame]] = None,
) -> gpd.GeoDataFrame:
    """
    Split an area of interest into tiles.

    There are two ways to define how the area is tiled:

    1. **By tile size and projection** — pass *projection* and *tile_size*.
       A :class:`_SizeBasedTileGrid` is created under the hood.

    2. **By pre-defined grid** — pass a :class:`_TileGridInterface` instance
       (e.g. :class:`_PredefinedTileGrid`) or a :class:`~geopandas.GeoDataFrame`
       as *tile_grid*.

    :param spatial_extent: area of interest as a bounding-box dict
        (keys ``west``, ``south``, ``east``, ``north``, optionally ``crs``),
        a :class:`~shapely.geometry.Polygon`, or
        a :class:`~shapely.geometry.MultiPolygon`.
    :param projection: EPSG string (e.g. ``"EPSG:3857"``) for the tile grid.
        Required when *tile_grid* is not supplied.
    :param tile_size: tile edge length in the unit of the projection.
        Required when *tile_grid* is not supplied.
    :param tile_grid: a :class:`_TileGridInterface` instance or a
        :class:`~geopandas.GeoDataFrame` (with CRS set) that defines the tiling
        strategy.  Mutually exclusive with *projection* / *tile_size*.
    :return: :class:`~geopandas.GeoDataFrame` with one row per tile and CRS equal to the tile grid CRS.
    :raises JobSplittingFailure: on invalid or contradictory arguments.
    """
    if tile_grid is not None:
        if projection is not None or tile_size is not None:
            raise JobSplittingFailure(
                "Cannot combine 'tile_grid' with 'projection' or 'tile_size'. "
                "Either pass a _TileGridInterface, or pass projection + tile_size."
            )
        if isinstance(tile_grid, gpd.GeoDataFrame):
            tile_grid = _PredefinedTileGrid(tiles=tile_grid)
        return tile_grid.get_tiles(spatial_extent)

    # --- Size-based splitting path ---
    if tile_size is None:
        raise JobSplittingFailure("Either provide a 'tile_grid', or provide both 'tile_size' and 'projection'.")

    if projection is None:
        raise JobSplittingFailure("'projection' is required when using size-based tiling.")

    grid = _SizeBasedTileGrid(epsg=normalize_crs(projection), size=tile_size)
    return grid.get_tiles(spatial_extent)


def _predefined_tile_keys(tile_grid: "_PredefinedTileGrid", points: np.ndarray) -> np.ndarray:
    """For each point (in the grid CRS), the position of the first tile containing it (-1: none)."""
    # TODO: polygons are assigned by their representative point only, so they may extend beyond their tile.
    #   Assigning them to e.g. the tile with the largest overlap could be a future addition.
    n_tiles = len(tile_grid._gdf)
    point_idx, tile_idx = shapely.STRtree(tile_grid._gdf.geometry.to_numpy()).query(points, predicate="intersects")
    # Lowest tile position wins for points on shared tile edges.
    keys = np.full(len(points), n_tiles)
    np.minimum.at(keys, point_idx, tile_idx)
    keys[keys == n_tiles] = -1
    return keys


def _bisect_group(
    positions: np.ndarray,
    *,
    xs: np.ndarray,
    ys: np.ndarray,
    max_geometries: Optional[int],
) -> List[np.ndarray]:
    """
    Recursively bisect a group of rows (given as positions) at the median of their locations,
    along the longer axis of the locations' spread, until groups have at most *max_geometries* rows.
    """
    result = []
    stack = [positions]
    while stack:
        pos = stack.pop()
        if max_geometries is None or len(pos) <= max_geometries:
            result.append(pos)
            continue
        px, py = xs[pos], ys[pos]
        axis = px if np.ptp(px) >= np.ptp(py) else py
        pos = pos[np.argsort(axis, kind="stable")]
        cut = len(pos) // 2
        stack.append(pos[cut:])
        stack.append(pos[:cut])
    return result


def group_geometries(
    geometries: gpd.GeoDataFrame,
    *,
    max_geometries: Optional[int] = None,
    projection: Optional[str] = None,
    tile_size: Optional[float] = None,
    tile_grid: Optional[Union[_PredefinedTileGrid, _SizeBasedTileGrid, gpd.GeoDataFrame]] = None,
) -> List[gpd.GeoDataFrame]:
    """
    Group geometries (e.g. sample locations for point-based extraction)
    into spatially clustered groups of at most *max_geometries* each.

    Each row is treated as a single "feature" to group, regardless of its
    geometry type (e.g. ``Point``, ``MultiPoint``, ``Polygon``):
    *max_geometries* limits the number of rows (features) per group,
    not the number of individual coordinates. To limit individual
    coordinates instead, explode multi-part geometries into separate rows
    first, e.g. with ``geometries.explode(index_parts=False)``.

    Features are located by their ``representative_point()`` (computed in the input CRS),
    they are not split themselves. For a single large area of interest that has to be split,
    use :func:`split_area` instead.

    Grouping is done by recursive bisection (k-d tree style): a group is cut at the
    median of the feature locations along the longer axis of their spread,
    until it has at most *max_geometries* features.
    This keeps groups spatially clustered and balanced.

    If *tile_grid* is given (e.g. a Sentinel-2 MGRS/UTM tile grid), each feature is first
    assigned to the tile containing its representative point, and every tile is bisected separately.
    Features whose representative point is not in any tile are grouped (and bisected) separately.
    Alternatively, *tile_size* and *projection* can be given to group
    features in a regular grid of square tiles anchored at the origin of the projection
    (so grouping does not change when other features are added).

    Current limitations:

    - Features are never split: a polygon crossing tile edges, or larger than a tile,
      is assigned to a single tile and extends beyond it. Use :func:`split_area` for large areas.
    - Groups are limited by feature count only, not by area or processing cost.
      Use a tile grid to limit the spatial extent of groups.

    :param geometries: GeoDataFrame with a ``geometry`` column.
        Null or empty geometries are not allowed.
    :param max_geometries: maximum number of geometries per group.
        ``None`` (default) means no limit.
    :param projection: EPSG string (e.g. ``"EPSG:3857"``) of the regular grid
        to group by. Must be combined with *tile_size*.
    :param tile_size: tile edge length in the unit of *projection*.
        Must be combined with *projection*.
    :param tile_grid: optional :class:`_PredefinedTileGrid` or
        :class:`_SizeBasedTileGrid` instance, or GeoDataFrame of tile polygons
        (with CRS set) to co-join geometries by before splitting, e.g. a
        satellite tiling grid.
        Mutually exclusive with *projection* / *tile_size*.
    :return: list of GeoDataFrames, with the original index of *geometries* preserved.
    :raises JobSplittingFailure: on invalid input (e.g. non-positive
        *max_geometries*, null/empty geometries, contradictory grid arguments).

    Like with :func:`split_area`, geometries without CRS information
    are assumed to be in the tile grid's CRS.
    Without tile grid, no CRS is needed and the returned groups have no CRS either.
    """
    if not isinstance(geometries, gpd.GeoDataFrame):
        raise JobSplittingFailure(f"Expected a GeoDataFrame, got {type(geometries).__name__}.")
    if "geometry" not in geometries.columns:
        raise JobSplittingFailure("The GeoDataFrame must contain a 'geometry' column.")
    if max_geometries is not None and max_geometries <= 0:
        raise JobSplittingFailure(f"'max_geometries' must be positive, got {max_geometries!r}.")

    if tile_grid is not None:
        if projection is not None or tile_size is not None:
            raise JobSplittingFailure(
                "Cannot combine 'tile_grid' with 'projection' or 'tile_size'. "
                "Either pass a tile_grid, or pass projection + tile_size."
            )
        if isinstance(tile_grid, gpd.GeoDataFrame):
            tile_grid = _PredefinedTileGrid(tiles=tile_grid)
        elif not isinstance(tile_grid, (_PredefinedTileGrid, _SizeBasedTileGrid)):
            raise JobSplittingFailure(
                "Unsupported tile_grid type: "
                f"{type(tile_grid).__name__}. Expected _PredefinedTileGrid, _SizeBasedTileGrid, or GeoDataFrame."
            )
    elif projection is not None or tile_size is not None:
        if projection is None or tile_size is None:
            raise JobSplittingFailure("'projection' and 'tile_size' must be provided together.")
        tile_grid = _SizeBasedTileGrid.from_size_projection(size=tile_size, projection=projection)

    n_invalid = int((geometries.geometry.isna() | geometries.geometry.is_empty).sum())
    if n_invalid:
        raise JobSplittingFailure(f"Found {n_invalid} null or empty geometries, which cannot be grouped.")

    if geometries.crs is None and tile_grid is not None:
        _log.warning(
            "Geometries have no CRS information; assuming they are already in the tile grid's CRS (EPSG:%d).",
            tile_grid.crs,
        )
        geometries = geometries.set_crs(f"EPSG:{tile_grid.crs}")
    if geometries.crs is not None and geometries.crs.to_epsg() is not None:
        _TileGridInterface._check_antimeridian_crossing(geometries.total_bounds, geometries.crs.to_epsg())

    if geometries.empty:
        return []

    # TODO: limiting groups by area or processing cost (e.g. a weight per feature) is not supported yet.
    # Work with positions (not index labels), so duplicated index labels are no problem.
    locations = geometries.geometry.representative_point()
    if tile_grid is not None:
        locations = locations.to_crs(epsg=tile_grid.crs)
    xs, ys = locations.x.to_numpy(), locations.y.to_numpy()

    all_positions = np.arange(len(geometries))
    if tile_grid is None:
        position_groups = [all_positions]
    else:
        if isinstance(tile_grid, _SizeBasedTileGrid):
            # Regular grid anchored at the origin of the CRS.
            keys = pd.DataFrame({"x": np.floor(xs / tile_grid.size), "y": np.floor(ys / tile_grid.size)})
        elif isinstance(tile_grid, _PredefinedTileGrid):
            keys = pd.DataFrame({"tile": _predefined_tile_keys(tile_grid, locations.to_numpy())})
        else:
            raise JobSplittingFailure(f"Unsupported tile grid type: {type(tile_grid).__name__}.")
        position_groups = list(keys.groupby(list(keys.columns), sort=True).indices.values())

    result = []
    for positions in position_groups:
        for group in _bisect_group(positions, xs=xs, ys=ys, max_geometries=max_geometries):
            result.append(geometries.iloc[np.sort(group)])
    return result
