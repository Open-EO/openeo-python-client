import abc
import logging
import math
from typing import Dict, List, Optional, Tuple, Union

import geopandas as gpd
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
            if bbox["west"] >= bbox["east"] or bbox["south"] >= bbox["north"]:
                raise JobSplittingFailure(
                    "Invalid bounding box: west must be less than east and south must be less than north. "
                    "Antimeridian-crossing bounding boxes are not supported."
                )
            raw_crs = bbox.get("crs")
            source_epsg = normalize_crs(raw_crs) if raw_crs is not None else None
            return bbox.as_polygon(), source_epsg
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
        """
        grid_epsg = self.crs
        if source_epsg is None:
            _log.warning(
                "Input geometry has no CRS information; assuming it is already in the tile grid's CRS (EPSG:%d).",
                grid_epsg,
            )
            return geom

        if source_epsg == grid_epsg:
            return geom

        _log.info("Reprojecting input geometry from EPSG:%d to EPSG:%d.", source_epsg, grid_epsg)
        src = gpd.GeoDataFrame(geometry=[geom], crs=f"EPSG:{source_epsg}")
        return src.to_crs(f"EPSG:{grid_epsg}").geometry[0]


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

        tiles = []
        for col in range(n_cols):
            for row in range(n_rows):
                tiles.append(
                    BBoxDict(
                        west=west + col * tile_size,
                        south=south + row * tile_size,
                        east=min(west + (col + 1) * tile_size, east),
                        north=min(south + (row + 1) * tile_size, north),
                    ).as_polygon()
                )
        return tiles

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        geom, source_epsg = self._parse_input_geometry(geometry)
        geom = self._reproject_to_grid_crs(geom, source_epsg)

        bbox = BBoxDict.from_any(geom, crs=self._epsg)
        polygons = self._split_bounding_box(to_cover=bbox, tile_size=self.size)
        gdf = gpd.GeoDataFrame(geometry=polygons, crs=f"EPSG:{self._epsg}")

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

    @property
    def crs(self) -> int:
        return self._gdf.crs.to_epsg()

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        geom, source_epsg = self._parse_input_geometry(geometry)
        geom = self._reproject_to_grid_crs(geom, source_epsg)

        mask = self._gdf.intersects(geom)
        return self._gdf.loc[mask].copy().reset_index(drop=True)


def split_area(
    aoi: Union[Dict, MultiPolygon, Polygon],
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

    :param aoi: area of interest as a bounding-box dict
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
    :return: :class:`~geopandas.GeoDataFrame` with one row per tile and CRS set.
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
        return tile_grid.get_tiles(aoi)

    # --- Size-based splitting path ---
    if tile_size is None:
        raise JobSplittingFailure("Either provide a 'tile_grid', or provide both 'tile_size' and 'projection'.")

    if projection is None:
        raise JobSplittingFailure("'projection' is required when using size-based tiling.")

    grid = _SizeBasedTileGrid(epsg=normalize_crs(projection), size=tile_size)
    return grid.get_tiles(aoi)
