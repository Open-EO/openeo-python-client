import abc
import math
from typing import Dict, List, Optional, Union

import geopandas as gpd
import shapely
import shapely.geometry.base
from shapely.geometry import MultiPolygon, Polygon

from openeo.util import BBoxDict, normalize_crs


def _to_geodataframe(geometries: list, *, epsg: int) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame from a list of shapely geometries and an EPSG code."""
    return gpd.GeoDataFrame(geometry=geometries, crs=f"EPSG:{epsg}")

class JobSplittingFailure(Exception):
    pass


class TileGridInterface(metaclass=abc.ABCMeta):
    """
    Interface for tile grid classes that split a geometry into tiles.

    Implementations must define :meth:`get_tiles`, which takes a geometry and
    returns a :class:`~geopandas.GeoDataFrame` of tile geometries covering it.
    """

    @abc.abstractmethod
    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> "gpd.GeoDataFrame":
        """
        Calculate tiles to cover the given geometry.

        :param geometry: area of interest as a bounding-box dict
            (with keys ``west``, ``south``, ``east``, ``north``, and optionally ``crs``),
            a :class:`~shapely.geometry.Polygon`, or a :class:`~shapely.geometry.MultiPolygon`.
        :return: GeoDataFrame with one row per tile and CRS set.
        """
        ...


class SizeBasedTileGrid(TileGridInterface):
    """
    Tile grid that splits a geometry into regular tiles of a given size.

    The size is in meters for UTM and Web Mercator projections, or degrees for
    WGS84 (EPSG:4326).

    :param epsg: EPSG code of the projection to use for tiling.
        Supported values: ``4326`` (WGS84 degrees), ``3857`` (Web Mercator meters),
        and UTM zones (``32601``–``32660``, ``32701``–``32760``).
    :param size: tile size in the unit of measure of the projection.
    """

    # EPSG ranges for UTM zones
    _UTM_NORTH = range(32601, 32661)
    _UTM_SOUTH = range(32701, 32761)

    def __init__(self, *, epsg: int, size: float):
        try:
            epsg = normalize_crs(epsg)
        except (ValueError, TypeError) as e:
            raise JobSplittingFailure(f"Failed to normalize EPSG code for tile grid splitting: {epsg!r}.") from e
        if not isinstance(epsg, int):
            raise JobSplittingFailure(f"Only integer EPSG codes are supported for tile grid splitting, got {epsg!r}.")
        self.epsg = epsg
        self.size = size

    @classmethod
    def from_size_projection(cls, *, size: float, projection: str) -> "SizeBasedTileGrid":
        """Create a tile grid from size and projection"""
        # TODO: the constructor also does normalize_crs, so this factory looks like overkill at the moment
        return cls(epsg=normalize_crs(projection), size=size)

    def _get_x_offset(self) -> float:
        """
        Return the easting offset for the projection, used to align tiles to the
        coordinate system's grid origin.

        - UTM zones have a false easting of 500 000 m.
        - EPSG:3857 and EPSG:4326 have no offset.

        :raises JobSplittingFailure: for unsupported EPSG codes.
        """
        if self.epsg in self._UTM_NORTH or self.epsg in self._UTM_SOUTH:
            return 500_000.0
        elif self.epsg in (3857, 4326):
            return 0.0
        else:
            raise JobSplittingFailure(
                f"Unsupported EPSG code {self.epsg} for tile grid splitting. "
                f"Supported codes: 4326, 3857, and UTM zones (32601-32660, 32701-32760)."
            )

    @staticmethod
    def _split_bounding_box(to_cover: BBoxDict, x_offset: float, tile_size: float) -> List[Polygon]:
        """
        Split a bounding box into tiles of given size and projection.
        :param to_cover: bounding box dict with keys "west", "south", "east", "north", "crs"
        :param x_offset: offset to apply to the west and east coordinates
        :param tile_size: size of tiles in unit of measure of the projection
        :return: list of tiles (polygons)
        """
        xmin = int(math.floor((to_cover["west"] - x_offset) / tile_size))
        xmax = int(math.ceil((to_cover["east"] - x_offset) / tile_size)) - 1
        ymin = int(math.floor(to_cover["south"] / tile_size))
        ymax = int(math.ceil(to_cover["north"] / tile_size)) - 1

        tiles = []
        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                tiles.append(
                    BBoxDict(
                        west=max(x * tile_size + x_offset, to_cover["west"]),
                        south=max(y * tile_size, to_cover["south"]),
                        east=min((x + 1) * tile_size + x_offset, to_cover["east"]),
                        north=min((y + 1) * tile_size, to_cover["north"]),
                    ).as_polygon()
                )

        return tiles

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        if isinstance(geometry, dict):
            bbox = BBoxDict.from_dict(geometry)
        elif isinstance(geometry, (Polygon, MultiPolygon)):
            bbox = BBoxDict.from_any(geometry, crs=self.epsg)
        else:
            raise JobSplittingFailure(
                f"Expected a bounding-box dict, Polygon, or MultiPolygon, got {type(geometry).__name__}."
            )

        x_offset = self._get_x_offset()
        polygons = self._split_bounding_box(to_cover=bbox, x_offset=x_offset, tile_size=self.size)
        return _to_geodataframe(polygons, epsg=self.epsg)


class PredefinedTileGrid(TileGridInterface):
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

    def get_tiles(self, geometry: Union[Dict, Polygon, MultiPolygon]) -> gpd.GeoDataFrame:
        if isinstance(geometry, dict):
            geom = BBoxDict.from_dict(geometry).as_polygon()
        elif isinstance(geometry, (Polygon, MultiPolygon)):
            geom = geometry
        else:
            raise JobSplittingFailure(
                f"Expected a bounding-box dict, Polygon, or MultiPolygon, got {type(geometry).__name__}."
            )

        mask = self._gdf.intersects(geom)
        return self._gdf.loc[mask].copy().reset_index(drop=True)


def split_area(
    aoi: Union[Dict, MultiPolygon, Polygon],
    *,
    projection: Optional[str] = None,
    tile_size: Optional[float] = None,
    tile_grid: Optional[Union[TileGridInterface, gpd.GeoDataFrame]] = None,
) -> gpd.GeoDataFrame:
    """
    Split an area of interest into tiles.

    There are two ways to define how the area is tiled:

    1. **By tile size and projection** — pass *projection* and *tile_size*.
       A :class:`SizeBasedTileGrid` is created under the hood.

    2. **By pre-defined grid** — pass a :class:`TileGridInterface` instance
       (e.g. :class:`PredefinedTileGrid`) or a :class:`~geopandas.GeoDataFrame`
       as *tile_grid*.

    :param aoi: area of interest as a bounding-box dict
        (keys ``west``, ``south``, ``east``, ``north``, optionally ``crs``),
        a :class:`~shapely.geometry.Polygon`, or
        a :class:`~shapely.geometry.MultiPolygon`.
    :param projection: EPSG string (e.g. ``"EPSG:3857"``) for the tile grid.
        Required when *tile_grid* is not supplied and the AOI has no ``crs`` field.
    :param tile_size: tile edge length in the unit of the projection.
        Required when *tile_grid* is not supplied.
    :param tile_grid: a :class:`TileGridInterface` instance or a
        :class:`~geopandas.GeoDataFrame` (with CRS set) that defines the tiling
        strategy.  Mutually exclusive with *projection* / *tile_size*.
    :return: :class:`~geopandas.GeoDataFrame` with one row per tile and CRS set.
    :raises JobSplittingFailure: on invalid or contradictory arguments.
    """
    if tile_grid is not None:
        if projection is not None or tile_size is not None:
            raise JobSplittingFailure(
                "Cannot combine 'tile_grid' with 'projection' or 'tile_size'. "
                "Either pass a TileGridInterface, or pass projection + tile_size."
            )
        if isinstance(tile_grid, gpd.GeoDataFrame):
            tile_grid = PredefinedTileGrid(tiles=tile_grid)
        return tile_grid.get_tiles(aoi)

    # --- Size-based splitting path ---
    if tile_size is None:
        raise JobSplittingFailure(
            "Either provide a 'tile_grid', or at least 'tile_size' (and optionally 'projection')."
        )

    if projection is None:
        if isinstance(aoi, dict) and "crs" in aoi:
            projection = aoi["crs"]
        else:
            raise JobSplittingFailure(
                "'projection' is required when the area of interest does not contain a 'crs' field."
            )

    grid = SizeBasedTileGrid(epsg=normalize_crs(projection), size=tile_size)
    return grid.get_tiles(aoi)
