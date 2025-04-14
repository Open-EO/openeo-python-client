import abc
import math
from typing import Dict, List, NamedTuple, Optional, Union

import shapely
from shapely.geometry import MultiPolygon, Polygon

from openeo.util import normalize_crs


class JobSplittingFailure(Exception):
    pass

class _BoundingBox(NamedTuple):
    """Simple NamedTuple container for a bounding box"""

    west: float
    south: float
    east: float
    north: float
    crs: int = 4326

    @classmethod
    def from_dict(cls, d: Dict) -> "_BoundingBox":
        """Create a bounding box from a dictionary"""
        if d.get("crs") is not None:
            d["crs"] = normalize_crs(d["crs"])
        return cls(**{k: d[k] for k in cls._fields if k not in cls._field_defaults or k in d})

    @classmethod
    def from_polygon(cls, polygon: Union[MultiPolygon, Polygon], crs: Optional[int] = None) -> "_BoundingBox":
        """Create a bounding box from a shapely Polygon or MultiPolygon"""
        crs = normalize_crs(crs)
        return cls(*polygon.bounds, crs=4326 if crs is None else crs)

    def as_dict(self) -> Dict:
        return self._asdict()

    def as_polygon(self) -> Polygon:
        """Get bounding box as a shapely Polygon"""
        return shapely.geometry.box(minx=self.west, miny=self.south, maxx=self.east, maxy=self.north)


class _TileGridInterface(metaclass=abc.ABCMeta):
    """Interface for tile grid classes"""

    @abc.abstractmethod
    def get_tiles(self, geometry: Union[Dict, MultiPolygon, Polygon]) -> List[Polygon]:
        """Calculate tiles to cover given bounding box"""
        ...


class _SizeBasedTileGrid(_TileGridInterface):
    """
    Specification of a tile grid, parsed from a size and a projection.
    The size is in m for UTM projections or degrees for WGS84.
    """

    def __init__(self, epsg: int, size: float):
        self.epsg = normalize_crs(epsg)
        self.size = size

    @classmethod
    def from_size_projection(cls, size: float, projection: str) -> "_SizeBasedTileGrid":
        """Create a tile grid from size and projection"""
        return cls(normalize_crs(projection), size)

    def _epsg_is_meters(self) -> bool:
        """Check if the projection unit is in meters. (EPSG:3857 or UTM)"""
        return 32601 <= self.epsg <= 32660 or 32701 <= self.epsg <= 32760 or self.epsg == 3857

    @staticmethod
    def _split_bounding_box(to_cover: _BoundingBox, x_offset: float, tile_size: float) -> List[Polygon]:
        """
        Split a bounding box into tiles of given size and projection.
        :param to_cover: bounding box dict with keys "west", "south", "east", "north", "crs"
        :param x_offset: offset to apply to the west and east coordinates
        :param tile_size: size of tiles in unit of measure of the projection
        :return: list of tiles (polygons)
        """
        xmin = int(math.floor((to_cover.west - x_offset) / tile_size))
        xmax = int(math.ceil((to_cover.east - x_offset) / tile_size)) - 1
        ymin = int(math.floor(to_cover.south / tile_size))
        ymax = int(math.ceil(to_cover.north / tile_size)) - 1

        tiles = []
        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                tiles.append(
                    _BoundingBox(
                        west=max(x * tile_size + x_offset, to_cover.west),
                        south=max(y * tile_size, to_cover.south),
                        east=min((x + 1) * tile_size + x_offset, to_cover.east),
                        north=min((y + 1) * tile_size, to_cover.north),
                    ).as_polygon()
                )

        return tiles

    def get_tiles(self, geometry: Union[Dict, MultiPolygon, Polygon]) -> List[Polygon]:
        if isinstance(geometry, dict):
            bbox = _BoundingBox.from_dict(geometry)

        elif isinstance(geometry, Polygon) or isinstance(geometry, MultiPolygon):
            bbox = _BoundingBox.from_polygon(geometry, crs=self.epsg)

        else:
            raise JobSplittingFailure("geometry must be a dict or a shapely.geometry.Polygon or MultiPolygon")

        x_offset = 500_000 if self._epsg_is_meters() else 0

        tiles = _SizeBasedTileGrid._split_bounding_box(bbox, x_offset, self.size)

        return tiles


def split_area(
    aoi: Union[Dict, MultiPolygon, Polygon], projection: str = "EPSG:3857", tile_size: float = 20_000.0
) -> List[Polygon]:
    """
    Split area of interest into tiles of given size and projection.
    :param aoi: area of interest (bounding box or shapely polygon)
    :param projection: projection to use for splitting. Default is web mercator (EPSG:3857)
    :param tile_size: size of tiles in unit of measure of the projection
    :return: list of tiles (polygons).
    """
    if isinstance(aoi, dict):
        projection = aoi.get("crs", projection)

    tile_grid = _SizeBasedTileGrid.from_size_projection(tile_size, projection)
    return tile_grid.get_tiles(aoi)
