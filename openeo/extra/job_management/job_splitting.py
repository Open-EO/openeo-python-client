import abc
import math
from typing import List, NamedTuple, Optional, Union

import pyproj
import shapely
from shapely.geometry import shape
from shapely.ops import transform


class JobSplittingFailure(Exception):
    pass


# TODO: This function is also defined in openeo-python-driver. But maybe we want to avoid a dependency on openeo-python-driver?
def reproject_bounding_box(bbox: dict, from_crs: Optional[str], to_crs: str) -> dict:
    """
    Reproject given bounding box dictionary

    :param bbox: bbox dict with fields "west", "south", "east", "north"
    :param from_crs: source CRS. Specify `None` to use the "crs" field of input bbox dict
    :param to_crs: target CRS
    :return: bbox dict (fields "west", "south", "east", "north", "crs")
    """
    box = shapely.geometry.box(bbox["west"], bbox["south"], bbox["east"], bbox["north"])
    if from_crs is None:
        from_crs = bbox["crs"]
    tranformer = pyproj.Transformer.from_crs(crs_from=from_crs, crs_to=to_crs, always_xy=True)
    reprojected = transform(tranformer.transform, box)
    return dict(zip(["west", "south", "east", "north"], reprojected.bounds), crs=to_crs)


# TODO: This class is also defined in openeo-aggregator. But maybe we want to avoid a dependency on openeo-aggregator?
class BoundingBox(NamedTuple):
    """Simple NamedTuple container for a bounding box"""

    west: float
    south: float
    east: float
    north: float
    crs: str = "EPSG:4326"

    @classmethod
    def from_dict(cls, d: dict) -> "BoundingBox":
        return cls(**{k: d[k] for k in cls._fields if k not in cls._field_defaults or k in d})

    @classmethod
    def from_polygon(cls, polygon: shapely.geometry.Polygon, projection: Optional[str] = None) -> "BoundingBox":
        """Create a bounding box from a shapely Polygon"""
        return cls(*polygon.bounds, projection if projection is not None else cls.crs)

    def as_dict(self) -> dict:
        return self._asdict()

    def as_polygon(self) -> shapely.geometry.Polygon:
        """Get bounding box as a shapely Polygon"""
        return shapely.geometry.box(minx=self.west, miny=self.south, maxx=self.east, maxy=self.north)


class TileGridInterface(metaclass=abc.ABCMeta):
    """Interface for tile grid classes"""

    @abc.abstractmethod
    def get_tiles(self, geometry: Union[dict, shapely.geometry.Polygon]) -> list[Union[dict, shapely.geometry.Polygon]]:
        """Calculate tiles to cover given bounding box"""
        ...


class SizeBasedTileGrid(TileGridInterface):
    """
    Specification of a tile grid, parsed from a size and a projection.
    """

    def __init__(self, epsg: str, size: float):
        self.epsg = epsg
        self.size = size

    @classmethod
    def from_size_projection(cls, size: float, projection: str) -> "SizeBasedTileGrid":
        """Create a tile grid from size and projection"""
        return cls(projection.lower(), size)

    def get_tiles(self, geometry: Union[dict, shapely.geometry.Polygon]) -> list[Union[dict, shapely.geometry.Polygon]]:
        if isinstance(geometry, dict):
            bbox = BoundingBox.from_dict(geometry)
            bbox_crs = bbox.crs
        elif isinstance(geometry, shapely.geometry.Polygon):
            bbox = BoundingBox.from_polygon(geometry, projection=self.epsg)
            bbox_crs = self.epsg
        else:
            raise JobSplittingFailure("geometry must be a dict or a shapely.geometry.Polygon")

        if self.epsg == "epsg:4326":
            tile_size = self.size
            x_offset = 0
        else:
            tile_size = self.size * 1000
            x_offset = 500_000

        to_cover = BoundingBox.from_dict(reproject_bounding_box(bbox.as_dict(), from_crs=bbox_crs, to_crs=self.epsg))
        xmin = int(math.floor((to_cover.west - x_offset) / tile_size))
        xmax = int(math.ceil((to_cover.east - x_offset) / tile_size)) - 1
        ymin = int(math.floor(to_cover.south / tile_size))
        ymax = int(math.ceil(to_cover.north / tile_size)) - 1

        tiles = []
        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                tile = BoundingBox(
                    west=max(x * tile_size + x_offset, to_cover.west),
                    south=max(y * tile_size, to_cover.south),
                    east=min((x + 1) * tile_size + x_offset, to_cover.east),
                    north=min((y + 1) * tile_size, to_cover.north),
                    crs=self.epsg,
                )

                if isinstance(geometry, dict):
                    tiles.append(reproject_bounding_box(tile.as_dict(), from_crs=self.epsg, to_crs=bbox_crs))
                else:
                    tiles.append(tile.as_polygon())

        return tiles


def split_area(
    aoi: Union[dict, shapely.geometry.Polygon], projection="EPSG:326", tile_size: float = 20.0
) -> list[Union[dict, shapely.geometry.Polygon]]:
    """
    Split area of interest into tiles of given size and projection.
    :param aoi: area of interest (bounding box or shapely polygon)
    :param projection: projection to use for splitting. Default is web mercator (EPSG:3857)
    :param tile_size: size of tiles in km for UTM projections or degrees for WGS84
    :return: list of tiles (dicts with keys "west", "south", "east", "north", "crs" or shapely polygons). For dicts the original crs is preserved. For polygons the projection is set to the given projection.
    """
    tile_grid = SizeBasedTileGrid.from_size_projection(tile_size, projection)
    return tile_grid.get_tiles(aoi)
