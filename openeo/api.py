from typing import Callable, List, Tuple
from shapely.geometry.polygon import Polygon
from datetime import date
from .catalog import EOProduct

def apply_to_pixels( function: Callable, product_id: str,from_date,to_date,bbox):
    """Applies 'function' on the pixels of a nD Coverage. Pixels are provided to the function in the form of a NumPy array.
    """
    return None

def apply_to_products( function: Callable[[EOProduct],None], product_id: str,from_date,to_date,bbox):
    """Apply 'function' to a timeseries of EO products. The function is reponsible for saving the result.

    :param function: The function to apply to each :class:`EOProduct`
    :param product_id: Identifier of a product
    :param from_date: timeseries will be computed from this date, inclusive
    :param to_date: timeseries will be computed to this date, inclusive

    """
    return None


def zonal_statistics(product_id: str,from_date: date,to_date: date,polygons: List[Tuple[str,Polygon]], method="Mean", crs: str="EPSG:4326") -> dict:
    """Computes a timeseries of zonal statistics for each polygon.

    TOOD how to specify polygon crs

    For example::

        import openeo
        from datetime import date
        start = date(2016, 8, 5)
        end = date.today()
        polygon = Polygon([(51, 4), (52, 5), (52, 4)])
        stats = openeo.zonal_statistics("SENTINEL2_NDVI",start,end,[("mypolygon",polygon)])

    :param product_id: Identifier of a product
    :param from_date: timeseries will be computed from this date, inclusive
    :param to_date: timeseries will be computed to this date, inclusive
    :param polygons: list of tuples containing a string to be used as identifier, and a polygon determining the area over which statistics will be computed
    :param method: The method to compute statistics, one of: mean, min, max
    :param crs: A coordinate reference system identifier indicating the CRS of the polygons. E.g. 'EPSG:4326"

    :return: A dict containing statistics for each polygon.
    :rtype: dict

    :raises ValueError: In case the product_id is invalid.
    """
    return None