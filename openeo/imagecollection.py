from abc import ABC
from typing import List, Dict

from pandas import Series


class ImageCollection(ABC):
    """Class representing an Image Collection. """

    def __init__(self):
        pass


    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection.

        This type applies a simple function to one pixel of the input image or image collection.
        The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
        The result has the same schema as the input image (collection) but different bands.
        Examples include the computation of vegetation indexes or filtering cloudy pixels.
        """
        pass

    def reduceByTime(self,temporal_window, aggregationfunction) -> Series :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.

            :param temporal_window: The time window to group by
            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.
            :return A pandas Timeseries object
        """
        pass



    ####VIEW methods #######
    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        """
        Extract a time series for the given point location.

        :param x: The x coordinate of the point
        :param y: The y coordinate of the point
        :param srs: The spatial reference system of the coordinates, by default this is 'EPSG:4326', where x=longitude and y=latitude.
        :return: Dict: A timeseries
        """
        pass

    def tiled_viewing_service(self) -> Dict:
        """
        Returns metadata for a tiled viewing service that visualizes this layer.
        :return: A dict containing viewing service metadata.
        """
        pass

    def geotiff(self, bbox="",time=""):
        """Extraxts a geotiff from this image collection."""
        pass
