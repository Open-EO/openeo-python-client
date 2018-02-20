from abc import ABC
from typing import List, Dict,Union
from datetime import datetime,date

from openeo.job import Job


class ImageCollection(ABC):
    """Class representing an Image Collection. """

    def __init__(self):
        pass


    def date_range_filter(self,start_date:Union[str,datetime,date],end_date:Union[str,datetime,date]) -> 'ImageCollection':
        """
        Specifies a date range filter to be applied on the ImageCollection

        :param start_date: Start date of the filter, inclusive.
        :param end_date: End date of the filter, exclusive TODO!
        :return: An ImageCollection filtered by date.
        """

    def bbox_filter(self,left:float,right:float,top:float,bottom:float,srs:str) -> 'ImageCollection':
        """
        Specifies a bounding box to filter input image collections.

        :param left:
        :param right:
        :param top:
        :param bottom:
        :param srs:
        :return: An image collection cropped to the specified bounding box.
        """

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection.

        This type applies a simple function to one pixel of the input image or image collection.
        The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
        The result has the same schema as the input image (collection) but different bands.
        Examples include the computation of vegetation indexes or filtering cloudy pixels.
        """
        pass

    def aggregate_time(self, temporal_window, aggregationfunction) -> 'ImageCollection' :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.

            :param temporal_window: The time window to group by
            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.
            :return An ImageCollection containing  a result for each time window
        """
        pass

    def reduce_time(self, aggregationfunction) -> 'ImageCollection' :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.


            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.
            :return An ImageCollection without a time dimension
        """
        pass

    def min_time(self) -> 'ImageCollection':
        """
            Finds the minimum value of time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
        """
        pass

    def max_time(self) -> 'ImageCollection':
        """
            Finds the minimum value of time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
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

        :return: A string containing the filename
        """
        pass

    def download(self,outputfile:str, bbox="", time="",**format_options):
        """Extraxts a geotiff from this image collection."""
        pass

    def send_job(self) -> Job:
        """Sends the current process to the backend, for processing."""
        pass