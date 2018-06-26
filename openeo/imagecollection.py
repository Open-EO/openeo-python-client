from abc import ABC
from typing import List, Dict, Union
from datetime import datetime,date

from openeo.job import Job
from shapely.geometry import Polygon, MultiPolygon


class ImageCollection(ABC):
    """Class representing an Image Collection. """

    def __init__(self):
        pass


    def date_range_filter(self,start_date:Union[str,datetime,date],end_date:Union[str,datetime,date]) -> 'ImageCollection':
        """
        Specifies a date range filter to be applied on the ImageCollection

        :param start_date: Start date of the filter, inclusive, format: "YYYY-MM-DD".
        :param end_date: End date of the filter, exclusive, format e.g.: "2018-01-13".
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

    def apply_tiles(self, code:str) -> 'ImageCollection':
        """Apply a function to the tiles of an image collection.

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

            :return: An ImageCollection containing  a result for each time window
        """
        pass

    def reduce_time(self, aggregationfunction) -> 'ImageCollection' :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.

            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.

            :return: An ImageCollection without a time dimension
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
            Finds the maximum value of time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
        """
        pass

    def mean_time(self) -> 'ImageCollection':
        """
            Finds the mean value of time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
        """
        pass

    def median_time(self) -> 'ImageCollection':
        """
            Finds the median value of time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
        """
        pass

    def ndvi(self, red, nir) -> 'ImageCollection':
        """ NDVI

            :param red: Reference to the red band
            :param nir: Reference to the nir band

            :return An ImageCollection instance
        """
        pass


    def stretch_colors(self, min, max) -> 'ImageCollection':
        """ Color stretching

            :param min: Minimum value
            :param max: Maximum value

            :return An ImageCollection instance
        """

    def band_filter(self, bands) -> 'ImageCollection':
        """Filter the imagecollection by the given bands

            :param bands: List of band names or single band name as a string.

            :return An ImageCollection instance
        """

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

    def zonal_statistics(self, regions, func, scale=1000, interval="day") -> 'Dict':
        """
        Calculates statistics for each zone specified in a file.

        :param regions: GeoJSON or a path to a GeoJSON file containing the
                        regions. For paths you must specify the path to a
                        user-uploaded file without the user id in the path.
        :param func: Statistical function to calculate for the specified
                     zones. example values: min, max, mean, median, mode
        :param scale: A nominal scale in meters of the projection to work
                      in. Defaults to 1000.
        :param interval: Interval to group the time series. Allowed values:
                        day, wee, month, year. Defaults to day.

        :return A timeseries
        """
        pass

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> Dict:
        """
        Extract a mean time series for the given (multi)polygon. Its points are expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon

        :return: Dict: A timeseries
        """
        pass

    def tiled_viewing_service(self) -> Dict:
        """
        Returns metadata for a tiled viewing service that visualizes this layer.

        :return: A dictionary object containing the viewing service metadata, such as the connection 'url'.
        """
        pass

    def download(self,outputfile:str, bbox="", time="",**format_options):
        """Extracts a binary raster from this image collection."""
        pass

    def send_job(self) -> Job:
        """Sends the current process to the backend, for batch processing.

            :return: Job: A job object that can be used to query the processing status.
        """
        pass

    def graph_add_process(self, process_id, args) -> 'ImageCollection':
        """
        Returns a new imagecollection with an added process with the given process
        id and a dictionary of arguments

        :param process_id: String, Process Id of the added process.
        :param args: Dict, Arguments of the process.

        :return: imagecollection: Instance of the ImageCollection class
        """
