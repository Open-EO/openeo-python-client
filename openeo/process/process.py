# This module consists of all processes available in openEO
# Calling a function in this module adds a process to the imagecollection and returns the new imagecollection.
from typing import List, Union
from datetime import datetime, date

# TODO is this module (still) useful? also see https://github.com/Open-EO/openeo-python-client/issues/61

def ndvi(imagecollection, red, nir):
    """ NDVI

    :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
    :param red: Reference to the red band
    :param nir: Reference to the nir band

    :return An ImageCollection instance
    """
    return imagecollection.ndvi(red, nir)


def filter_temporal(imagecollection, start_date: Union[str, datetime, date], end_date: Union[str, datetime, date]) -> 'ImageCollection':
    """
    Specifies a date range filter to be applied on the ImageCollection

    :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
    :param start_date: Start date of the filter, inclusive, format: "YYYY-MM-DD".
    :param end_date: End date of the filter, exclusive, format e.g.: "2018-01-13".

    :return: An ImageCollection filtered by date.
    """
    return imagecollection.filter_temporal(start_date, end_date)


def filter_bbox(imagecollection, west: float, east: float, north: float, south: float, crs: str) -> 'ImageCollection':
    """
    Specifies a bounding box to filter input image collections.

    :param imagecollection: Image collection to apply the process, Instance of ImageCollection
    :param west:
    :param east:
    :param north:
    :param south:
    :param crs:

    :return: An image collection cropped to the specified bounding box.
    """
    return imagecollection.filter_bbox(west=west, east=east, north=north, south=south, crs=crs)


def apply_pixel(imagecollection, bands: List, bandfunction) -> 'ImageCollection':
    """Apply a function to the given set of bands in this image collection.

    This type applies a simple function to one pixel of the input image or image collection.
    The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
    The result has the same schema as the input image (collection) but different bands.
    Examples include the computation of vegetation indexes or filtering cloudy pixels.

    :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
    :param bands: Bands to be used
    :param bandfunction: Band function to be used

    :return: An image collection with the pixel applied function.
    """
    return imagecollection.apply_pixel(bands, bandfunction)


def apply_tiles(imagecollection, code: str) -> 'ImageCollection':
    """Apply a function to the tiles of an image collection.

    This type applies a simple function to one pixel of the input image or image collection.
    The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
    The result has the same schema as the input image (collection) but different bands.
    Examples include the computation of vegetation indexes or filtering cloudy pixels.

    :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
    :param code: Code to apply to the ImageCollection

    :return: An image collection with the tiles applied function.
    """
    return imagecollection.apply_tiles(code)


def aggregate_time(imagecollection, temporal_window, aggregationfunction) -> 'ImageCollection' :
    """ Applies a windowed reduction to a timeseries by applying a user defined function.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
        :param temporal_window: The time window to group by
        :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.

        :return: An ImageCollection containing  a result for each time window
    """
    return imagecollection.aggregate_time(temporal_window, aggregationfunction)


def reduce_time(imagecollection, aggregationfunction) -> 'ImageCollection' :
    """ Applies a windowed reduction to a timeseries by applying a user defined function.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
        :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.

        :return: An ImageCollection without a time dimension
    """
    return imagecollection.reduce_time(aggregationfunction)


def min_time(imagecollection) -> 'ImageCollection':
    """
        Finds the minimum value of time series for all bands of the input dataset.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection

        :return: An ImageCollection without a time dimension.
    """
    return imagecollection.min_time()


def max_time(imagecollection) -> 'ImageCollection':
    """
        Finds the maximum value of time series for all bands of the input dataset.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection

        :return: An ImageCollection without a time dimension.
    """
    return imagecollection.max_time()


def mean_time(imagecollection) -> 'ImageCollection':
    """
        Finds the mean value of time series for all bands of the input dataset.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection

        :return: An ImageCollection without a time dimension.
    """
    return imagecollection.mean_time()


def median_time(imagecollection) -> 'ImageCollection':
    """
        Finds the median value of time series for all bands of the input dataset.

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection

        :return: An ImageCollection without a time dimension.
    """
    return imagecollection.median_time()


def stretch_colors(imagecollection, min, max) -> 'ImageCollection':
    """ Color stretching

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
        :param min: Minimum value
        :param max: Maximum value

        :return An ImageCollection instance
    """
    return imagecollection.stretch_colors(min, max)


def band_filter(imagecollection, bands) -> 'ImageCollection':
    """Filter the imagecollection by the given bands

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
        :param bands: List of band names or single band name as a string.

        :return An ImageCollection instance
    """
    return imagecollection.band_filter(bands)


def graph_add_process(imagecollection, process_id, args) -> 'ImageCollection':
    """
        Returns a new imagecollection with an added process with the given process
        id and a dictionary of arguments

        :param imagecollection: Imagecollection to apply the process, Instance of ImageCollection
        :param process_id: String, Process Id of the added process.
        :param args: Dict, Arguments of the process.

        :return: imagecollection: Instance of the ImageCollection class
    """
    return imagecollection.graph_add_process(process_id, args)
