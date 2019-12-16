import warnings
from abc import ABC
from collections import namedtuple
from datetime import datetime, date
from typing import List, Dict, Union, Sequence, Tuple, Callable

from deprecated import deprecated
from shapely.geometry import Polygon, MultiPolygon

from openeo.job import Job
from openeo.util import get_temporal_extent, first_not_none


class CollectionMetadata:
    """
    Wrapper for Image Collection metadata.

    Simplifies getting values from deeply nested mappings,
    allows additional parsing and normalizing compatibility issues.

    Metadata is expected to follow format defined by
    https://open-eo.github.io/openeo-api/apireference/#tag/EO-Data-Discovery/paths/~1collections~1{collection_id}/get
    """

    # Simple container class for band metadata (name, common name, wavelength in micrometer)
    Band = namedtuple("Band", ["name", "common_name", "wavelength_um"])

    def __init__(self, metadata: dict):
        self._metadata = metadata
        # Cached band metadata (for lazy loading/parsing)
        self._bands = None

    def get(self, *args, default=None):
        """Helper to recursively index into nested metadata dict"""
        cursor = self._metadata
        for arg in args:
            if arg not in cursor:
                return default
            cursor = cursor[arg]
        return cursor

    @property
    def extent(self) -> dict:
        return self._metadata.get('extent')

    @property
    def bands(self) -> List[Band]:
        """Get band metadata as list of Band metadata tuples"""
        if self._bands is None:
            self._bands = self._get_bands()
        return self._bands

    def _get_bands(self) -> List[Band]:
        # TODO refactor this specification specific processing away in a subclass?
        # First try `properties/eo:bands`
        eo_bands = self.get('properties', 'eo:bands')
        if eo_bands:
            # center_wavelength is in micrometer according to spec
            return [self.Band(b['name'], b.get('common_name'), b.get('center_wavelength')) for b in eo_bands]
        warnings.warn("No band metadata under `properties/eo:bands` trying some fallback sources.")
        # Fall back on `properties/cube:dimensions`
        cube_dimensions = self.get('properties', 'cube:dimensions', default={})
        for dim in cube_dimensions.values():
            if dim["type"] == "bands":
                # TODO: warn when multiple (or no) "bands" type?
                return [self.Band(b, None, None) for b in dim["values"]]
        # Try non-standard VITO bands metadata
        # TODO remove support for this legacy non-standard band metadata
        if "bands" in self._metadata:
            nm_to_um = lambda nm: nm / 1000. if nm is not None else None
            return [self.Band(b["band_id"], b.get("name"), nm_to_um(b.get("wavelength_nm")))
                    for b in self._metadata["bands"]]

    @property
    def band_names(self) -> List[str]:
        return [b.name for b in self.bands]

    @property
    def band_common_names(self) -> List[str]:
        return [b.common_name for b in self.bands]

    def filter_bands(self,bands_names):
        indices = [ self.get_band_index(name) for name in bands_names]
        self._bands = [ self.bands[index] for index in indices]

    def get_band_index(self, band: Union[int, str]) -> int:
        """
        Resolve a band name/index to band index
        :param band: band name, common name or index
        :return int: band index
        """
        band_names = self.band_names
        if isinstance(band, int) and 0 <= band < len(band_names):
            return band
        elif isinstance(band, str):
            common_names = self.band_common_names
            # First try common names if possible
            if band in common_names:
                return common_names.index(band)
            if band in band_names:
                return band_names.index(band)
        raise ValueError("Band {b!r} not available in collection. Valid names: {n!r}".format(b=band, n=band_names))



class ImageCollection(ABC):
    """Class representing Processes. """

    def __init__(self, metadata: CollectionMetadata = None):
        self.metadata = metadata if isinstance(metadata, CollectionMetadata) else CollectionMetadata(metadata or {})

    @deprecated("Use `filter_temporal()` instead")
    def date_range_filter(self, start_date:Union[str,datetime,date],end_date:Union[str,datetime,date]) -> 'ImageCollection':
        """
        Specifies a date range filter to be applied on the ImageCollection
        DEPRECATED: use :func:`openeo.ImageCollection.filter_temporal`

        :param start_date: Start date of the filter, inclusive, format: "YYYY-MM-DD".
        :param end_date: End date of the filter, exclusive, format e.g.: "2018-01-13".
        :return: An ImageCollection filtered by date.
        """
        return self.filter_temporal(start_date=start_date, end_date=end_date)

    @deprecated("Use `filter_temporal()` instead")
    def filter_daterange(self, extent) -> 'ImageCollection':
        """Drops observations from a collection that have been captured before
            a start or after a given end date.

            :param extent: List of starting date and ending date of the filter
            :return: An ImageCollection filtered by date.
        """
        return self.filter_temporal(extent=extent)

    def filter_temporal(self, *args,
                        start_date: Union[str, datetime, date] = None, end_date: Union[str, datetime, date] = None,
                        extent: Union[list, tuple] = None) -> 'ImageCollection':
        """
        Limit the ImageCollection to a certain date range, which can be specified in several ways:

        >>> im.filter_temporal("2019-07-01", "2019-08-01")
        >>> im.filter_temporal(["2019-07-01", "2019-08-01"])
        >>> im.filter_temporal(extent=["2019-07-01", "2019-08-01"])
        >>> im.filter_temporal(start_date="2019-07-01", end_date="2019-08-01"])

        :param start_date: start date of the filter (inclusive), as a string or date object
        :param end_date: end date of the filter (exclusive), as a string or date object
        :param extent: two element list/tuple start and end date of the filter
        :return: An ImageCollection filtered by date.

        Subclasses are recommended to implement `_filter_temporal', which has simpler API.

        https://open-eo.github.io/openeo-api/processreference/#filter_temporal
        """
        start, end = get_temporal_extent(*args, start_date=start_date, end_date=end_date, extent=extent)
        return self._filter_temporal(start, end)

    def _filter_temporal(self, start_date: str, end_date: str) -> 'ImageCollection':
        # Subclasses are expected to implement this method, but for bit of backward compatibility
        # with old style subclasses we forward to `date_range_filter`
        # TODO: replace this with raise NotImplementedError() or decorate with @abstractmethod
        return self.date_range_filter(start_date, end_date)

    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'ImageCollection':
        """
        Limits the ImageCollection to a given spatial bounding box.

        :param west: west boundary (longitude / easting)
        :param east: east boundary (longitude / easting)
        :param north: north boundary (latitude / northing)
        :param south: south boundary (latitude / northing)
        :param crs: spatial reference system of boundaries as
                    proj4 or EPSG:12345 like string
        :param base: lower left corner coordinate axis 3
        :param height: upper right corner coordinate axis 3
        :return: An image collection cropped to the specified bounding box.

        https://open-eo.github.io/openeo-api/v/0.4.1/processreference/#filter_bbox

        # TODO: allow passing some kind of bounding box object? e.g. a (xmin, ymin, xmax, ymax) tuple?
        """
        # Subclasses are expected to implement this method, but for bit of backwards compatibility
        # with old style subclasses we forward to `bbox_filter`
        # TODO: replace this with raise NotImplementedError() or decorate with @abstractmethod
        kwargs = dict(west=west, east=east, north=north, south=south, crs=crs)
        if base or height:
            kwargs.update(base=base, height=height)
        return self.bbox_filter(**kwargs)

    @deprecated(reason="Use `filter_bbox()` instead.")
    def bbox_filter(self, west=None, east=None, north=None, south=None, crs=None,left=None, right=None, top=None, bottom=None, srs=None, base=None, height=None ) -> 'ImageCollection':
        """
        Specifies a bounding box to filter input image collections.
        DEPRECATED: use :func:`openeo.ImageCollection.filter_bbox`

        :param left:
        :param right:
        :param top:
        :param bottom:
        :param srs:
        :return: An image collection cropped to the specified bounding box.
        """
        return self.filter_bbox(
            west=first_not_none(west, left), east=first_not_none(east, right),
            north=first_not_none(north, top), south=first_not_none(south, bottom),
            base=base, height=height,
            crs=first_not_none(crs, srs)
        )

    def resample_spatial(self, resolution: Union[float, Tuple[float, float]],
                         projection: Union[int, str] = None, method: str = 'near', align: str = 'upper-left'):
        """
        Resamples the spatial dimensions (x,y) of the data cube to a specified resolution and/or warps the data cube
        to the target projection. At least resolution or projection must be specified.

        Use filter_bbox to set the target spatial extent.

        https://open-eo.github.io/openeo-api/v/0.4.0/processreference/#resample_spatial

        :param resolution: Either a single number or an array with separate resolutions for each spatial dimension. Resamples the data cube to the target resolution, which can be specified either as separate values for x and y or as a single value for both axes.  Specified in the units of the target projection. Doesn't change the resolution by default (0).
        :param projection: Either an epsg code, as an integer, or a proj-definition
        string. Warps the data cube to the target projection. Target projection specified as EPSG code or PROJ
        definition. Doesn't change the projection by default (null).
        :param method: Resampling method. Methods are
        inspired by GDAL, see gdalwarp for more information. Possible values: near, bilinear, cubic, cubicspline,
        lanczos, average, mode, max, min, med, q1, q3
        :param align: Specifies to which corner of the spatial extent
        the new resampled data is aligned to. Possible values: lower-left, upper-left, lower-right, upper-right
        :return: A raster data cube with values warped onto the new projection.
        """
        pass

    def apply(self,process:str,arguments = {}) -> 'ImageCollection':
        """
        Applies a unary process (a local operation) to each value of the specified or all dimensions in the data cube.
        https://open-eo.github.io/openeo-api/v/0.4.0/processreference/#apply

        :param process: A process (callback) to be applied on each value. The specified process must be unary meaning that it must work on a single value.
        :param dimensions: The names of the dimensions to apply the process on. Defaults to an empty array so that all dimensions are used.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        raise NotImplementedError("Apply function not supported by this data cube.")

    def apply_pixel(self, bands: List, bandfunction) -> 'ImageCollection':
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
        pass

    def apply_tiles(self, code: str) -> 'ImageCollection':
        """Apply a function to the tiles of an image collection.

        This type applies a simple function to one pixel of the input image or image collection.
        The function gets the value of one pixel (including all bands) as input and produces a single scalar or tuple output.
        The result has the same schema as the input image (collection) but different bands.
        Examples include the computation of vegetation indexes or filtering cloudy pixels.

        :param code: Code to apply to the ImageCollection

        :return: An image collection with the tiles applied function.
        """
        pass

    def apply_dimension(self, code: str, runtime=None, version="latest",dimension='temporal') -> 'ImageCollection':
        """
        Applies an n-ary process (i.e. takes an array of pixel values instead of a single pixel value) to a raster data cube.
        In contrast, the process apply applies an unary process to all pixel values.

        By default, apply_dimension applies the the process on all pixel values in the data cube as apply does, but the parameter dimension can be specified to work only on a particular dimension only. For example, if the temporal dimension is specified the process will work on a time series of pixel values.

        The n-ary process must return as many elements in the returned array as there are in the input array. Otherwise a CardinalityChanged error must be returned.


        :param code: UDF code or process identifier
        :param runtime:
        :param version:
        :param dimension:
        :return:
        :raises: CardinalityChangedError
        """
        pass

    def aggregate_time(self, temporal_window, aggregationfunction) -> 'ImageCollection' :
        """ Applies a windowed reduction to a timeseries by applying a user defined function.
            DEPRECATED: use Aggregate_temporal

            :param temporal_window: The time windows to group by, can be a list of halfopen intervals
            :param aggregationfunction: The function to apply to each time window. Takes a pandas Timeseries as input.

            :return: An ImageCollection containing  a result for each time window
        """
        pass

    def aggregate_temporal(self, intervals:List,labels:List, reducer, dimension:str = None) -> 'ImageCollection' :
        """ Computes a temporal aggregation based on an array of date and/or time intervals.

            Calendar hierarchies such as year, month, week etc. must be transformed into specific intervals by the clients. For each interval, all data along the dimension will be passed through the reducer. The computed values will be projected to the labels, so the number of labels and the number of intervals need to be equal.

            If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :param intervals: Temporal left-closed intervals so that the start time is contained, but not the end time.
            :param labels: Labels for the intervals. The number of labels and the number of groups need to be equal.
            :param reducer: A reducer to be applied on all values along the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
            :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :return: An ImageCollection containing  a result for each time window
        """
        pass

    def reduce(self,reducer,dimension):
        """
        Applies a reducer to a data cube dimension by collapsing all the input values along the specified dimension into a single output value computed by the reducer.

        The reducer must accept an array and return a single value (see parameter reducer). Nominal values are possible, but need to be mapped, e.g. band names to wavelengths, date strings to numeric timestamps since 1970 etc.

        https://open-eo.github.io/openeo-api/v/0.4.0/processreference/#reduce

        :param reducer: A reducer to be applied on the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
        :param dimension: The dimension over which to reduce.
        :return: A data cube with the newly computed values. The number of dimensions is reduced, but the resolution and cardinality are the same as for the original data cube.
        """
        raise NotImplementedError("This image collection does not support the reduce operation.")

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

    def count_time(self) -> 'ImageCollection':
        """
            Counts the number of images with a valid mask in a time series for all bands of the input dataset.

            :return: An ImageCollection without a time dimension.
        """
        pass

    def ndvi(self, name: str = "ndvi") -> 'ImageCollection':
        """ Normalized Difference Vegetation Index (NDVI)

            :param name: Name of the newly created band

            :return: An ImageCollection instance
        """
        pass

    def stretch_colors(self, min, max) -> 'ImageCollection':
        """ Color stretching

            :param min: Minimum value
            :param max: Maximum value

            :return: An ImageCollection instance
        """
        pass

    def filter_bands(self, bands) -> 'ImageCollection':
        """Filters the bands in the data cube so that bands that don't match any of the criteria are dropped from the data cube.
        The data cube is expected to have only one spectral dimension.
        The following criteria can be used to select bands:

            :param bands: List of band names or single band name as a string. The order of the specified array defines the order of the bands in the data cube, which can be important for subsequent processes.
            :return An ImageCollection instance
        """
        # TODO: also handle a common_names (and wavelengths) argument like https://open-eo.github.io/openeo-api/processreference/#filter_bands?
        #       see https://github.com/Open-EO/openeo-processes/issues/77
        pass

    @deprecated("use `filter_bands()` instead")
    def band_filter(self, bands) -> 'ImageCollection':
        return self.filter_bands(bands=bands)

    def band(self, band_name) -> 'ImageCollection':
        """Select the given band, as input for subsequent operations.

            :param band_name: List of band names or single band name as a string.
            :return: An ImageCollection instance
        """
        # TODO: does this method have to be defined at the level of the ImageCollection base class? it is only implemented by the rest client
        pass

    def mask(self, polygon: Union[Polygon, MultiPolygon]=None, srs="EPSG:4326", rastermask: 'ImageCollection'=None,
             replacement=None) -> 'ImageCollection':
        """
        Mask the image collection using either a polygon or a raster mask.

        All pixels outside the polygon should be set to the nodata value.
        All pixels inside, or intersecting the polygon should retain their original value.

        All pixels are replaced for which the corresponding pixels in the mask are non-zero (for numbers) or True
        (for boolean values).

        The pixel values are replaced with the value specified for replacement, which defaults to None (no data).
        No data values will be left untouched by the masking operation.

        TODO: Does mask by polygon imply cropping?
        TODO: what about naming? Masking can also be done using a raster mask...
        TODO: what will happen if the intersection between the mask and the imagecollection is empty? Raise an error?

        :param polygon: A polygon, provided as a :class:`shapely.geometry.Polygon` or :class:`shapely.geometry.MultiPolygon`
        :param srs: The reference system of the provided polygon, provided as an 'EPSG:XXXX' string. By default this is Lat Lon (EPSG:4326).
        :param rastermask: the raster mask
        :param replacement: the value to replace the masked pixels with
        :raise: :class:`ValueError` if a polygon is supplied and its area is 0.
        :return: A new ImageCollection, with the mask applied.
        """
        pass

    def merge(self, other: 'ImageCollection') -> 'ImageCollection':
        """
        Merge the bands of this data cubes with the bands of another datacube. The bands of 'other' will be appended to the bands
        of this datacube, maintaining the order.

        :param other: The other datacube to merge with this datacube
        :return: A new datacube with bands merged.
        """
        pass

    def apply_kernel(self, kernel, factor=1.0) -> 'ImageCollection':
        """
        Applies a focal operation based on a weighted kernel to each value of the specified dimensions in the data cube.

        :param kernel: The kernel to be applied on the data cube. The kernel has to be as many dimensions as the data cube has dimensions.
        :param factor: A factor that is multiplied to each value computed by the focal operation. This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        pass

    ####VIEW methods #######

    def zonal_statistics(self, regions, func, scale=1000, interval="day") -> 'ImageCollection':
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

        :return: A timeseries
        """
        pass

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'ImageCollection':
        """
        Extract a mean time series for the given (multi)polygon. Its points are expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file

        :return: Dict: A timeseries
        """
        pass

    def polygonal_histogram_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'ImageCollection':
        """
        Extract a histogram time series for the given (multi)polygon. Its points are expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file

        :return: Dict: A timeseries
        """
        pass

    def polygonal_median_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'ImageCollection':
        """
        Extract a median time series for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """
        pass


    def polygonal_standarddeviation_timeseries(self, polygon: Union[Polygon, MultiPolygon, str]) -> 'ImageCollection':
        """
        Extract a time series of standard deviations for the given (multi)polygon. Its points are
        expected to be in the EPSG:4326 coordinate
        reference system.

        :param polygon: The (multi)polygon; or a file path or HTTP URL to a GeoJSON file or shape file
        :return: ImageCollection
        """
        pass

    def tiled_viewing_service(self,**kwargs) -> Dict:
        """
        Returns metadata for a tiled viewing service that visualizes this layer.

        :param type: The type of viewing service to create, for instance: 'WMTS'
        :param title: A short description to easily distinguish entities.
        :param description: Detailed description to fully explain the entity. CommonMark 0.28 syntax MAY be used for rich text representation.

        :return: A dictionary object containing the viewing service metadata, such as the connection 'url'.
        """
        pass

    def pipe(self, func: Callable, *args, **kwargs):
        """
        Pipe the image collection through a function and return the result.

        Allows to wrap a sequence of operations in a function and reuse it in a chained fashion.
        For example:

        >>> # Define a reusable set of ImageCollection operations
        >>> def ndvi_percent(cube):
        ...     return cube.ndvi().linear_scale_range(0, 1, 0, 100)
        >>> # Reuse the procedure
        >>> ndvi1 = cube1.pipe(ndvi_percent)
        >>> ndvi2 = cube2.pipe(ndvi_percent)
        >>> ndvi3 = cube3.pipe(ndvi_percent)

        Inspired by pandas.DataFrame.pipe

        :param func: function that expects a ImageCollection as first argument (and optionally additional arguments)
        :return: result of applying the function to the ImageCollection
        """
        return func(self, *args, **kwargs)
