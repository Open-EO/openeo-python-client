from abc import ABC
from datetime import datetime, date
from typing import List, Dict, Union, Tuple, Callable
import pathlib

from deprecated import deprecated
from shapely.geometry import Polygon, MultiPolygon

from openeo.metadata import CollectionMetadata
from openeo.util import get_temporal_extent, first_not_none
from openeo.job import Job

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

        https://processes.openeo.org/#resample_spatial

        :param resolution: Either a single number or an array with separate resolutions for each spatial dimension.
            Resamples the data cube to the target resolution, which can be specified either as separate values
            for x and y or as a single value for both axes.  Specified in the units of the target projection.
            Doesn't change the resolution by default (0).
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

    def resample_cube_spatial(self, target:'ImageCollection', method:str='near')-> 'ImageCollection':
        """
        Resamples the spatial dimensions (x,y) of this data cube to a target data cube and return the results as a new data cube.

        https://processes.openeo.org/#resample_cube_spatial

        :param target: An ImageCollection that specifies the target
        :param method: The resampling method.
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

    def apply_dimension(self, code: str, runtime=None, version="latest", dimension='t', target_dimension=None) -> 'ImageCollection':
        """
        Applies a user defined process to all pixel values along a dimension of a raster data cube. For example,
        if the temporal dimension is specified the process will work on a time series of pixel values.

        The process reduce_dimension also applies a process to pixel values along a dimension, but drops
        the dimension afterwards. The process apply applies a process to each pixel value in the data cube.

        The target dimension is the source dimension if not specified otherwise in the target_dimension parameter.
        The pixel values in the target dimension get replaced by the computed pixel values. The name, type and
         reference system are preserved.

        The dimension labels are preserved when the target dimension is the source dimension and the number of
        pixel values in the source dimension is equal to the number of values computed by the process. Otherwise,
        the dimension labels will be incrementing integers starting from zero, which can be changed using
        rename_labels afterwards. The number of labels will equal to the number of values computed by the process.



        :param code: UDF code or process identifier
        :param runtime: UDF runtime to use
        :param version: Version of the UDF runtime to use
        :param dimension: The name of the source dimension to apply the process on. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target_dimension: The name of the target dimension or null (the default) to use the source dimension
        specified in the parameter dimension. By specifying a target dimension, the source dimension is removed.
        The target dimension with the specified name and the type other (see add_dimension) is created, if it doesn't exist yet.

        :return: A datacube with the UDF applied to the given dimension.
        :raises: DimensionNotAvailable
        """
        pass

    def apply_neighborhood(self,process, size:List[Dict],overlap:List[Dict]):
        """
        Applies a focal process to a data cube.
        A focal process is a process that works on a 'neighbourhood' of pixels. The neighbourhood can extend into multiple dimensions, this extent is specified by the `size` argument. It is not only (part of) the size of the input window, but also the size of the output for a given position of the sliding window. The sliding window moves with multiples of `size`.

        An overlap can be specified so that neighbourhoods can have overlapping boundaries. This allows for continuity of the output. The values included in the data cube as overlap can't be modified by the given `process`.

        The neighbourhood size should be kept small enough, to avoid running beyond computational resources, but a too small size will result in a larger number of process invocations, which may slow down processing. Window sizes for spatial dimensions typically are in the range of 64 to 512 pixels, while overlaps of 8 to 32 pixels are common.

        The process must not add new dimensions, or remove entire dimensions, but the result can have different dimension labels.

        For the special case of 2D convolution, it is recommended to use ``apply_kernel()``.

        @param process: Process to be applied on all neighbourhoods.
        @param size: Neighbourhood sizes along each dimension. This object maps dimension names to either a physical measure (e.g. 100 m, 10 days) or pixels (e.g. 32 pixels). For dimensions not specified, the default is to provide all values. Be aware that including all values from overly large dimensions may not be processed at once.
        @param overlap: Overlap of neighbourhoods along each dimension to avoid border effects. For instance a temporal dimension can add 1 month before and after a neighbourhood. In the spatial dimensions, this is often a number of pixels. The overlap specified is added before and after, so an overlap of 8 pixels will add 8 pixels on both sides of the window, so 16 in total. Be aware that large overlaps increase the need for computational resources and modifying overlapping data in subsequent operations have no effect.
        @return: A data cube with the newly computed values. The cardinality, resolution and the number of dimensions are the same as for the original data cube.
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

    def aggregate_temporal(self, intervals:List[List],reducer,labels:List = None, dimension:str = None,context:Dict=None) -> 'ImageCollection' :
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

    @deprecated("Use a more specific reduce method instead.")
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

    def ndvi(self, **kwargs) -> 'ImageCollection':
        """ Normalized Difference Vegetation Index (NDVI)

            :param kwargs:

            :return: An ImageCollection instance
        """
        pass

    def rename_labels(self, dimension: str, target: list, source: list=None) -> 'ImageCollection':
        """ Renames the labels of the specified dimension in the data cube from source to target.

            :param dimension: Dimension name
            :param target: The new names for the labels.
            :param source: The names of the labels as they are currently in the data cube.

            :return: An ImageCollection instance
        """
        pass

    def rename_dimension(self, source:str, target:str):
        """
        Renames a dimension in the data cube while preserving all other properties.

        :param source: The current name of the dimension. Fails with a DimensionNotAvailable error if the specified dimension does not exist.
        :param target: A new Name for the dimension. Fails with a DimensionExists error if a dimension with the specified name exists.

        :return: A new datacube with the dimension renamed.
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

    def merge(self, other: 'ImageCollection') -> 'ImageCollection':
        """
        Merge the bands of this data cubes with the bands of another datacube. The bands of 'other' will be appended to the bands
        of this datacube, maintaining the order.

        :param other: The other datacube to merge with this datacube
        :return: A new datacube with bands merged.
        """
        pass

    def apply_kernel(self, kernel, factor=1.0, border = 0, replace_invalid=0) -> 'ImageCollection':
        """
        Applies a focal operation based on a weighted kernel to each value of the specified dimensions in the data cube.

        The border parameter determines how the data is extended when the kernel overlaps with the borders.
        The following options are available:

        * numeric value - fill with a user-defined constant number n: nnnnnn|abcdefgh|nnnnnn (default, with n = 0)
        * replicate - repeat the value from the pixel at the border: aaaaaa|abcdefgh|hhhhhh
        * reflect - mirror/reflect from the border: fedcba|abcdefgh|hgfedc
        * reflect_pixel - mirror/reflect from the center of the pixel at the border: gfedcb|abcdefgh|gfedcb
        * wrap - repeat/wrap the image: cdefgh|abcdefgh|abcdef


        :param kernel: The kernel to be applied on the data cube. The kernel has to be as many dimensions as the data cube has dimensions.
        :param factor: A factor that is multiplied to each value computed by the focal operation. This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
        :param border: Determines how the data is extended when the kernel overlaps with the borders. Defaults to fill the border with zeroes.
        :param: replace_invalid: This parameter specifies the value to replace non-numerical or infinite numerical values with. By default, those values are replaced with zeroes.
        :return: A data cube with the newly computed values. The resolution, cardinality and the number of dimensions are the same as for the original data cube.
        """
        pass

    def raster_to_vector(self) -> 'VectorCube':
        """
        EXPERIMENTAL: not generally supported, API subject to change
        Converts this raster data cube into a vector data cube. The bounding polygon of homogenous areas of pixels is constructed.


        @return: A vectorcube
        """
        pass

    ####VIEW methods #######

    @deprecated(reason="use aggregate_spatial instead")
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

    def tiled_viewing_service(self, **kwargs) -> Dict:
        """
        Returns metadata for a tiled viewing service that visualizes this layer.

        :param process_graph: process graph dict
        :param service_type: The type of viewing service to create, for instance: 'WMTS'

        :return: A dictionary object containing the viewing service metadata, such as the connection 'url'.
        """
        pass

    def execute_batch(
            self,
            outputfile: Union[str, pathlib.Path], out_format: str = None,
            print=print, max_poll_interval=60, connection_retry_interval=30,
            job_options=None, **format_options) -> Job:
        """
        Evaluate the process graph by creating a batch job, and retrieving the results when it is finished.
        This method is mostly recommended if the batch job is expected to run in a reasonable amount of time.

        For very long running jobs, you probably do not want to keep the client running. In that case, using
        :func:`~openeo.imagecollection.ImageCollection.send_job` might be more appropriate.

        :param job_options: A dictionary containing (custom) job options
        :param outputfile: The path of a file to which a result can be written
        :param out_format: (optional) Format of the job result.
        :param format_options: String Parameters for the job result format

        """
        pass

    def send_job(self, out_format:str=None, job_options:Dict=None, **format_options) -> Job:
        """
        Sends a job to the backend and returns a Job instance. The job will still need to be started and managed explicitly.
        The :func:`~openeo.imagecollection.ImageCollection.execute_batch` method allows you to run batch jobs without managing it.

        :param out_format: String Format of the job result.
        :param job_options: A dictionary containing (custom) job options
        :param format_options: String Parameters for the job result format
        :return: status: Job resulting job.
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
