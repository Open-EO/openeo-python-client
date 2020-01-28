def ln(cube, x):
    """The natural logarithm is the logarithm to the base *e* of the number `x`. This
process is an alias for the *log* process with the base set to *e*: `log(x,
e())`. The natural logarithm is the inverse function of taking *e* to the power
x.The computations should follow [IEEE Standard
754](https://ieeexplore.ieee.org/document/4610935) so that for example `ln(0)`
should result in ±infinity if the processing environment supports it. Otherwise
an exception must the thrown for incomputable results.The no-data value `null`
is passed through and therefore gets propagated.
  :param x: A number to compute the natural logarithm for.

  :return: The computed natural logarithm.
"""
    return cube.graph_add_process('ln', {"x": x})


def cos(cube, x):
    """Computes the cosine of `x`.Works on radians only.The no-data value `null` is
passed through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed cosine of `x`.
"""
    return cube.graph_add_process('cos', {"x": x})


def lt(cube, x, y):
    """Compares whether `x` is strictly less than `y`.**Remarks:*** If any of the
operands is `null`, the return value is `null`.* Temporal strings can *not* be
compared based on their string representation due to the time zone / time-offset
representations.* Comparing strings is currently not supported, but is planned
to be added in the future.
  :param x: 
  :param y: 

  :return: `true` if `x` is strictly less than `y`, `null` if any of the operands is
`null`, otherwise `false`.
"""
    return cube.graph_add_process('lt', {"x": x, "y": y})


def filter_polygon(cube, polygons, from_node=None):
    """Limits the data cube over the spatial dimensions to the specified polygons.The
filter retains a pixel in the data cube if the point at the pixel center
intersects with at least one of the polygons (as defined in the Simple Features
standard by the OGC).
  :param data: 
  :param polygons: 

  :return: A data cube restricted to the specified polygons. Therefore, the cardinality is
potentially lower, but the resolution and the number of dimensions are the same
as for the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('filter_polygon', {"data": {"from_node": from_node}, "polygons": polygons})


def arccos(cube, x):
    """Computes the arc cosine of `x`. The arc cosine is the inverse function of the
cosine so that *arccos(cos(x)) = x*.Works on radians only.The no-data value
`null` is passed through and therefore gets propagated.
  :param x: A number.

  :return: The computed angle in radians.
"""
    return cube.graph_add_process('arccos', {"x": x})


def merge_cubes(cube, cube2, cube1, binary, overlap_resolver):
    """The data cubes have to be compatible. A merge is the inverse of a split if there
is no overlap. If data overlaps the parameter `overlap_resolver` must be
specified to resolve the overlap. It doesn't add dimensions.
  :param cube2: 
  :param cube1: 
  :param binary: 
  :param overlap_resolver: 

  :return: The merged data cube.
"""
    return cube.graph_add_process('merge_cubes', {"cube2": cube2, "cube1": cube1, "binary": binary, "overlap_resolver": overlap_resolver})


def sqrt(cube, x):
    """Computes the square root of a real number `x`. This process is an alias for `x`
to the power of *0.5*: `power(x, 0.5)`.A square root of x is a number a such
that *a^2^ = x*. Therefore, the square root is the inverse function of a to the
power of 2, but only for *a >= 0*.The no-data value `null` is passed through and
therefore gets propagated.
  :param x: A number.

  :return: The computed square root.
"""
    return cube.graph_add_process('sqrt', {"x": x})


def gte(cube, x, y):
    """Compares whether `x` is greater than or equal to `y`.**Remarks:*** If any of the
operands is `null`, the return value is `null`.* Temporal strings can *not* be
compared based on their string representation due to the time zone / time-offset
representations.* Comparing strings is currently not supported, but is planned
to be added in the future.
  :param x: 
  :param y: 

  :return: `true` if `x` is greater than or equal to `y` or `null` if any of the operands
is `null`, otherwise `false`.
"""
    return cube.graph_add_process('gte', {"x": x, "y": y})


def neq(cube, case_sensitive, x, delta, y):
    """Compares whether `x` is *not* strictly equal to `y`. This process is an alias
for: `not(eq(val1, val2))`.**Remarks:*** Data types MUST be checked strictly,
for example a string with the content *1* is not equal to the number *1*.
Nevertheless, an integer *1* is equal to a floating point number *1.0* as
`integer` is a sub-type of `number`.* If any of the operands is `null`, the
return value is `null`.* Strings are expected to be encoded in UTF-8 by
default.* Temporal strings MUST be compared differently than other strings and
MUST NOT be compared based on their string representation due to different
possible representations. For example, the UTC time zone representation `Z` has
the same meaning as `+00:00`.
  :param case_sensitive: 
  :param x: 
  :param delta: 
  :param y: 

  :return: Returns `true` if `x` is *not* equal to `y`, `null` if any of the operands is
`null`, otherwise `false`.
"""
    return cube.graph_add_process('neq', {"case_sensitive": case_sensitive, "x": x, "delta": delta, "y": y})


def max_time(cube, from_node=None):
    """Finds the maximum value of a time series for every given pixel location for all
bands.
  :param data: EO data to process.

  :return: Processed EO data.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('max_time', {"data": {"from_node": from_node}})


def exp(cube, p):
    """Exponential function to the base *e* raised to the power of `p`. This process is
an alias for *e^p^* / `power(e(), p)`.The no-data value `null` is passed through
and therefore gets propagated.
  :param p: The numerical exponent.

  :return: The computed value for *e* raised to the power of `p`.
"""
    return cube.graph_add_process('exp', {"p": p})


def reduce(cube, target_dimension, binary, reducer, dimension, from_node=None):
    """Applies a reducer to a data cube dimension by collapsing all the input values
along the specified dimension into an output value computed by the reducer.The
reducer must be a callable process (or a set of processes as process graph) that
accepts by default array as input. The process can also work on two values by
setting the parameter `binary` to `true`. The reducer must compute a single or
multiple return values of the same type as the input values were. Multiple
values must be wrapped in an array. An example for a process returning a single
value is ``median()``. In this case the specified dimension would be removed. If
a callback such as ``extrema()`` returns multiple values, a new dimension with
the specified name in `target_dimension` is created (see the description of the
parameter for more information).A special case is that the reducer can be set to
`null`, which is the default if no reducer is specified. It acts as a no-
operation reducer so that the remaining value is treated like a reduction result
and the dimension gets dropped. This only works on dimensions with a single
dimension value left (e.g. after filtering for a single band), otherwise the
process fails with a `TooManyDimensionValues` error.Nominal values can be
reduced too, but need to be mapped. For example date strings to numeric
timestamps since 1970 etc.
  :param target_dimension: 
  :param data: 
  :param binary: 
  :param reducer: 
  :param dimension: 

  :return: A data cube with the newly computed values. The number of dimensions is reduced
for callbacks returning a single value or doesn't change if the callback returns
multiple values. The resolution and cardinality are the same as for the original
data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('reduce', {"target_dimension": target_dimension, "data": {"from_node": from_node}, "binary": binary, "reducer": reducer, "dimension": dimension})


def tan(cube, x):
    """Computes the tangent of `x`. The tangent is defined to be the sine of x divided
by the cosine of x.Works on radians only.The no-data value `null` is passed
through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed tangent of `x`.
"""
    return cube.graph_add_process('tan', {"x": x})


def array_element(cube, return_nodata, index, from_node=None):
    """Returns the element at the specified index from the array.
  :param return_nodata: 
  :param data: 
  :param index: 

  :return: The value of the requested element.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('array_element', {"return_nodata": return_nodata, "data": {"from_node": from_node}, "index": index})


def sinh(cube, x):
    """Computes the hyperbolic sine of `x`.Works on radians only.The no-data value
`null` is passed through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed hyperbolic sine of `x`.
"""
    return cube.graph_add_process('sinh', {"x": x})


def subtract(cube, ignore_nodata, from_node=None):
    """Takes the first element of a sequential array of numbers and subtracts all other
elements from it.The computations should follow [IEEE Standard
754](https://ieeexplore.ieee.org/document/4610935) whenever the processing
environment supports it. Otherwise an exception must the thrown for incomputable
results.By default no-data values are ignored. Setting `ignore_nodata` to
`false` considers no-data values so that `null` is returned if any element is
such a value.
  :param data: 
  :param ignore_nodata: 

  :return: The computed result of the sequence of numbers.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('subtract', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def count(cube, expression, from_node=None):
    """Gives the number of elements in an array that matches a certain criterion /
expression.**Remarks:*** By default counts the number of valid elements. A valid
element is every element for which ``is_valid()`` returns `true`.* To count all
elements in a list set the `expression` parameter to boolean `true`.
  :param expression: 
  :param data: 

  :return: The counted number of elements.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('count', {"expression": expression, "data": {"from_node": from_node}})


def eq(cube, case_sensitive, x, delta, y):
    """Compares whether `x` is strictly equal to `y`.**Remarks:*** Data types MUST be
checked strictly, for example a string with the content *1* is not equal to the
number *1*. Nevertheless, an integer *1* is equal to a floating point number
*1.0* as `integer` is a sub-type of `number`.* If any of the operands is `null`,
the return value is `null`.* Strings are expected to be encoded in UTF-8 by
default.* Temporal strings MUST be compared differently than other strings and
MUST NOT be compared based on their string representation due to different
possible representations. For example, the UTC time zone representation `Z` has
the same meaning as `+00:00`.
  :param case_sensitive: 
  :param x: 
  :param delta: 
  :param y: 

  :return: Returns `true` if `x` is equal to `y`, `null` if any of the operands is `null`,
otherwise `false`.
"""
    return cube.graph_add_process('eq', {"case_sensitive": case_sensitive, "x": x, "delta": delta, "y": y})


def save_result(cube, format, options, from_node=None):
    """Saves processed data to the local user workspace / data store of the
authenticated user. This process aims to be compatible to GDAL/OGR formats and
options. STAC-compatible metadata should be stored with the processed
data.Calling this process may be rejected by back-ends in the context of
secondary web services.
  :param data: 
  :param format: 
  :param options: 

  :return: `false` if saving failed, `true` otherwise.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('save_result', {"data": {"from_node": from_node}, "format": format, "options": options})


def filter_temporal(cube, extent, dimension, from_node=None):
    """Limits the data cube to the specified interval of dates and/or times.More
precisely, the filter checks whether the temporal dimension value is greater
than or equal to the lower boundary (start date/time) and the temporal dimension
value is less than the value of the upper boundary (end date/time). This
corresponds to a left-closed interval, which contains the lower boundary but not
the upper boundary.If the dimension is set to `null` (it's the default value),
the data cube is expected to only have one temporal dimension.
  :param extent: 
  :param data: 
  :param dimension: 

  :return: A data cube restricted to the specified temporal extent. Therefore, the
cardinality is potentially lower, but the resolution and the number of
dimensions are the same as for the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('filter_temporal', {"extent": extent, "data": {"from_node": from_node}, "dimension": dimension})


def arcsin(cube, x):
    """Computes the arc sine of `x`. The arc sine is the inverse function of the sine
so that *arcsin(sin(x)) = x*.Works on radians only.The no-data value `null` is
passed through and therefore gets propagated.
  :param x: A number.

  :return: The computed angle in radians.
"""
    return cube.graph_add_process('arcsin', {"x": x})


def resample_spatial(cube, method, projection, align, resolution, from_node=None):
    """Resamples the spatial dimensions (x,y) of the data cube to a specified
resolution and/or warps the data cube to the target projection. At least
`resolution` or `projection` must be specified.Use ``filter_bbox()`` to set the
target spatial extent.
  :param data: 
  :param method: 
  :param projection: 
  :param align: 
  :param resolution: 

  :return: A raster data cube with values warped onto the new projection.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('resample_spatial', {"data": {"from_node": from_node}, "method": method, "projection": projection, "align": align, "resolution": resolution})


def log(cube, x, base):
    """Logarithm to the base `base` of the number `x` is defined to be the inverse
function of taking b to the power of x.The computations should follow [IEEE
Standard 754](https://ieeexplore.ieee.org/document/4610935) so that for example
`log(0, 2)` should result in ±infinity if the processing environment supports
it. Otherwise an exception must the thrown for incomputable results.The no-data
value `null` is passed through and therefore gets propagated if any of the
arguments is `null`.
  :param x: 
  :param base: 

  :return: The computed logarithm.
"""
    return cube.graph_add_process('log', {"x": x, "base": base})


def resample_cube_spatial(cube, method, target, from_node=None):
    """Resamples the spatial dimensions (x,y) from a source data cube to a target data
cube and return the results as a new data cube.
  :param data: 
  :param method: 
  :param target: 

  :return: A data cube with potentially lower spatial resolution and potentially lower
cardinality, but the same number of dimensions as the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('resample_cube_spatial', {"data": {"from_node": from_node}, "method": method, "target": target})


def filter_bands(cube, common_names, wavelengths, bands, from_node=None):
    """Filters the bands in the data cube so that bands that don't match any of the
criteria are dropped from the data cube. The data cube is expected to have only
one dimension of type `bands`. Fails with a `DimensionMissing` error if no such
dimension exists.The following criteria can be used to select bands:* `bands`:
band name (e.g. `B01` or `B8A`)* `common_names`: common band names (e.g. `red`
or `nir`)* `wavelengths`: ranges of wavelengths in micrometres (?m) (e.g. 0.5 -
0.6)To keep algorithms interoperable it is recommended to prefer the common
bands names or the wavelengths over collection and/or back-end specific band
names.If multiple criteria are specified, any of them must match and not all of
them, i.e. they are combined with an OR-operation. If no criteria is specified,
the `BandFilterParameterMissing` exception must be thrown.**Important:** The
order of the specified array defines the order of the bands in the data cube,
which can be important for subsequent processes. If multiple bands are matched
by a single criterion (e.g. a range of wavelengths), they are ordered
alphabetically by band names. Bands without names have an arbitrary order.
  :param data: 
  :param common_names: 
  :param wavelengths: 
  :param bands: 

  :return: A data cube limited to a subset of its original bands. Therefore, the
cardinality is potentially lower, but the resolution and the number of
dimensions are the same as for the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('filter_bands', {"data": {"from_node": from_node}, "common_names": common_names, "wavelengths": wavelengths, "bands": bands})


def sum(cube, ignore_nodata, from_node=None):
    """Sums up all elements in a sequential array of numbers and returns the computed
sum.The computations should follow [IEEE Standard
754](https://ieeexplore.ieee.org/document/4610935) whenever the processing
environment supports it. Otherwise an exception must the thrown for incomputable
results.By default no-data values are ignored. Setting `ignore_nodata` to
`false` considers no-data values so that `null` is returned if any element is
such a value.
  :param data: 
  :param ignore_nodata: 

  :return: The computed sum of the sequence of numbers.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('sum', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def load_collection(cube, temporal_extent, spatial_extent, id, bands, properties):
    """Loads a collection from the current back-end by its id and returns it as
processable data cube. The data that is added to the data cube can be restricted
with the additional `spatial_extent`, `temporal_extent`, `bands` and
`properties`.**Remarks:*** The bands (and all dimensions that specify nominal
dimension values) are expected to be ordered as specified in the metadata if the
`bands` parameter is set to `null`.* If no additional parameter is specified
this would imply that the whole data set is expected to be loaded. Due to the
large size of many data sets this is not recommended and may be optimized by
back-ends to only load the data that is actually required after evaluating
subsequent processes such as filters. This means that the pixel values should be
processed only after the data has been limited to the required extents and as a
consequence also to a manageable size.
  :param temporal_extent: 
  :param spatial_extent: 
  :param id: 
  :param bands: 
  :param properties: 

  :return: A data cube for further processing.
"""
    return cube.graph_add_process('load_collection', {"temporal_extent": temporal_extent, "spatial_extent": spatial_extent, "id": id, "bands": bands, "properties": properties})


def tanh(cube, x):
    """Computes the hyperbolic tangent of `x`. The tangent is defined to be the
hyperbolic sine of x divided by the hyperbolic cosine of x.Works on radians
only.The no-data value `null` is passed through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed hyperbolic tangent of `x`.
"""
    return cube.graph_add_process('tanh', {"x": x})


def _not(cube, expression):
    """Inverts a single boolean so that `true` gets `false` and `false` gets `true`.The
no-data value `null` is passed through and therefore gets propagated.
  :param expression: Boolean value to invert.

  :return: Inverted boolean value.
"""
    return cube.graph_add_process('_not', {"expression": expression})


def min(cube, ignore_nodata, from_node=None):
    """Computes the smallest value of an array of numbers, which is is equal to the
last element of a sorted (i.e., ordered) version the array.
  :param data: 
  :param ignore_nodata: 

  :return: The minimum value.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('min', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def linear_scale_range(cube, inputMax, inputMin, x, outputMin, outputMax):
    """Performs a linear transformation between the input and output range.The
underlying formula is: `((x - inputMin) / (inputMax - inputMin)) * (outputMax -
outputMin) + outputMin`.Potential use case include* scaling values to the 8-bit
range (0 - 255) often used for numeric representation of values in one of the
channels of the [RGB colour
model](https://en.wikipedia.org/wiki/RGB_color_model#Numeric_representations)
or* calculating percentages (0 - 100).The no-data value `null` is passed through
and therefore gets propagated.
  :param inputMax: 
  :param inputMin: 
  :param x: 
  :param outputMin: 
  :param outputMax: 

  :return: The transformed number.
"""
    return cube.graph_add_process('linear_scale_range', {"inputMax": inputMax, "inputMin": inputMin, "x": x, "outputMin": outputMin, "outputMax": outputMax})


def linear_stretch_cube(cube, min, max, from_node=None):
    """Performs a linear transformation between the input and output range.The
underlying formula is: `((x - inputMin) / (inputMax - inputMin)) * (outputMax -
outputMin) + outputMin`.Potential use case include* scaling values to the 8-bit
range (0 - 255) often used for numeric representation of values in one of the
channels of the [RGB colour
model](https://en.wikipedia.org/wiki/RGB_color_model#Numeric_representations)
or* calculating percentages (0 - 100).The no-data value `null` is passed through
and therefore gets propagated.
  :param min: 
  :param data: 
  :param max: 

  :return: A raster data cube with every value in the cube scaled according to the min and
max parameters.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('linear_stretch_cube', {"min": min, "data": {"from_node": from_node}, "max": max})


def _and(cube, ignore_nodata, expressions):
    """Checks if **all** of the values are true. Evaluates each expression from the
first to the last element and stops once the outcome is unambiguous.If only one
value is given the process evaluates to the given value. If no value is given
(i.e. the array is empty) the process returns `null`.By default all no-data
values are ignored so that the process returns `true` if all other values are
true and otherwise returns `false`.Setting the `ignore_nodata` flag to `false`
considers no-data values so that `null` is a valid logical object. If a
component is `null`, the result will be `null` if the outcome is ambiguous. See
the following truth table:```      || null  | false | true----- || ----- | -----
| -----null  || null  | false | nullfalse || false | false | falsetrue  || null
| false | true```
  :param ignore_nodata: 
  :param expressions: 

  :return: Boolean result of the logical expressions.
"""
    return cube.graph_add_process('_and', {"ignore_nodata": ignore_nodata, "expressions": expressions})


def sin(cube, x):
    """Computes the sine of `x`.Works on radians only.The no-data value `null` is
passed through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed sine of `x`.
"""
    return cube.graph_add_process('sin', {"x": x})


def xor(cube, ignore_nodata, expressions):
    """Checks if **exactly one** of the values is true. Evaluates each expression from
the first to the last element and stops once the outcome is unambiguous.If only
one value is given the process evaluates to the given value. If no value is
given (i.e. the array is empty) the process returns `null`.By default all no-
data values are ignored so that the process returns `true` if exactly one of the
other values is true and otherwise returns `false`.Setting the `ignore_nodata`
flag to `false` considers no-data values so that `null` is a valid logical
object. If a component is `null`, the result will be `null` if the outcome is
ambiguous. See the following truth table:```      || null | false | true----- ||
---- | ----- | -----null  || null | null  | nullfalse || null | false | truetrue
|| null | true  | false```
  :param ignore_nodata: 
  :param expressions: 

  :return: Boolean result of the logical expressions.
"""
    return cube.graph_add_process('xor', {"ignore_nodata": ignore_nodata, "expressions": expressions})


def divide(cube, ignore_nodata, from_node=None):
    """Divides the first element in a sequential array of numbers by all other
elements.The computations should follow [IEEE Standard
754](https://ieeexplore.ieee.org/document/4610935) so that for example a
division by zero should result in ±infinity if the processing environment
supports it. Otherwise an exception must the thrown for incomputable results.By
default no-data values are ignored. Setting `ignore_nodata` to `false` considers
no-data values so that `null` is returned if any element is such a value.
  :param data: 
  :param ignore_nodata: 

  :return: The computed result of the sequence of numbers.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('divide', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def ndvi(cube, name, from_node=None):
    """Computes the Normalized Difference Vegetation Index (NDVI). The NDVI is computed
as *(nir - red) / (nir + red)*.The `data` parameter expects a raster data cube
with two bands that have the common names `red` and `nir` assigned. The process
returns a raster data cube with two bands being replaced with a new band that
holds the computed values. The newly created band is named `ndvi` by default.
This name can be changed with the `name` parameter.This process is very similar
to the process ``normalized_difference()``, but determines the bands
automatically based on the common name (`red`/`nir`) specified in the metadata.
  :param data: 
  :param name: 

  :return: A raster data cube with the two bands being replaced with a new band that holds
the computed values.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('ndvi', {"data": {"from_node": from_node}, "name": name})


def power(cube, p, base):
    """Computes the exponentiation for the base `base` raised to the power of `p`.The
no-data value `null` is passed through and therefore gets propagated if any of
the arguments is `null`.
  :param p: 
  :param base: 

  :return: The computed value for `base` raised to the power of `p`.
"""
    return cube.graph_add_process('power', {"p": p, "base": base})


def lte(cube, x, y):
    """Compares whether `x` is less than or equal to `y`.**Remarks:*** If any of the
operands is `null`, the return value is `null`.* Temporal strings can *not* be
compared based on their string representation due to the time zone / time-offset
representations.* Comparing strings is currently not supported, but is planned
to be added in the future.
  :param x: 
  :param y: 

  :return: `true` if `x` is less than or equal to `y`, `null` if any of the operands is
`null`, otherwise `false`.
"""
    return cube.graph_add_process('lte', {"x": x, "y": y})


def product(cube, ignore_nodata, from_node=None):
    """This process is an exact alias for the `multiply` process. See ``multiply()``
for more information.
  :param data: 
  :param ignore_nodata: 

  :return: See ``multiply()`` for more information.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('product', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def _or(cube, ignore_nodata, expressions):
    """Checks if **at least one** of the values is true. Evaluates each expression from
the first to the last element and stops once the outcome is unambiguous.If only
one value is given the process evaluates to the given value. If no value is
given (i.e. the array is empty) the process returns `null`.By default all no-
data values are ignored so that the process returns `true` if at least one of
the other values is true and otherwise returns `false`.Setting the
`ignore_nodata` flag to `false` considers no-data values so that `null` is a
valid logical object. If a component is `null`, the result will be `null` if the
outcome is ambiguous. See the following truth table:```      || null | false |
true----- || ---- | ----- | ----null  || null | null  | truefalse || null |
false | truetrue  || true | true  | true```
  :param ignore_nodata: 
  :param expressions: 

  :return: Boolean result of the logical expressions.
"""
    return cube.graph_add_process('_or', {"ignore_nodata": ignore_nodata, "expressions": expressions})


def mask_colored(cube, red, upperThreshold, green, blue, lowerThreshold, from_node=None):
    """Applies a mask to a raster data cube. A mask can either be specified as:*
**Raster data cube** for which parallel pixels among `data` and `mask` are
compared and those pixels in `data` are replaced, which are non-zero (for
numbers) or `true` (for boolean values) in `mask`.* **GeoJSON or vector data
cube** containing one or more polygons. All pixels for which the point at the
pixel center intersects with the corresponding polygon (as defined in the Simple
Features standard by the OGC) are replaced.The pixel values are replaced with
the value specified for `replacement`, which defaults to `null` (no data). No
data values will be left untouched by the masking operation.
  :param red: 
  :param upperThreshold: 
  :param green: 
  :param data: 
  :param blue: 
  :param lowerThreshold: 

  :return: The masked raster data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('mask_colored', {"red": red, "upperThreshold": upperThreshold, "green": green, "data": {"from_node": from_node}, "blue": blue, "lowerThreshold": lowerThreshold})


def e(cube):
    """The real number *e* is a mathematical constant that is the base of the natural
logarithm such that *ln(e) = 1*. The numerical value is approximately *2.71828*.

  :return: The numerical value of Euler's number.
"""
    return cube.graph_add_process('e', None)


def max(cube, ignore_nodata, from_node=None):
    """Computes the largest value of an array of numbers, which is is equal to the
first element of a sorted (i.e., ordered) version the array.
  :param data: 
  :param ignore_nodata: 

  :return: The maximum value.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('max', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def apply(cube, process, from_node=None):
    """Applies a **unary** process which takes a single value such as `abs` or `sqrt`
to each pixel value in the data cube (i.e. a local operation). In contrast, the
process ``apply_dimension()`` applies an n-ary process to a particular
dimension.
  :param process: 
  :param data: 

  :return: A data cube with the newly computed values. The resolution, cardinality and the
number of dimensions are the same as for the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('apply', {"process": process, "data": {"from_node": from_node}})


def run_udf(cube, udf, context, runtime, version, from_node=None):
    """Runs an UDF in one of the supported runtime environments.The process can
either:1. load and run a locally stored UDF from a file in the workspace of the
authenticated user. The path to the UDF file must be relative to the root
directory of the user's workspace.2. fetch and run a remotely stored and
published UDF by absolute URI, for example from [openEO
Hub](https://hub.openeo.org)).3. run the source code specified inline as
string.The loaded UDF can be executed in several processes such as
``aggregate_temporal()``, ``apply()``, ``apply_dimension()``, ``filter()`` and
``reduce()``. In this case an array is passed instead of a raster data cube. The
user must ensure that the data is properly passed as an array so that the UDF
can make sense of it.
  :param data: 
  :param udf: 
  :param context: 
  :param runtime: 
  :param version: 

  :return: The data processed by the UDF. Returns a raster data cube if a raster data cube
was passed for `data`. If an array was passed for `data`, the returned value is
defined by the context and is exactly what the UDF returned.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('run_udf', {"data": {"from_node": from_node}, "udf": udf, "context": context, "runtime": runtime, "version": version})


def linear_scale_cube(cube, inputMax, inputMin, outputMin, outputMax, from_node=None):
    """Performs a linear transformation between the input and output range.The
underlying formula is: `((x - inputMin) / (inputMax - inputMin)) * (outputMax -
outputMin) + outputMin`.Potential use case include* scaling values to the 8-bit
range (0 - 255) often used for numeric representation of values in one of the
channels of the [RGB colour
model](https://en.wikipedia.org/wiki/RGB_color_model#Numeric_representations)
or* calculating percentages (0 - 100).The no-data value `null` is passed through
and therefore gets propagated.
  :param data: 
  :param inputMax: 
  :param inputMin: 
  :param outputMin: 
  :param outputMax: 

  :return: A raster data cube with every value in the cube scaled according to the min and
max parameters.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('linear_scale_cube', {"data": {"from_node": from_node}, "inputMax": inputMax, "inputMin": inputMin, "outputMin": outputMin, "outputMax": outputMax})


def gt(cube, x, y):
    """Compares whether `x` is strictly greater than `y`.**Remarks:*** If any of the
operands is `null`, the return value is `null`.* Temporal strings can *not* be
compared based on their string representation due to the time zone / time-offset
representations.* Comparing strings is currently not supported, but is planned
to be added in the future.
  :param x: 
  :param y: 

  :return: `true` if `x` is strictly greater than `y` or `null` if any of the operands is
`null`, otherwise `false`.
"""
    return cube.graph_add_process('gt', {"x": x, "y": y})


def min_time(cube, from_node=None):
    """Finds the minimum value of a time series for every given pixel location for all
bands.
  :param data: EO data to process.

  :return: Processed EO data.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('min_time', {"data": {"from_node": from_node}})


def cosh(cube, x):
    """Computes the hyperbolic cosine of `x`.Works on radians only.The no-data value
`null` is passed through and therefore gets propagated.
  :param x: An angle in radians.

  :return: The computed hyperbolic cosine of `x`.
"""
    return cube.graph_add_process('cosh', {"x": x})


def arctan(cube, x):
    """Computes the arc tangent of `x`. The arc tangent is the inverse function of the
tangent so that *arctan(tan(x)) = x*.Works on radians only.The no-data value
`null` is passed through and therefore gets propagated.
  :param x: A number.

  :return: The computed angle in radians.
"""
    return cube.graph_add_process('arctan', {"x": x})


def absolute(cube, x):
    """Computes the absolute value of a real number `x`, which is the "unsigned"
portion of x and often denoted as *|x|*.The no-data value `null` is passed
through and therefore gets propagated.
  :param x: A number.

  :return: The computed absolute value.
"""
    return cube.graph_add_process('absolute', {"x": x})


def mean(cube, ignore_nodata, from_node=None):
    """The arithmetic mean of an array of numbers is the quantity commonly called the
average. It is defined as the sum of all elements divided by the number of
elements.
  :param data: 
  :param ignore_nodata: 

  :return: The computed arithmetic mean.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('mean', {"data": {"from_node": from_node}, "ignore_nodata": ignore_nodata})


def normalized_difference(cube, name, band1, band2):
    """Computes the normalized difference for two bands. The normalized difference is
computed as *(band1 - band2) / (band1 + band2)*.Each of the parameters expects a
raster data cube with exactly one band. The process returns a raster data cube
with exactly one band that holds the computed values. The newly created band is
named `normalized_difference` by default. This name can be changed with the
`name` parameter.This process could be used for a number of remote sensing
indices such as:* [NDVI](https://eos.com/ndvi/)* [NDWI](https://eos.com/ndwi/)*
[NDSI](https://eos.com/ndsi/)Please note that some back-ends may have native
processes available for convenience such as the ``ndvi()``.
  :param name: 
  :param band1: 
  :param band2: 

  :return: A raster data cube with exactly one band that holds the computed values.
"""
    return cube.graph_add_process('normalized_difference', {"name": name, "band1": band1, "band2": band2})


def pi(cube):
    """The real number Pi (π) is a mathematical constant that is the ratio of the
circumference of a circle to its diameter. The numerical value is approximately
*3.14159*.

  :return: The numerical value of Pi.
"""
    return cube.graph_add_process('pi', None)


def filter_bbox(cube, extent, from_node=None):
    """Limits the data cube to the specified bounding box.The filter retains a pixel in
the data cube if the point at the pixel center intersects with the bounding box
(as defined in the Simple Features standard by the OGC).
  :param extent: 
  :param data: 

  :return: A data cube restricted to the bounding box. Therefore, the cardinality is
potentially lower, but the resolution and the number of dimensions are the same
as for the original data cube.
"""
    if not from_node:
        from_node = cube.node_id
    return cube.graph_add_process('filter_bbox', {"extent": extent, "data": {"from_node": from_node}})


