"""

"""

# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

import functools
import importlib
import inspect
import logging
import math
import pathlib
from typing import Callable, Union

import numpy
import pandas
import shapely
import xarray
from pandas import Series

from openeo.udf import OpenEoUdfException
from openeo.udf.feature_collection import FeatureCollection
from openeo.udf.structured_data import StructuredData
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube

_log = logging.getLogger(__name__)


def _build_default_execution_context():
    # TODO: is it really necessary to "pre-load" these modules? Isn't user going to import them explicitly in their script anyway?
    context = {
        "numpy": numpy, "np": numpy,
        "xarray": xarray,
        "pandas": pandas, "pd": pandas,
        "shapely": shapely,
        "math": math,
        "UdfData": UdfData,
        "XarrayDataCube": XarrayDataCube,
        "DataCube": XarrayDataCube,  # Legacy alias
        "StructuredData": StructuredData,
        "FeatureCollection": FeatureCollection,
        # "SpatialExtent": SpatialExtent,  # TODO?
        # "MachineLearnModel": MachineLearnModelConfig, # TODO?
    }
    for name in ["geopandas", "torch", "torchvision", "tensorflow", "tensorboard"]:
        try:
            context[name] = importlib.import_module(name)
        except ImportError:
            _log.info("Module {m} not available for UDF execution context".format(m=name))

    return context


@functools.lru_cache(maxsize=100)
def load_module_from_string(code: str):
    """
    Experimental: avoid loading same UDF module more than once, to make caching inside the udf work.
    @param code:
    @return:
    """
    globals = _build_default_execution_context()
    exec(code, globals)
    return globals


def _get_annotation_str(annotation: Union[str, type]) -> str:
    """Get parameter annotation as a string"""
    if isinstance(annotation, str):
        return annotation
    elif isinstance(annotation, type):
        mod = annotation.__module__
        return (mod + "." if mod != str.__module__ else "") + annotation.__name__
    else:
        return str(annotation)


def _annotation_is_pandas_series(annotation) -> bool:
    return annotation in {pandas.Series, _get_annotation_str(pandas.Series)}


def _annotation_is_udf_datacube(annotation) -> bool:
    return annotation is XarrayDataCube or _get_annotation_str(annotation) in {
        _get_annotation_str(XarrayDataCube),
        'openeo_udf.api.datacube.DataCube',  # Legacy `openeo_udf` annotation
    }


def _annotation_is_udf_data(annotation) -> bool:
    return annotation is UdfData or _get_annotation_str(annotation) in {
        _get_annotation_str(UdfData),
        'openeo_udf.api.udf_data.UdfData'  # Legacy `openeo_udf` annotation
    }


def _apply_timeseries_xarray(array: xarray.DataArray, callback: Callable[[Series], Series]) -> xarray.DataArray:
    """
    Apply timeseries callback to given xarray data array
    along its time dimension (named "t" or "time")

    :param array: array to transform
    :param callback: function that transforms a timeseries in another (same size)
    :return: transformed array
    """
    # Make time dimension the last one, and flatten the rest
    # to create a 1D sequence of input time series (also 1D).
    [time_position] = [i for (i, d) in enumerate(array.dims) if d in ["t", "time"]]
    input_series = numpy.moveaxis(array.values, time_position, -1)
    orig_shape = input_series.shape
    input_series = input_series.reshape((-1, input_series.shape[-1]))

    applied = numpy.asarray([callback(s) for s in input_series])

    # Reshape to original shape
    applied = applied.reshape(orig_shape)
    applied = numpy.moveaxis(applied, -1, time_position)
    assert applied.shape == array.shape

    return xarray.DataArray(applied, coords=array.coords, dims=array.dims, name=array.name)


def apply_timeseries_generic(
        udf_data: UdfData,
        callback: Callable[[Series, dict], Series]
) -> UdfData:
    """
    Implements the UDF contract by calling a user provided time series transformation function.

    :param udf_data:
    :param callback: callable that takes a pandas Series and context dict and returns a pandas Series.
        See template :py:func:`openeo.udf.udf_signatures.apply_timeseries`
    :return:
    """
    callback = functools.partial(callback, context=udf_data.user_context)
    datacubes = [
        XarrayDataCube(_apply_timeseries_xarray(array=cube.array, callback=callback))
        for cube in udf_data.get_datacube_list()
    ]
    # Insert the new tiles as list of raster collection tiles in the input object. The new tiles will
    # replace the original input tiles.
    udf_data.set_datacube_list(datacubes)
    return udf_data


def run_udf_code(code: str, data: UdfData) -> UdfData:
    # TODO: current implementation uses first match directly, first check for multiple matches?
    module = load_module_from_string(code)
    functions = ((k, v) for (k, v) in module.items() if callable(v))

    for (fn_name, func) in functions:
        try:
            sig = inspect.signature(func)
        except ValueError:
            continue
        params = sig.parameters
        first_param = next(iter(params.values()), None)

        if (
                fn_name == 'apply_timeseries'
                and 'series' in params and 'context' in params
                and _annotation_is_pandas_series(params["series"].annotation)
                and _annotation_is_pandas_series(sig.return_annotation)
        ):
            _log.info("Found timeseries mapping UDF `{n}` {f!r}".format(n=fn_name, f=func))
            return apply_timeseries_generic(data, func)
        elif (
                fn_name in ['apply_hypercube', 'apply_datacube']
                and 'cube' in params and 'context' in params
                and _annotation_is_udf_datacube(params["cube"].annotation)
                and _annotation_is_udf_datacube(sig.return_annotation)
        ):
            _log.info("Found datacube mapping UDF `{n}` {f!r}".format(n=fn_name, f=func))
            if len(data.get_datacube_list()) != 1:
                raise ValueError("The provided UDF expects exactly one datacube, but {c} were provided.".format(
                    c=len(data.get_datacube_list())
                ))
            # TODO: also support calls without user context?
            result_cube = func(data.get_datacube_list()[0], data.user_context)
            data.set_datacube_list([result_cube])
            return data
        elif len(params) == 1 and _annotation_is_udf_data(first_param.annotation):
            _log.info("Found generic UDF `{n}` {f!r}".format(n=fn_name, f=func))
            func(data)
            return data

    raise OpenEoUdfException("No UDF found.")


def execute_local_udf(udf: str, datacube: Union[str, xarray.DataArray, XarrayDataCube], fmt='netcdf'):
    """
    Locally executes an user defined function on a previously downloaded datacube.

    :param udf: the code of the user defined function
    :param datacube: the path to the downloaded data in disk or a DataCube
    :param fmt: format of the file if datacube is string
    :return: the resulting DataCube
    """

    if isinstance(datacube, (str, pathlib.Path)):
        d = XarrayDataCube.from_file(path=datacube, fmt=fmt)
    elif isinstance(datacube, XarrayDataCube):
        d = datacube
    elif isinstance(datacube, xarray.DataArray):
        d = XarrayDataCube(datacube)
    else:
        raise ValueError(datacube)
    # TODO: skip going through XarrayDataCube above, we only need xarray.DataArray here anyway.
    # datacube's data is to be float and x,y not provided
    d = XarrayDataCube(d.get_array()
                       .astype(numpy.float64)
                       .drop(labels='x')
                       .drop(labels='y')
                       )
    # wrap to udf_data
    udf_data = UdfData(datacube_list=[d])

    # TODO: enrich to other types like time series, vector data,... probalby by adding  named arguments
    # signature: UdfData(proj, datacube_list, feature_collection_list, structured_data_list, ml_model_list, metadata)

    # run the udf through the same routine as it would have been parsed in the backend
    result = run_udf_code(udf, udf_data)
    return result
