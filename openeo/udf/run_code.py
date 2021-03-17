import functools
import inspect
import logging
import math
from typing import Dict, Callable
from typing import Union

import numpy
import pandas
import shapely
import xarray
from pandas import Series

from openeo.udf.structured_data import StructuredData
from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube

log = logging.getLogger(__name__)


def _build_default_execution_context():
    context = {
        'numpy': numpy,
        'xarray': xarray,
        # 'geopandas': geopandas,
        'pandas': pandas,
        'shapely': shapely,
        'math': math,
        # 'FeatureCollection': FeatureCollection,
        # 'SpatialExtent': SpatialExtent,
        'StructuredData': StructuredData,
        # 'MachineLearnModel': MachineLearnModelConfig,
        'DataCube': XarrayDataCube,
        'XarrayDataCube': XarrayDataCube,
        'UdfData': UdfData
    }
    try:
        import torch
        context['torch'] = torch
        import torchvision
    except ImportError as e:
        log.info('torch not available')
    try:
        import tensorflow
        context['tensorflow'] = tensorflow
        import tensorboard
    except ImportError as e:
        log.info('tensorflow not available')

    return context


@functools.lru_cache(maxsize=100)
def load_module_from_string(code: str):
    """
    Experimental: avoid loading same UDF module more than once, to make caching inside the udf work.
    @param code:
    @return:
    """
    module = _build_default_execution_context()
    exec(code, module)
    return module


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


def apply_timeseries_example(series: Series, context: Dict) -> Series:
    """
    UDF Template for callbacks that process timeseries

    :param series:
    :param context:
    :return:
    """
    return series


def apply_timeseries_generic(udf_data: UdfData, callback: Callable[[Series, dict], Series] = apply_timeseries_example):
    """
    Implements the UDF contract by calling a user provided time series transformation function (apply_timeseries).
    Multiple bands are currently handled separately, another approach could provide a dataframe with a timeseries for each band.

    :param udf_data:
    :param callback: callable that takes a pandas Series and context dict and returns a pandas Series
    :return:
    """
    # The list of tiles that were created
    tile_results = []

    # Iterate over each cube
    for cube in udf_data.get_datacube_list():
        array3d = []
        # use rollaxis to make the time dimension the last one
        for time_x_slice in numpy.rollaxis(cube.array.values, 1):
            time_x_result = []
            for time_slice in time_x_slice:
                series = pandas.Series(time_slice)
                transformed_series = callback(series, udf_data.user_context)
                time_x_result.append(transformed_series)
            array3d.append(time_x_result)

        # We need to create a new 3D array with the correct shape for the computed aggregate
        result_tile = numpy.rollaxis(numpy.asarray(array3d), 1)
        assert result_tile.shape == cube.array.shape
        # Create the new raster collection cube
        rct = XarrayDataCube(xarray.DataArray(result_tile))
        tile_results.append(rct)
    # Insert the new tiles as list of raster collection tiles in the input object. The new tiles will
    # replace the original input tiles.
    udf_data.set_datacube_list(tile_results)
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
            # this is a UDF that transforms pandas series
            return apply_timeseries_generic(data, func)
        elif (
                fn_name in ['apply_hypercube', 'apply_datacube']
                and 'cube' in params and 'context' in params
                and _annotation_is_udf_datacube(params["cube"].annotation)
                and _annotation_is_udf_datacube(sig.return_annotation)
        ):
            # found a datacube mapping function
            if len(data.get_datacube_list()) != 1:
                raise ValueError("The provided UDF expects exactly one datacube, but {c} were provided.".format(
                    c=len(data.get_datacube_list())
                ))
            # TODO: also support calls without user context?
            result_cube = func(data.get_datacube_list()[0], data.user_context)
            if not isinstance(result_cube, XarrayDataCube):
                raise ValueError("The provided UDF did not return a DataCube, but got: %s" % result_cube)
            data.set_datacube_list([result_cube])
            return data
        elif len(params) == 1 and _annotation_is_udf_data(first_param.annotation):
            # found a generic UDF function
            func(data)
        else:
            # TODO: raise exception?
            pass

    return data
