import inspect
import logging
from pathlib import Path

import openeo_processes_dask.process_implementations
import openeo_processes_dask.specs
import rasterio
import rioxarray
import xarray as xr
from openeo_pg_parser_networkx import ProcessRegistry
from openeo_pg_parser_networkx.process_registry import Process
from openeo_processes_dask.process_implementations.core import process
from openeo_processes_dask.process_implementations.data_model import RasterCube

_log = logging.getLogger(__name__)


def init_process_registry():
    process_registry = ProcessRegistry(wrap_funcs=[process])

    # Import these pre-defined processes from openeo_processes_dask and register them into registry
    processes_from_module = [
        func
        for _, func in inspect.getmembers(
            openeo_processes_dask.process_implementations,
            inspect.isfunction,
        )
    ]

    specs = {}
    for func in processes_from_module:
        try:
            specs[func.__name__] = getattr(openeo_processes_dask.specs, func.__name__)
        except Exception:
            continue

    for func in processes_from_module:
        try:
            process_registry[func.__name__] = Process(
            spec=specs[func.__name__], implementation=func
            )
        except Exception:
            continue
    return process_registry


PROCESS_REGISTRY = init_process_registry()


def load_local_collection(*args, **kwargs):
    pretty_args = {k: repr(v)[:80] for k, v in kwargs.items()}
    _log.info("Running process load_collection")
    _log.debug(
            f"Running process load_collection with resolved parameters: {pretty_args}"
        )
    collection = Path(kwargs['id'])
    if '.zarr' in collection.suffixes:
        data = xr.open_dataset(kwargs['id'],chunks={},engine='zarr')
    elif '.nc' in collection.suffixes:
        data = xr.open_dataset(kwargs['id'],chunks={},decode_coords='all') # Add decode_coords='all' if the crs as a band gives some issues
        crs = None
        if 'crs' in data.coords:
            if 'spatial_ref' in data.crs.attrs:
                crs = data.crs.attrs['spatial_ref']
            elif 'crs_wkt' in data.crs.attrs:
                crs = data.crs.attrs['crs_wkt']
        data = data.to_array(dim='bands')
        if crs is not None:
            data.rio.write_crs(crs,inplace=True)
    elif '.tiff' in collection.suffixes or '.tif' in collection.suffixes:
        data = rioxarray.open_rasterio(kwargs['id'],chunks={},band_as_variable=True)
        for d in data.data_vars:
            descriptions = [v for k, v in data[d].attrs.items() if k.lower() == "description"]
            if descriptions:
                data = data.rename({d: descriptions[0]})
        data = data.to_array(dim='bands')
    return data

PROCESS_REGISTRY["load_collection"] = Process(
    spec=openeo_processes_dask.specs.load_collection,
    implementation=load_local_collection,
)

def resample_cube_spatial_rioxarray(data: RasterCube, target: RasterCube, method: str = "near") -> RasterCube:
    _log.info("Running process resample_cube_spatial")
    methods_dict = {
        "near": rasterio.enums.Resampling.nearest,
        "bilinear": rasterio.enums.Resampling.bilinear,
        "cubic": rasterio.enums.Resampling.cubic,
        "cubicspline": rasterio.enums.Resampling.cubic_spline,
        "lanczos": rasterio.enums.Resampling.lanczos,
        "average": rasterio.enums.Resampling.average,
        "mode": rasterio.enums.Resampling.mode,
        "gauss": rasterio.enums.Resampling.gauss,
        "max": rasterio.enums.Resampling.max,
        "min": rasterio.enums.Resampling.min,
        "med": rasterio.enums.Resampling.med,
        "q1": rasterio.enums.Resampling.q1,
        "q3": rasterio.enums.Resampling.q3,
        "sum": rasterio.enums.Resampling.sum,
        "rms": rasterio.enums.Resampling.rms
    }

    if method not in methods_dict:
        raise ValueError(
            f'Selected resampling method "{method}" is not available! Please select one of '
            f"[{', '.join(methods_dict.keys())}]"
        )
    if len(data.openeo.temporal_dims) > 0:
        for i,t in enumerate(data[data.openeo.temporal_dims[0]]):
            if i == 0:
                resampled_data = data.loc[{data.openeo.temporal_dims[0]:t}].rio.reproject_match(
                    target, resampling=methods_dict[method]
                )
                resampled_data = resampled_data.assign_coords({data.openeo.temporal_dims[0]:t}).expand_dims(data.openeo.temporal_dims[0])
            else:
                tmp = data.loc[{data.openeo.temporal_dims[0]:t}].rio.reproject_match(
                    target, resampling=methods_dict[method]
                )
                tmp = tmp.assign_coords({data.openeo.temporal_dims[0]:t}).expand_dims(data.openeo.temporal_dims[0])
                resampled_data = xr.concat([resampled_data,tmp],dim=data.openeo.temporal_dims[0])
    else:
        resampled_data = data.rio.reproject_match(
            target, resampling=methods_dict[method]
        )
    resampled_data.rio.write_crs(target.rio.crs, inplace=True)

    # Order axes back to how they were before
    resampled_data = resampled_data.transpose(*data.dims)

    # Ensure that attrs except crs are copied over
    for k, v in data.attrs.items():
        if k.lower() != "crs":
            resampled_data.attrs[k] = v
    return resampled_data


PROCESS_REGISTRY["resample_cube_spatial"] = Process(
    spec=openeo_processes_dask.specs.resample_cube_spatial,
    implementation=resample_cube_spatial_rioxarray,
)
