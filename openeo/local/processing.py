import inspect
import importlib
import logging
import xarray as xr
import rioxarray
from pathlib import Path

from openeo_pg_parser_networkx import ProcessRegistry
from openeo_processes_dask.process_implementations.core import process
from openeo_pg_parser_networkx.process_registry import Process

PROCESS_REGISTRY = ProcessRegistry(wrap_funcs=[process])

# Import these pre-defined processes from openeo_processes_dask and register them into registry
processes_from_module = [
    func
    for _, func in inspect.getmembers(
        importlib.import_module("openeo_processes_dask.process_implementations"),
        inspect.isfunction,
    )
]

specs_module = importlib.import_module("openeo_processes_dask.specs")
specs = {
    func.__name__: getattr(specs_module, func.__name__)
    for func in processes_from_module
}

for func in processes_from_module:
    PROCESS_REGISTRY[func.__name__] = Process(
        spec=specs[func.__name__], implementation=func
    )

_log = logging.getLogger(__name__)
def load_local_collection(*args, **kwargs):
    pretty_args = {k: type(v) for k, v in kwargs.items()}
    _log.debug(f"Running process load_collection")
    _log.debug(f"kwargs: {pretty_args}")
    _log.debug("-" * 80)
    collection = Path(kwargs['id'])
    if '.zarr' in collection.suffixes:
        data = xr.open_dataset(kwargs['id'],chunks={},engine='zarr')
    elif '.nc' in collection.suffixes:
        data = xr.open_dataset(kwargs['id'],chunks={},decode_coords='all').to_array(dim='bands') # Add decode_coords='all' if the crs as a band gives some issues
    elif '.tiff' in collection.suffixes or '.tif' in collection.suffixes:
        data = rioxarray.open_rasterio(kwargs['id']).rename({'band':'bands'})
    return data

from openeo_processes_dask.specs import load_collection as load_collection_spec
PROCESS_REGISTRY["load_collection"] = Process(spec=load_collection_spec, implementation=load_local_collection)