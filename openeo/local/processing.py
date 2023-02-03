import importlib
import inspect
import logging
from pathlib import Path

import rioxarray
import xarray as xr
from openeo_pg_parser_networkx import ProcessRegistry
from openeo_processes_dask.core import process as openeo_process

standard_processes = [
    func
    for _, func in inspect.getmembers(
        importlib.import_module("openeo_processes_dask.process_implementations"),
        inspect.isfunction,
    )
]

PROCESS_REGISTRY = ProcessRegistry(wrap_funcs=[openeo_process])

for func in standard_processes:
    PROCESS_REGISTRY[func.__name__] = func

# We need to define a custom `load_collection` process, used to load local netCDFs
_log = logging.getLogger(__name__)


def load_local_collection(*args, **kwargs):
    pretty_args = {k: type(v) for k, v in kwargs.items()}
    _log.debug(f"Running process load_collection")
    _log.debug(f"kwargs: {pretty_args}")
    _log.debug("-" * 80)
    collection = Path(kwargs["id"])
    if ".zarr" in collection.suffixes:
        data = xr.open_dataset(kwargs["id"], chunks={}, engine="zarr")
    elif ".nc" in collection.suffixes:
        data = xr.open_dataset(kwargs["id"], chunks={}, decode_coords="all").to_array(
            dim="bands"
        )  # Add decode_coords='all' if the crs as a band gives some issues
    elif ".tiff" in collection.suffixes or ".tif" in collection.suffixes:
        data = rioxarray.open_rasterio(kwargs["id"]).rename({"band": "bands"})
    return data


PROCESS_REGISTRY["load_collection"] = load_local_collection
