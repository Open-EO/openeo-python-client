import numpy as np
import xarray

from openeo.metadata import CollectionMetadata
from openeo.udf.debug import inspect


def apply_metadata(input_metadata: CollectionMetadata, context: dict) -> CollectionMetadata:
    res= [ d.step / 2.0 for d in input_metadata.spatial_dimensions]
    return input_metadata.resample_spatial(resolution=res)


def fancy_upsample_function(array: np.array, factor: int = 2) -> np.array:
    assert array.ndim == 3
    return array.repeat(factor, axis=-1).repeat(factor, axis=-2)


def apply_datacube(cube: xarray.DataArray, context: dict) -> xarray.DataArray:
    cubearray: xarray.DataArray = cube.copy() + 60

    # We make prediction and transform numpy array back to datacube

    # Pixel size of the original image
    init_pixel_size_x = cubearray.coords["x"][-1].item() - cubearray.coords["x"][-2].item()
    init_pixel_size_y = cubearray.coords["y"][-1].item() - cubearray.coords["y"][-2].item()

    if cubearray.data.ndim == 4 and cubearray.data.shape[0] == 1:
        cubearray = cubearray[0]
    predicted_array = fancy_upsample_function(cubearray.data, 2)
    inspect(data=predicted_array, message=f"predicted array")
    coord_x = np.linspace(
        start=cube.coords["x"][0].item(),
        stop=cube.coords["x"][-1].item() + init_pixel_size_x,
        num=predicted_array.shape[-2],
        endpoint=False,
    )
    coord_y = np.linspace(
        start=cube.coords["y"].min().item(),
        stop=cube.coords["y"].max().item() + init_pixel_size_y,
        num=predicted_array.shape[-1],
        endpoint=False,
    )
    predicted_cube = xarray.DataArray(
        predicted_array,
        dims=["bands", "x", "y"],
        coords=dict(x=coord_x, y=coord_y),
    )

    return predicted_cube
