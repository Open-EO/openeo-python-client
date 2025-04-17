import numpy as np
import xarray

from openeo.metadata import CollectionMetadata
from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect


def apply_metadata(input_metadata: CollectionMetadata, context: dict) -> CollectionMetadata:

    xstep = input_metadata.get("x", "step")
    ystep = input_metadata.get("y", "step")
    new_metadata = {
        "x": {"type": "spatial", "axis": "x", "step": xstep / 2.0, "reference_system": 4326},
        "y": {"type": "spatial", "axis": "y", "step": ystep / 2.0, "reference_system": 4326},
        "t": {"type": "temporal"},
    }
    return CollectionMetadata(new_metadata)


def fancy_upsample_function(array: np.array, factor: int = 2) -> np.array:
    assert array.ndim == 3
    return array.repeat(factor, axis=-1).repeat(factor, axis=-2)


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xarray.DataArray = cube.get_array()

    cubearray: xarray.DataArray = cube.get_array().copy() + 60

    # We make prediction and transform numpy array back to datacube

    # Pixel size of the original image
    init_pixel_size_x = cubearray.coords["x"][-1] - cubearray.coords["x"][-2]
    init_pixel_size_y = cubearray.coords["y"][-1] - cubearray.coords["y"][-2]

    if cubearray.data.ndim == 4 and cubearray.data.shape[0] == 1:
        cubearray = cubearray[0]
    predicted_array = fancy_upsample_function(cubearray.data, 2)
    inspect(predicted_array, "test message")
    coord_x = np.linspace(
        start=cube.get_array().coords["x"].min(),
        stop=cube.get_array().coords["x"].max() + init_pixel_size_x,
        num=predicted_array.shape[-2],
        endpoint=False,
    )
    coord_y = np.linspace(
        start=cube.get_array().coords["y"].min(),
        stop=cube.get_array().coords["y"].max() + init_pixel_size_y,
        num=predicted_array.shape[-1],
        endpoint=False,
    )
    predicted_cube = xarray.DataArray(predicted_array, dims=["bands", "x", "y"], coords=dict(x=coord_x, y=coord_y))

    return XarrayDataCube(predicted_cube)
