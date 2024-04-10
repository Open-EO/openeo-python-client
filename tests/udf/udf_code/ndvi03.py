from xarray import DataArray


def apply_datacube(cube: DataArray, context: dict) -> DataArray:
    B4 = cube.loc[:, 'bandzero']
    B8 = cube.loc[:, 'bandone']
    ndvi = (B8 - B4) / (B8 + B4)
    ndvi = ndvi.expand_dims(dim='bands', axis=-3).assign_coords(bands=['ndvi'])
    return ndvi
