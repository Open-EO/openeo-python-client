from openeo.udf.xarraydatacube import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    inarr = cube.get_array()
    B4 = inarr.loc[:, 'bandzero']
    B8 = inarr.loc[:, 'bandone']
    ndvi = (B8 - B4) / (B8 + B4)
    ndvi = ndvi.expand_dims(dim='bands', axis=-3).assign_coords(bands=['ndvi'])
    return XarrayDataCube(ndvi)
