from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    # access the underlying xarray
    inarr = cube.get_array()

    # ndvi
    B4 = inarr.loc[:, "B04"]
    B8 = inarr.loc[:, "B08"]
    ndvi = (B8 - B4) / (B8 + B4)

    # extend bands dim
    ndvi = ndvi.expand_dims(dim="bands", axis=-3).assign_coords(bands=["ndvi"])

    # wrap back to datacube and return
    return XarrayDataCube(ndvi)
