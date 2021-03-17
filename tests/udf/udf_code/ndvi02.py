from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube


def hyper_ndvi(udf_data: UdfData):
    """Compute the NDVI based on RED and NIR hypercubes

    Hypercubes with ids "red" and "nir" are required. The NDVI computation will be applied
    to all hypercube dimensions.

    Args:
        udf_data (UdfData): The UDF data object that contains raster and vector tiles as well as hypercubes
        and structured data.

    Returns:
        This function will not return anything, the UdfData object "udf_data" must be used to store the resulting
        data.

    """
    red = None
    nir = None

    # Iterate over each tile
    for cube in udf_data.get_datacube_list():
        if "red" in cube.id.lower():
            red = cube
        if "nir" in cube.id.lower():
            nir = cube
    if red is None:
        raise Exception("Red hypercube is missing in input")
    if nir is None:
        raise Exception("Nir hypercube is missing in input")

    ndvi = (nir.array - red.array) / (nir.array + red.array)
    ndvi.name = "NDVI"

    hc = XarrayDataCube(array=ndvi)
    udf_data.set_datacube_list([hc, ])
