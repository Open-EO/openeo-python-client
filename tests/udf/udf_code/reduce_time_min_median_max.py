from openeo.udf.udf_data import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube


def hyper_min_median_max(udf_data: UdfData):
    """Compute the min, median and max of the time dimension of a hyper cube

    Hypercubes with time dimensions are required. The min, median and max reduction of th time axis will be applied
    to all hypercube dimensions.

    Args:
        udf_data (UdfData): The UDF data object that contains raster and vector tiles as well as hypercubes
        and structured data.

    Returns:
        This function will not return anything, the UdfData object "udf_data" must be used to store the resulting
        data.

    """
    # Iterate over each tile
    cube_list = []
    for cube in udf_data.get_datacube_list():
        min = cube.array.min(dim="t")
        median = cube.array.median(dim="t")
        max = cube.array.max(dim="t")

        min.name = cube.id + "_min"
        median.name = cube.id + "_median"
        max.name = cube.id + "_max"

        cube_list.append(XarrayDataCube(array=min))
        cube_list.append(XarrayDataCube(array=median))
        cube_list.append(XarrayDataCube(array=max))

    udf_data.set_datacube_list(cube_list)
