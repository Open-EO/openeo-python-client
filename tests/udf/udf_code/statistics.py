from openeo.udf.structured_data import StructuredData
from openeo.udf.udf_data import UdfData


def rct_stats(udf_data: UdfData):
    """Compute univariate statistics for each hypercube

    Args:
        udf_data (UdfData): The UDF data object that contains raster and vector tiles

    Returns:
        This function will not return anything, the UdfData object "udf_data" must be used to store the resulting
        data.

    """
    # The dictionary that stores the statistical data
    stats = {}
    # Iterate over each raster collection cube and compute statistical values
    for cube in udf_data.get_datacube_list():
        # make sure to cast the values to floats, otherwise they are not serializable
        stats[cube.id] = dict(
            sum=float(cube.array.sum()),
            mean=float(cube.array.mean()),
            min=float(cube.array.min()),
            max=float(cube.array.max())
        )
    # Create the structured data object
    sd = StructuredData(
        data=stats,
        type="dict",
        description="Statistical data sum, min, max and mean for each raster collection cube as dict",
    )
    # Remove all collections and set the StructuredData list
    udf_data.del_datacube_list()
    udf_data.set_structured_data_list([sd, ])
