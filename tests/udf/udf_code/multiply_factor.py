from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    factor = context["factor"]
    array = cube.get_array() * factor
    return XarrayDataCube(array)
