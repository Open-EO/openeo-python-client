import numpy
import xarray

from openeo.udf import OpenEoUdfException
from openeo.util import dict_no_none, deep_get


class XarrayDataCube:
    """
    This is a thin wrapper around :py:class:`xarray.DataArray`
    providing a basic "DataCube" interface for openEO UDF usage around multi-dimensional data.
    """

    def __init__(self, array: xarray.DataArray):
        if not isinstance(array, xarray.DataArray):
            raise OpenEoUdfException("Argument data must be of type xarray.DataArray")
        self._array = array

    def get_array(self) -> xarray.DataArray:
        """
        Get the :py:class:`xarray.DataArray` that contains the data and dimension definition
        """
        return self._array

    @property
    def id(self):
        return self._array.name

    def to_dict(self) -> dict:
        """
        Convert this hypercube into a dictionary that can be converted into
        a valid JSON representation

        >>> example = {
        ...     "id": "test_data",
        ...     "data": [
        ...         [[0.0, 0.1], [0.2, 0.3]],
        ...         [[0.0, 0.1], [0.2, 0.3]],
        ...     ],
        ...     "dimension": [
        ...         {"name": "time", "coordinates": ["2001-01-01", "2001-01-02"]},
        ...         {"name": "X", "coordinates": [50.0, 60.0]},
        ...         {"name": "Y"},
        ...     ],
        ... }
        """
        xd = self._array.to_dict()
        return dict_no_none({
            "id": xd.get("name"),
            "data": xd.get("data"),
            "description": deep_get(xd, "attrs", "description", default=None),
            "dimensions": [
                dict_no_none(
                    name=dim,
                    coordinates=deep_get(xd, "coords", dim, "data", default=None)
                )
                for dim in xd.get("dims", [])
            ]
        })

    @classmethod
    def from_dict(cls, xdc_dict: dict) -> "XarrayDataCube":
        """
        Create a :py:class:`XarrayDataCube` from a Python dictionary that was created from
        the JSON definition of the data cube

        :param data: The dictionary that contains the data cube definition
        """

        if "data" not in xdc_dict:
            raise OpenEoUdfException("Missing data in dictionary")

        data = numpy.asarray(xdc_dict["data"])

        if "dimensions" in xdc_dict:
            dims = [dim["name"] for dim in xdc_dict["dimensions"]]
            coords = {dim["name"]: dim["coordinates"] for dim in xdc_dict["dimensions"] if "coordinates" in dim}
        else:
            dims = None
            coords = None

        x = xarray.DataArray(data, dims=dims, coords=coords, name=xdc_dict.get("id"))

        if "description" in xdc_dict:
            x.attrs["description"] = xdc_dict["description"]

        return cls(array=x)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
