from typing import Optional, List, Union

from openeo.udf.structured_data import StructuredData
from openeo.udf.xarraydatacube import XarrayDataCube


class UdfData:
    """
    Container for data passed to a user defined function (UDF)
    """

    def __init__(
            self,
            proj: dict = None,
            datacube_list: Optional[List[XarrayDataCube]] = None,
            structured_data_list: Optional[List[StructuredData]] = None,
            user_context: Optional[dict] = None,
    ):
        """
        The constructor of the UDF argument class that stores all data required by the
        user defined function.

        :param proj: A dictionary of form {"proj type string": "projection description"} i. e. {"EPSG":4326}
        :param datacube_list: A list of data cube objects
        :param structured_data_list: A list of structured data objects
        """
        self.datacube_list = datacube_list or []
        self.structured_data_list = structured_data_list or []
        self.proj = proj
        self._user_context = user_context or {}

    @property
    def user_context(self) -> dict:
        """Return the user context that was passed to the run_udf function"""
        return self._user_context

    def get_datacube_list(self) -> List[XarrayDataCube]:
        """Get the data cube list"""
        return self._datacube_list

    def set_datacube_list(self, datacube_list: Union[List[XarrayDataCube], None]):
        """
        Set the data cube list

        If datacube_list is empty, then the list will be cleared

        :param datacube_list: A list of data cubes
        """
        self._datacube_list = datacube_list or []

    def del_datacube_list(self):
        """Delete all data cubes"""
        self._datacube_list = None

    def get_structured_data_list(self) -> List[StructuredData]:
        """
        Get all structured data entries

        :return: A list of StructuredData objects
        """
        return self._structured_data_list

    def set_structured_data_list(self, structured_data_list: Union[List[StructuredData], None]):
        """
        Set the list of structured data

        If structured_data_list is empty, then the list will be cleared

        :param structured_data_list: A list of StructuredData objects
        """
        self._structured_data_list = structured_data_list or []

    def del_structured_data_list(self):
        """Delete all structured data entries"""
        self._structured_data_list = None

    # TODO: get rid of these getter/setter properties to make things more excplicit and readable?
    datacube_list = property(
        fget=get_datacube_list, fset=set_datacube_list, fdel=del_datacube_list
    )
    structured_data_list = property(
        fget=get_structured_data_list, fset=set_structured_data_list, fdel=del_structured_data_list
    )

    def to_dict(self) -> dict:
        """
        Convert this UdfData object into a dictionary that can be converted into
        a valid JSON representation
        """
        return {
            "datacubes": [x.to_dict() for x in self.datacube_list],
            "structured_data_list": [x.to_dict() for x in self.structured_data_list],
            "proj": self.proj,
            "user_context": self.user_context,
        }

    @classmethod
    def from_dict(cls, udf_dict: dict) -> "UdfData":
        """
        Create a udf data object from a python dictionary that was created from
        the JSON definition of the UdfData class

        :param udf_dict: The dictionary that contains the udf data definition
        """

        datacubes = [XarrayDataCube.from_dict(x) for x in udf_dict.get("datacubes", [])]
        structured_data_list = [StructuredData.from_dict(x) for x in udf_dict.get("structured_data_list", [])]
        udf_data = cls(
            proj=udf_dict.get("proj"),
            datacube_list=datacubes,
            structured_data_list=structured_data_list,
            user_context=udf_dict.get("user_context")
        )
        return udf_data
