"""

"""

# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

from __future__ import annotations

from typing import List, Optional, Union

from openeo.udf.feature_collection import FeatureCollection
from openeo.udf.structured_data import StructuredData
from openeo.udf.xarraydatacube import XarrayDataCube


class UdfData:
    """
    Container for data passed to a user defined function (UDF)
    """

    # TODO: original implementation in `openeo_udf` project had `get_datacube_by_id`, `get_feature_collection_by_id`: is it still useful to provide this?
    # TODO: original implementation in `openeo_udf` project had `server_context`: is it still useful to provide this?

    def __init__(
            self,
            proj: dict = None,
            datacube_list: Optional[List[XarrayDataCube]] = None,
            feature_collection_list: Optional[List[FeatureCollection]] = None,
            structured_data_list: Optional[List[StructuredData]] = None,
            user_context: Optional[dict] = None,
    ):
        """
        The constructor of the UDF argument class that stores all data required by the
        user defined function.

        :param proj: A dictionary of form {"proj type string": "projection description"} e.g. {"EPSG": 4326}
        :param datacube_list: A list of data cube objects
        :param feature_collection_list: A list of VectorTile objects
        :param structured_data_list: A list of structured data objects
        """
        self.datacube_list = datacube_list
        self.feature_collection_list = feature_collection_list
        self.structured_data_list = structured_data_list
        self.proj = proj
        self._user_context = user_context or {}

    def __repr__(self) -> str:
        fields = " ".join(
            f"{f}:{getattr(self, f)!r}" for f in
            ["datacube_list", "feature_collection_list", "structured_data_list"]
        )
        return f"<{type(self).__name__} {fields}>"

    @property
    def user_context(self) -> dict:
        """Return the user context that was passed to the run_udf function"""
        return self._user_context

    def get_datacube_list(self) -> Union[List[XarrayDataCube], None]:
        """Get the data cube list"""
        return self._datacube_list

    def set_datacube_list(self, datacube_list: Union[List[XarrayDataCube], None]):
        """
        Set the data cube list

        :param datacube_list: A list of data cubes
        """
        self._datacube_list = datacube_list

    datacube_list = property(fget=get_datacube_list, fset=set_datacube_list)

    def get_feature_collection_list(self) -> Union[List[FeatureCollection], None]:
        """get all feature collections as list"""
        return self._feature_collection_list

    def set_feature_collection_list(self, feature_collection_list: Union[List[FeatureCollection], None]):
        self._feature_collection_list = feature_collection_list

    feature_collection_list = property(fget=get_feature_collection_list, fset=set_feature_collection_list)

    def get_structured_data_list(self) -> Union[List[StructuredData], None]:
        """
        Get all structured data entries

        :return: A list of StructuredData objects
        """
        return self._structured_data_list

    def set_structured_data_list(self, structured_data_list: Union[List[StructuredData], None]):
        """
        Set the list of structured data

        :param structured_data_list: A list of StructuredData objects
        """
        self._structured_data_list = structured_data_list

    structured_data_list = property(fget=get_structured_data_list, fset=set_structured_data_list)

    def to_dict(self) -> dict:
        """
        Convert this UdfData object into a dictionary that can be converted into
        a valid JSON representation
        """
        return {
            "datacubes": [x.to_dict() for x in self.datacube_list] \
                if self.datacube_list else None,
            "feature_collection_list": [x.to_dict() for x in self.feature_collection_list] \
                if self.feature_collection_list else None,
            "structured_data_list": [x.to_dict() for x in self.structured_data_list] \
                if self.structured_data_list else None,
            "proj": self.proj,
            "user_context": self.user_context,
        }

    @classmethod
    def from_dict(cls, udf_dict: dict) -> UdfData:
        """
        Create a udf data object from a python dictionary that was created from
        the JSON definition of the UdfData class

        :param udf_dict: The dictionary that contains the udf data definition
        """

        datacubes = [XarrayDataCube.from_dict(x) for x in udf_dict.get("datacubes", [])]
        feature_collection_list = [FeatureCollection.from_dict(x) for x in udf_dict.get("feature_collection_list", [])]
        structured_data_list = [StructuredData.from_dict(x) for x in udf_dict.get("structured_data_list", [])]
        udf_data = cls(
            proj=udf_dict.get("proj"),
            datacube_list=datacubes,
            feature_collection_list=feature_collection_list,
            structured_data_list=structured_data_list,
            user_context=udf_dict.get("user_context")
        )
        return udf_data
