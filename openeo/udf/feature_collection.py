"""

"""

# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

from typing import Optional, Union, Any, List

import pandas
import shapely.geometry

# Geopandas is optional dependency for now
try:
    from geopandas import GeoDataFrame
except ImportError:
    class GeoDataFrame:
        pass


class FeatureCollection:
    """
    A feature collection that represents a subset or a whole feature collection
    where single vector features may have time stamps assigned.
    """

    def __init__(
            self,
            id: str,
            data: GeoDataFrame,
            start_times: Optional[Union[pandas.DatetimeIndex, List[str]]] = None,
            end_times: Optional[Union[pandas.DatetimeIndex, List[str]]] = None
    ):
        """
        Constructor of the  of a vector collection

        :param id: The unique id of the vector collection
        :param data: A GeoDataFrame with geometry column and attribute data
        :param start_times: The vector with start times for each spatial x,y slice
        :param end_times: The pandas.DateTimeIndex vector with end times
            for each spatial x,y slice, if no
            end times are defined, then time instances are assumed not intervals
        """
        self.id = id
        self._data = data
        self._start_times = self._as_datetimeindex(start_times, expected_length=len(self.data))
        self._end_times = self._as_datetimeindex(end_times, expected_length=len(self.data))

    @staticmethod
    def _as_datetimeindex(dates: Any, expected_length: int = None) -> Union[pandas.DatetimeIndex, None]:
        if dates is None:
            return dates
        if not isinstance(dates, pandas.DatetimeIndex):
            dates = pandas.DatetimeIndex(dates)
        if expected_length is not None and expected_length != len(dates):
            raise ValueError("Expected size {e} but got {a}: {d}".format(e=expected_length, a=len(dates), d=dates))
        return dates

    @property
    def data(self) -> GeoDataFrame:
        """
        Get the geopandas.GeoDataFrame that contains the geometry column and any number of attribute columns

        :return: A data frame that contains the geometry column and any number of attribute columns
        """
        return self._data

    @property
    def start_times(self) -> Union[pandas.DatetimeIndex, None]:
        return self._start_times

    @property
    def end_times(self) -> Union[pandas.DatetimeIndex, None]:
        return self._end_times

    def to_dict(self) -> dict:
        """
        Convert this FeatureCollection into a dictionary that can be converted into
        a valid JSON representation
        """
        data = {
            "id": self.id,
            "data": shapely.geometry.mapping(self.data),
        }
        if self.start_times is not None:
            data["start_times"] = [t.isoformat() for t in self.start_times]
        if self.end_times is not None:
            data["end_times"] = [t.isoformat() for t in self.end_times]
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "FeatureCollection":
        """
        Create a feature collection from a python dictionary that was created from
        the JSON definition of the FeatureCollection

        :param data: The dictionary that contains the feature collection  definition
        :return: A new FeatureCollection object
        """
        return cls(
            id=data["id"],
            data=GeoDataFrame.from_features(data["data"]),
            start_times=data.get("start_times"),
            end_times=data.get("end_times"),
        )
