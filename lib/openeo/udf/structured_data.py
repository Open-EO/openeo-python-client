"""

"""

# Note: this module was initially developed under the ``openeo-udf`` project (https://github.com/Open-EO/openeo-udf)

from __future__ import annotations

import builtins
from typing import Union


class StructuredData:
    """
    This class represents structured data that is produced by an UDF and can not be represented
    as a raster or vector data cube. For example: the result of a statistical
    computation.

    Usage example::

        >>> StructuredData([3, 5, 8, 13])
        >>> StructuredData({"mean": 5, "median": 8})
        >>> StructuredData([('col_1', 'col_2'), (1, 2), (2, 3)], type="table")
    """

    def __init__(self, data: Union[list, dict], description: str = None, type: str = None):
        self.data = data
        self.type = type or builtins.type(data).__name__
        self.description = description or self.type

    def __repr__(self):
        return f"<{type(self).__name__} with {self.type}>"

    def to_dict(self) -> dict:
        return dict(
            data=self.data,
            description=self.description,
            type=self.type,
        )

    @classmethod
    def from_dict(cls, data: dict) -> StructuredData:
        return cls(
            data=data["data"],
            description=data.get("description"),
            type=data.get("type")
        )
