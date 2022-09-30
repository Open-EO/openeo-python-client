"""
Public openEO process graph building utilities
'''''''''''''''''''''''''''''''''''''''''''''''

"""

from typing import Optional

from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.processes import ProcessBuilder


class CollectionProperty(_FromNodeMixin):
    def __init__(self, name: str, _builder: Optional[ProcessBuilder] = None):
        self.name = name
        self._builder = _builder or ProcessBuilder(pgnode={"from_parameter": "value"})

    def from_node(self) -> PGNode:
        return self._builder.from_node()

    def __eq__(self, other):
        return CollectionProperty(self.name, _builder=self._builder == other)

    def __ne__(self, other):
        return CollectionProperty(self.name, _builder=self._builder != other)

    def __gt__(self, other):
        return CollectionProperty(self.name, _builder=self._builder > other)

    def __ge__(self, other):
        return CollectionProperty(self.name, _builder=self._builder >= other)

    def __lt__(self, other):
        return CollectionProperty(self.name, _builder=self._builder < other)

    def __le__(self, other):
        return CollectionProperty(self.name, _builder=self._builder <= other)


def collection_property(name: str) -> CollectionProperty:
    """
    Helper to easily create simple a `load_collection` property filter.

    Usage example:

    .. code-block:: python

        connection.load_collection(
            ...
            properties=[
                collection_property("eo:cloud_cover") <= 75,
                collection_property("platform") == "Sentinel-2B",
            ]
        )

    :param name: name of the property to filter on
    :return: an object which supports operators like ``<=``, ``==`` to build a simple property filter.
    """
    return CollectionProperty(name=name)
