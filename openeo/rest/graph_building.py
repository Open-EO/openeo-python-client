"""
Public openEO process graph building utilities
'''''''''''''''''''''''''''''''''''''''''''''''

"""
from __future__ import annotations

from typing import Optional

from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.processes import ProcessBuilder


class CollectionProperty(_FromNodeMixin):
    """
    Helper object to easily create simple collection metadata property filters
    to be used with :py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>`.

    .. note:: This class should not be used directly by end user code.
        Use the :py:func:`~openeo.rest.graph_building.collection_property` factory instead.

    .. warning:: this is an experimental feature, naming might change.
    """

    def __init__(self, name: str, _builder: Optional[ProcessBuilder] = None):
        self.name = name
        self._builder = _builder or ProcessBuilder(pgnode={"from_parameter": "value"})

    def from_node(self) -> PGNode:
        return self._builder.from_node()

    def __eq__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder == other)

    def __ne__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder != other)

    def __gt__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder > other)

    def __ge__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder >= other)

    def __lt__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder < other)

    def __le__(self, other) -> CollectionProperty:
        return CollectionProperty(self.name, _builder=self._builder <= other)


def collection_property(name: str) -> CollectionProperty:
    """
    Helper to easily create simple collection metadata property filters
    to be used with :py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>`.

    Usage example:

    .. code-block:: python

        from openeo import collection_property
        ...

        connection.load_collection(
            ...
            properties=[
                collection_property("eo:cloud_cover") <= 75,
                collection_property("platform") == "Sentinel-2B",
            ]
        )

    .. warning:: this is an experimental feature, naming might change.

    .. versionadded:: 0.26.0

    :param name: name of the collection property to filter on
    :return: an object that supports operators like ``<=``, ``==`` to easily build simple property filters.
    """
    return CollectionProperty(name=name)
