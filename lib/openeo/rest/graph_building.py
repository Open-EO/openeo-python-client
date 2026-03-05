"""
Public openEO process graph building utilities
'''''''''''''''''''''''''''''''''''''''''''''''

"""
from __future__ import annotations

from typing import Optional

import openeo.processes
from openeo.internal.graph_building import PGNode, _FromNodeMixin
from openeo.processes import ProcessBuilder


class CollectionProperty(_FromNodeMixin):
    """
    Helper object to easily create simple collection metadata property filters
    to be used with :py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>`
    and :py:meth:`Connection.load_stac() <openeo.rest.connection.Connection.load_stac>`.

    .. warning:: This class should not be used directly by end user code.
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

    def is_one_of(self, *args) -> CollectionProperty:
        """
        Filter on property being in an allow-list,
        provided as a single argument list/tuple/set:

        .. code-block:: python

            grid_codes = ["MGRS-32ULB", "MGRS-32UMB"]
            ...
            collection_property("grid:code").is_one_of(grid_codes)

        or as multiple arguments:

        .. code-block:: python

            collection_property("grid:code").is_one_of("MGRS-32ULB", "MGRS-32UMB")

        Both are equivalent.

        .. versionadded:: 0.49.0
        """
        if len(args) == 1 and isinstance(args[0], (list, tuple, set)):
            options = list(args[0])
        else:
            options = list(args)
        return CollectionProperty(
            self.name, _builder=openeo.processes.array_contains(data=options, value=self._builder)
        )


def collection_property(name: str) -> CollectionProperty:
    """
    Helper to easily create simple collection metadata property filters
    to be used with :py:meth:`Connection.load_collection() <openeo.rest.connection.Connection.load_collection>`
    and :py:meth:`Connection.load_stac() <openeo.rest.connection.Connection.load_stac>`.

    Usage example:

    .. code-block:: python

        from openeo import collection_property

        ...
        connection.load_collection(
            ...
            properties=[
                collection_property("eo:cloud_cover") <= 75,
                collection_property("platform") == "Sentinel-2B",
                collection_property("grid:code").is_one_of(
                    ["MGRS-32UKB", "MGRS-32ULB", "MGRS-32MB", "MGRS-32UNB"]
                ),
            ]
        )

    .. seealso::
        :ref:`collection_property_helper`

    .. warning:: this is an experimental feature, naming might change.

    .. versionadded:: 0.26.0

    .. versionchanged:: 0.49.0
        added :py:meth:`~CollectionProperty.is_one_of()` helper to filter based having a value from given allow-list

    :param name: name of the collection property to filter on
    :return: an object that supports operators like ``<=``, ``==`` to easily build simple property filters.
    """
    return CollectionProperty(name=name)
