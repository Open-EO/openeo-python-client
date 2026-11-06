=========================
API: ``openeo.processes``
=========================

The ``openeo.processes`` module contains building blocks and helpers
to construct so called "child callbacks" for openEO processes like
:py:meth:`openeo.rest.datacube.DataCube.apply` and
:py:meth:`openeo.rest.datacube.DataCube.reduce_dimension`,
as discussed at :ref:`child_callback_callable`.

.. note::
    The contents of the ``openeo.processes`` module is automatically compiled
    from the official openEO process specifications.
    Developers that want to fix bugs in, or add implementations to this
    module should not touch the file directly, but instead address it in the
    upstream `openeo-processes <https://github.com/Open-EO/openeo-processes>`_ repository
    or in the internal tooling to generate this file.


.. contents:: Sections:
   :depth: 1
   :local:
   :backlinks: top


.. _openeo_processes_functions:

Functions in ``openeo.processes``
---------------------------------

The ``openeo.processes`` module implements (at top-level)
a regular Python function for each openEO process
(not only the official stable ones, but also experimental ones in "proposal" state).

These functions can be used directly as child callback,
for example as follows:

.. code-block:: python

    from openeo.processes import absolute, max

    cube.apply(absolute)
    cube.reduce_dimension(max, dimension="t")


Note how the signatures of the parent :py:class:`DataCube <openeo.rest.datacube.DataCube>` methods
and the callback functions match up:

-   :py:meth:`DataCube.apply() <openeo.rest.datacube.DataCube.apply>`
    expects a callback that receives a single numerical value,
    which corresponds to the parameter signature of :py:func:`openeo.processes.absolute`
-   :py:meth:`DataCube.reduce_dimension() <openeo.rest.datacube.DataCube.reduce_dimension>`
    expects a callback that receives an array of numerical values,
    which corresponds to the parameter signature :py:func:`openeo.processes.max`


.. automodule:: openeo.processes
    :members:
    :exclude-members: ProcessBuilder, process, _process


``ProcessBuilder`` helper class
--------------------------------

..  FYI the ProcessBuilder docs are provided through its doc block
    with an RST "include" of "api-processbuilder.rst"

.. autoclass:: openeo.processes.ProcessBuilder
