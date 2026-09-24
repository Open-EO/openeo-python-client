
=======================
DataCube construction
=======================


The ``load_collection`` process
=================================

The most straightforward way to start building your openEO data cube is through the ``load_collection`` process.
As mentioned earlier, this is provided by the
:py:meth:`~openeo.rest.connection.Connection.load_collection` method
on a :py:class:`~openeo.rest.connection.Connection` object,
which produces a :py:class:`~openeo.rest.datacube.DataCube` instance.
For example::

    cube = connection.load_collection("SENTINEL2_TOC")

While this should cover the majority of use cases,
there some cases
where one wants to build a :py:class:`~openeo.rest.datacube.DataCube` object
from something else or something more than just a simple ``load_collection`` process.



.. _datacube_from_process:

Construct DataCube from process
=================================

Through :ref:`user-defined processes <user-defined-processes>` one can encapsulate
one or more ``load_collection`` processes and additional processing steps in a single
reusable user-defined process.
For example, imagine a user-defined process "masked_s2"
that loads an openEO collection "SENTINEL2_TOC" and applies some kind of cloud masking.
The implementation details of the cloud masking are not important here,
but let's assume there is a parameter "dilation" to fine-tune the cloud mask.
Also note that the collection id "SENTINEL2_TOC" is hardcoded in the user-defined process.

We can now construct a data cube from this user-defined process
with :py:meth:`~openeo.rest.connection.Connection.datacube_from_process`
as follows::

    cube = connection.datacube_from_process("masked_s2", dilation=10)

    # Further processing of the cube:
    cube = cube.filter_temporal("2020-09-01", "2020-09-10")


Note that :py:meth:`~openeo.rest.connection.Connection.datacube_from_process` can be
used with all kind of processes, not only user-defined processes.
For example, while this is not exactly a real EO data use case,
it will produce a valid openEO process graph that can be executed::

    >>> cube = connection.datacube_from_process("mean", data=[2, 3, 5, 8])
    >>> cube.execute()
    4.5



.. _datacube_from_json:

Construct a DataCube from JSON
===============================

openEO process graphs are typically stored and published in JSON format.
Most notably, user-defined processes are transferred between openEO client
and back-end in a JSON structure roughly like in this example::

    {
      "id": "evi",
      "parameters": [
        {"name": "red", "schema": {"type": "number"}},
        {"name": "blue", "schema": {"type": "number"}},
        ...
      ],
      "process_graph": {
        "sub": {"process_id": "subtract", "arguments": {"x": {"from_parameter": "nir"}, "y": {"from_parameter": "red"}}},
        "p1": {"process_id": "multiply", "arguments": {"x": 6, "y": {"from_parameter": "red"}}},
        "div": {"process_id": "divide", "arguments": {"x": {"from_node": "sub"}, "y": {"from_node": "sum"}},
        ...


It is possible to construct a :py:class:`~openeo.rest.datacube.DataCube` object that corresponds with this
process graph with the :py:meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>` method.
It can be given one of:

    - a raw JSON string,
    - a path to a local JSON file,
    - an URL that points to a JSON resource

The JSON structure should be one of:

    - a mapping (dictionary) like the example above with at least a ``"process_graph"`` item,
      and optionally a ``"parameters"`` item.
    - a mapping (dictionary) with ``{"process_id": ...}`` items


Some examples
---------------

Load a :py:class:`~openeo.rest.datacube.DataCube` from a raw JSON string, containing a
simple "flat graph" representation:

.. code-block:: python

    raw_json = '''{
        "lc": {"process_id": "load_collection", "arguments": {"id": "SENTINEL2_TOC"}},
        "ak": {"process_id": "apply_kernel", "arguments": {"data": {"from_node": "lc"}, "kernel": [[1,2,1],[2,5,2],[1,2,1]]}, "result": true}
    }'''
    cube = connection.datacube_from_json(raw_json)

Load from a raw JSON string, containing a mapping with "process_graph" and "parameters":

.. code-block:: python

    raw_json = '''{
        "parameters": [
            {"name": "kernel", "schema": {"type": "array"}, "default": [[1,2,1], [2,5,2], [1,2,1]]}
        ],
        "process_graph": {
            "lc": {"process_id": "load_collection", "arguments": {"id": "SENTINEL2_TOC"}},
            "ak": {"process_id": "apply_kernel", "arguments": {"data": {"from_node": "lc"}, "kernel": {"from_parameter": "kernel"}}, "result": true}
        }
    }'''
    cube = connection.datacube_from_json(raw_json)

Load directly from a local file or URL containing these kind of JSON representations:

.. code-block:: python

    # Local file
    cube = connection.datacube_from_json("path/to/my_udp.json")

    # URL
    cube = connection.datacube_from_json("https://example.com/my_udp.json")


Parameterization
-----------------

When the process graph uses parameters, you must specify the desired parameter values
at the time of calling :py:meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>`.

For example, take this simple toy example of a process graph that takes the sum of 5 and a parameter "increment":

.. code-block:: python

    raw_json = '''{"add": {
        "process_id": "add",
        "arguments": {"x": 5, "y": {"from_parameter": "increment"}},
        "result": true
    }}'''

Trying to build a :py:class:`~openeo.rest.datacube.DataCube` from it without specifying parameter values will fail
like this:

.. code-block:: pycon

    >>> cube = connection.datacube_from_json(raw_json)
    ProcessGraphVisitException: No substitution value for parameter 'increment'.

Instead, specify the parameter value:

.. code-block:: pycon
    :emphasize-lines: 3

    >>> cube = connection.datacube_from_json(
    ...    raw_json,
    ...    parameters={"increment": 4},
    ... )
    >>> cube.execute()
    9


Parameters can also be defined with default values, which will be used when they are not specified
in the :py:meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>` call:

.. code-block:: python

    raw_json = '''{
        "parameters": [
            {"name": "increment", "schema": {"type": "number"}, "default": 100}
        ],
        "process_graph": {
            "add": {"process_id": "add", "arguments": {"x": 5, "y": {"from_parameter": "increment"}}, "result": true}
        }
    }'''

    cube = connection.datacube_from_json(raw_json)
    result = cube.execute())
    # result will be 105


Re-parameterization
```````````````````

TODO



.. _multi-result-process-graphs:

Building process graphs with multiple result nodes
===================================================

.. note::
    Multi-result support is added in version 0.35.0

Most openEO use cases are just about building a single result data cube,
which is readily covered in the openEO Python client library through classes like
:py:class:`~openeo.rest.datacube.DataCube` and :py:class:`~openeo.rest.vectorcube.VectorCube`.
It is straightforward to create a batch job from these, or execute/download them synchronously.

The openEO API also allows multiple result nodes in a single process graph,
for example to persist intermediate results or produce results in different output formats.
To support this, the openEO Python client library provides the :py:class:`~openeo.rest.multiresult.MultiResult` class,
which allows to group multiple :py:class:`~openeo.rest.datacube.DataCube` and :py:class:`~openeo.rest.vectorcube.VectorCube` objects
in a single entity that can be used to create or run batch jobs. For example:


.. code-block:: python

    from openeo import MultiResult

    res1 = cube1.save_result(...)
    res2 = cube2.save_result(...)
    multi_result = MultiResult([res1, res2])
    job = multi_result.create_job()


Moreover, it is not necessary to explicitly create such a
:py:class:`~openeo.rest.multiresult.MultiResult` object,
as the :py:meth:`Connection.create_job() <openeo.rest.connection.Connection.create_job>` method
directly supports passing multiple data cube objects in a list,
which will be automatically grouped as a multi-result:

.. code-block:: python

    res1 = cube1.save_result(...)
    res2 = cube2.save_result(...)
    job = connection.create_job([res1, res2])


.. important::

    Only a single :py:class:`~openeo.rest.connection.Connection` can be in play
    when grouping multiple results like this.
    As everything is to be merged in a single process graph
    to be sent to a single backend,
    it is not possible to mix cubes created from different connections.
