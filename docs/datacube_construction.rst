
=======================
DataCube construction
=======================


The ``load_collection`` process
=================================

The most straightforward way to start building your openEO data cube is through the ``load_collection`` process.
As mentioned earlier, this is provided by the
:meth:`~openeo.rest.connection.Connection.load_collection` method
on a :class:`~openeo.rest.connection.Connection` object,
which produces a :class:`~openeo.rest.datacube.DataCube` instance.
For example::

    cube = connection.load_collection("SENTINEL2_TOC")

While this should cover the majority of use cases,
there some cases
where one wants to build a :class:`~openeo.rest.datacube.DataCube` object
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
with :meth:`~openeo.rest.connection.Connection.datacube_from_process`
as follows::

    cube = connection.datacube_from_process("masked_s2", dilation=10)

    # Further processing of the cube:
    cube = cube.filter_temporal("2020-09-01", "2020-09-10")


Note that :meth:`~openeo.rest.connection.Connection.datacube_from_process` can be
used with all kind of processes, not only user-defined processes.
For example, while this is not exactly a real EO data use case,
it will produce a valid openEO process graph that can be executed::

    >>> cube = connection.datacube_from_process("mean", data=[2, 3, 5, 8])
    >>> cube.execute()
    4.5



.. _datacube_from_json:

Construct DataCube from JSON
==============================

openEO process graphs are typically stored and published in JSON format.
Most notably, user-defined processes are transferred between openEO client
and backend in a JSON structure roughly like in this example::

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


It is possible to construct a :class:`~openeo.rest.datacube.DataCube` object that corresponds with this
process graph with the :meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>` method.
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

Load a :class:`~openeo.rest.datacube.DataCube` from a raw JSON string, containing a
simple "flat graph" representation::

    raw_json = '''{
        "lc": {"process_id": "load_collection", "arguments": {"id": "SENTINEL2_TOC"}},
        "ak": {"process_id": "apply_kernel", "arguments": {"data": {"from_node": "lc"}, "kernel": [[1,2,1],[2,5,2],[1,2,1]]}, "result": true}
    }'''
    cube = connection.datacube_from_json(raw_json)

Load from a raw JSON string, containing a mapping with "process_graph" and "parameters"::

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

Load directly from a file or URL containing these kind of JSON representations::

    cube = connection.datacube_from_json("path/to/my_udp.json")

    cube = connection.datacube_from_json("https://openeo.example/process_graphs/my_udp")


Parameterization
-----------------

When the process graph uses parameters, you must specify the desired parameter values
at the time of calling :meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>`.

For example, take this simple toy example of a process graph that takes the sum of 5 and a parameter "increment"::

    raw_json = '''{"add": {
        "process_id": "add",
        "arguments": {"x": 5, "y": {"from_parameter": "increment"}},
        "result": true
    }}'''

Trying to build a :class:`~openeo.rest.datacube.DataCube` from it without specifying parameter values will fail
like this::

    >>> cube = connection.datacube_from_json(raw_json)
    ProcessGraphVisitException: No substitution value for parameter 'increment'.

Instead, specify the parameter value::

    >>> cube = connection.datacube_from_json(raw_json, parameters={"increment": 4})
    >>> cube.execute()
    9


Parameters can also be defined with default values, which will be used when they are not specified
in the :meth:`Connection.datacube_from_json <openeo.rest.connection.Connection.datacube_from_json>` call::

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
