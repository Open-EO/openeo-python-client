***********************
Working with processes
***********************

In openEO, a **process** is an operation that performs a specific task on
a set of parameters and returns a result. For example: one wishes to
apply a statistical operation such as mean or median on selected EO data.
A process is similar to a *function* or method in common programming languages
and likewise, multiple processes can be combined or chained together
into new, more complex operations.

A **pre-defined process** is a process provided by a given *backend*.
These are often the processes `defined centrally by openEO <https://openeo.org/documentation/1.0/processes.html>`_,
including but not limited to common
mathematical (``sum``, ``divide``, ``sqrt``, ...),
logical (``and``, ``if``, ...), statistical (``mean``, ``max``, ...),
image processing (``mask``, ``apply_kernel``, ...)
operations.
Backends are expected to support most of these.
Backends are also free to pre-define additional processes that are
not (yet) centrally defined by openEO.

As noted above, processes can be combined into a larger, reusable unit
and stored on the backend as a **user-defined process**.
Note: don't confuse user-defined processes with
**user-defined functions** (UDF's) in openEO, which is a mechanism to
inject actual source code (for example Python code) into a process
for fine-grained processing.

How processes are combined into a larger unit
is internally represented by a so-called **process graph**.
It describes how the inputs and outputs of process graphs
should be linked together.
A user of the Python client should normally not worry about
the details of a process graph structure, as most of these aspects
are hidden behind regular Python functions, classes and methods.



Using common pre-defined processes
===================================

The listing of pre-defined processes provided by a backend
can be inspected with :func:`~openeo.rest.connection.Connection.list_processes`.
For example, to get a list of the process names (process ids)::

    >>> process_ids = [process["id"] for process in connection.list_processes()]
    >>> print(process_ids[:16])
    ['arccos', 'arcosh', 'power', 'last', 'subtract', 'not', 'cosh', 'artanh',
    'is_valid', 'first', 'median', 'eq', 'absolute', 'arctan2', 'divide','is_nan']

More information about the processes, like a description
or expected parameters, can be queried like that,
but it is often easier to look them up on the
`official openEO process documentation <https://openeo.org/documentation/1.0/processes.html>`_


Most of the important pre-defined processes are covered directly by methods
of the :class:`~openeo.rest.datacube.DataCube` class.
For example, to apply the ``filter_temporal`` process to a data cube::

    cube = cube.filter_temporal("2020-02-20", "2020-06-06")

Being regular Python methods, you get usual function call features
you're accustomed to: default values, keyword arguments, ``kwargs`` usage, ...
For example, to use a bounding box dictionary with ``kwargs``-expansion::

    bbox = {
        "west": 5.05, "south": 51.20, "east": 5.10, "north": 51.23,
        "crs": "EPSG:4326"
    }
    cube = cube.filter_bbox(**bbox)

Note that some methods try to be more flexible and convenient to use
than how the official process definition prescribes.
For example, the ``filter_temporal`` process expects an ``extent`` array
with 2 items (the start and end date),
but you can call the corresponding client method in multiple equivalent ways::

    cube.filter_temporal("2019-07-01", "2019-08-01")
    cube.filter_temporal(["2019-07-01", "2019-08-01"])
    cube.filter_temporal(extent=["2019-07-01", "2019-08-01"])
    cube.filter_temporal(start_date="2019-07-01", end_date="2019-08-01"])



Generic API for adding processes
=================================

An openEO backend may offer processes that are not part of the core API,
or the client may not (yet) have a corresponding method
for a process that you wish to use.
In that case, you can fall back to a more generic API
that allows you to add processes directly.

Basics
------

To add a simple process to the graph, use
the :func:`~openeo.rest.datacube.DataCube.process` method
on a :class:`~openeo.rest.datacube.DataCube`.
You have to specify the process id and arguments
(as a single dictionary or through keyword arguments ``**kwargs``).
It will return a new DataCube with the new process appended
to the internal process graph.

A very simple example using the ``mean`` process and a
literal list in an arguments dictionary::

    arguments= {
        "data": [1, 3, -1]
    }
    res = cube.process("mean", arguments)

or equivalently, leveraging keyword arguments::

    res = cube.process("mean", data=[1, 3, -1])


Passing data cube arguments
----------------------------

The example above is a bit convoluted however in the sense that
you start from a given data cube ``cube``, you add a ``mean`` process
that works on a given data array, while completely ignoring the original cube.
In reality you typically want to apply the process on the cube.
This is possible by passing a data cube object directly as argument,
for example with the ``ndvi`` process that at least expects
a data cube as ``data`` argument ::

    res = cube.process("ndvi", data=cube)


Note that you have have to specify ``cube`` twice here:
a first time to call the method and a second time as argument.
Moreover, it requires you to define a Python variable for the data
cube, which is annoying if you want to use a chained expressions.
To solve these issues, you can use the :const:`~openeo.rest.datacube.THIS`
constant as symbolic reference to the "current" cube::

    from openeo.rest.datacube import THIS

    res = (
        cube
            .process("filter_bands", data=THIS)
            .process("mask", data=THIS, mask=mask)
            .process("ndvi", data=THIS)
    )


Data cube from process
-----------------------

There is a convenience function
:func:`~openeo.rest.connection.Connection.datacube_from_process`
to directly create a DataCube from a single process using the Connection::

    cube = connection.datacube_from_process("mean", data=[1, 3, -1])


Working with User-Defined Processes (UDP)
==========================================

The openEO API specification allow users to define their
own **user-defined processes**, expressed in terms of other
existing pre-defined or other user-defined processes,
and to store them on the backend so they can easily be reused.

To store a user-defined process, you have to express it as
a process graph.
Where you expect input (e.g. a data cube from preceding processes),
you have to reference a *parameter* of your user-defined process
with ``{"from_parameter": "parameter_name"}``.
For example::

    blur = {
        "applykernel1": {
            "process_id": "apply_kernel",
            "arguments": {
                "data": {"from_parameter": "data"},
                "kernel": [[1, 1, 1], [1, 2, 1], [1, 1, 1]],
                "factor": 0.1,
            },
            "result": True,
        },
    }
    connection.save_user_defined_process("blur", blur)

This user-defined process can now be applied to a data cube as follows::

    res = cube.process("blur", arguments={"data": THIS}, namespace="user")


Process parameters in user-defined processes
---------------------------------------------

To keep things well-documented, it is recommended to properly list
the parameters used in your user-defined process, as
:class:`~openeo.api.process.Parameter` instances.
This also allows to specify default values.
For example, iterating on the "blur" example::

    from openeo.api.process import Parameter

    blur = {
        "applykernel1": {
            "process_id": "apply_kernel",
            "arguments": {
                "data": {"from_parameter": "data"},
                "kernel": [[1, 1, 1], [1, 2, 1], [1, 1, 1]],
                "factor": {"from_parameter": "scale"},
            },
            "result": True,
        },
    }
    connection.save_user_defined_process("blur", blur, parameters=[
        Parameter(
            name="data", description="A data cube",
            schema={"type": "object", "subtype": "raster-cube"}
        ),
        Parameter(
            name="scale", description="Kernel multiplication factor",
            schema="number", default=0.1
        ),
    ])

Because the "raster-cube" parameter is so common,
there is a helper function :func:`~openeo.api.process.Parameter.raster_cube`
to easily create such a parameter.
Also, you can specify the parameters as dictionaries if that would be
more convenient.
The parameter listing of the example above could be written like this::

    parameters=[
        Parameter.raster_cube(name="data"),
        {
            "name": "scale", "description": "Kernel multiplication factor",
            "schema": "number", "default": 0.1
        }
    ]


