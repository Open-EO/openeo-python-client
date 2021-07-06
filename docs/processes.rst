***********************
Working with processes
***********************

In openEO, a **process** is an operation that performs a specific task on
a set of parameters and returns a result.
For example, with the ``add`` process you can add two numbers, in openEO's JSON notation::

    {
        "process_id": "add",
        "arguments": {"x": 3, "y": 5}
    }


A process is similar to a *function* in common programming languages.
and likewise, multiple processes can be combined or chained together
into new, more complex operations.

A bit of terminology
====================

A **pre-defined process** is a process provided out of the box by a given *backend*.
These are often the `centrally defined openEO processes <https://openeo.org/documentation/1.0/processes.html>`_,
such as common mathematical (``sum``, ``divide``, ``sqrt``, ...),
statistical (``mean``, ``max``, ...) and
image processing (``mask``, ``apply_kernel``, ...)
operations.
Backends are expected to support most of these standard ones,
but are free to pre-define additional ones too.


Processes can be combined into a larger pipeline, parameterized
and stored on the backend as a so called **user-defined process**.
This allows you to build a library of reusable building blocks
that can be be inserted easily in multiple other places.
See :ref:`user-defined-processes` for more information.


How processes are combined into a larger unit
is internally represented by a so-called **process graph**.
It describes how the inputs and outputs of processes
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

.. # TODO this example makes no sense: it uses cube for what?

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


Note that you have to specify ``cube`` twice here:
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



.. _callbackfunctions:

Processes with child "callbacks"
================================

Some openEO processes expect some kind of sub-process
to be invoked on a subset or slice of the datacube.
For example:

*   process ``apply`` requires a transformation that will be applied
    to each pixel in the cube (separately)
*   process ``reduce_dimension`` requires an aggregation function to convert
    an array of pixel values (along a given dimension) to a single value
*   process ``aggregate_spatial`` requires a function to aggregate the values
    in one or more geometries

These transformation functions are usually called "**callbacks**"
because instead of being called explicitly by the user,
they are called by their "parent" process
(the ``apply``, ``reduce_dimension`` and ``aggregate_spatial`` in the examples)


The openEO Python Client Library currently provides a couple of DataCube methods
that expect a callback, most commonly:

- :py:meth:`openeo.rest.datacube.DataCube.aggregate_spatial`
- :py:meth:`openeo.rest.datacube.DataCube.aggregate_temporal`
- :py:meth:`openeo.rest.datacube.DataCube.apply`
- :py:meth:`openeo.rest.datacube.DataCube.apply_dimension`
- :py:meth:`openeo.rest.datacube.DataCube.apply_neighborhood`
- :py:meth:`openeo.rest.datacube.DataCube.reduce_dimension`

These functions support several ways to specify the desired callback.


Callback as string
------------------

The easiest way is passing a process name as a string,
for example:

.. code-block:: python

    # Take the absolute value of each pixel
    cube.apply("absolute")

    # Reduce a cube along the temporal dimension by taking the maximum value
    cube.reduce_dimension("max", dimension="t")

This approach is only possible if the desired transformation is available
as a single process. If not, use one of the methods below.

Also important is that the "signature" of the provided callback process
should correspond properly with what the parent process expects.
For example: ``apply`` requires a callback process that receives a
number and returns one (like ``absolute`` or ``sqrt``),
while ``reduce_dimension`` requires a callback process that receives
an array of numbers and returns a single number (like ``max`` or ``mean``).


.. _child_callback_callable:

Callback as a callable
-----------------------

You can also specify the callback as a "callable":
a Python object that can be called (e.g. a function without parenthesis).

The openEO Python Client Library defines the
official processes in the :py:mod:`openeo.processes` module,
which can be used directly:

.. code-block:: python

    from openeo.processes import absolute, max

    cube.apply(absolute)
    cube.reduce_dimension(max, dimension="t")

You can also use ``lambda`` functions:

.. code-block:: python

    cube.apply(lambda x: x * 2 + 3)


or normal Python functions:

.. code-block:: python

    from openeo.processes import array_element

    def my_bandmath(data):
        band1 = array_element(data, index=0)
        band2 = array_element(data, index=1)
        return band1 + 1.2 * band2


    cube.reduce_dimension(my_bandmath, dimension="bands")


The argument that is passed to these functions is
an instance of :py:class:`openeo.processes.ProcessBuilder`.
This is a helper object with predefined methods for all standard processes,
allowing to use an object oriented coding style to define the callback.
For example:

.. code-block:: python

    from openeo.processes import ProcessBuilder

    def avg(data: ProcessBuilder):
        return data.mean()

    cube.reduce_dimension(avg, dimension="t")


These methods also return ``ProcessBuilder`` objects,
which also allows writing callbacks in chained fashion:

.. code-block:: python

    cube.apply(lambda x: x.absolute().cos().add(y=1.23))


All this gives a lot of flexibility to define callbacks compactly
in a desired coding style.
The following examples result in the same callback:

.. code-block:: python

    from openeo.processes import ProcessBuilder, mean, cos, add

    # Chained methods
    cube.reduce_dimension(
        lambda data: data.mean().cos().add(y=1.23),
        dimension="t"
    )

    # Functions
    cube.reduce_dimension(
        lambda data: add(x=cos(mean(data)), y=1.23),
        dimension="t"
    )

    # Mixing methods, functions and operators
    cube.reduce_dimension(
        lambda data: cos(data.mean())) + 1.23,
        dimension="t"
    )


Caveats
````````

Specifying callbacks through Python functions (or lambdas)
looks intuitive and straightforward, but it should be noted
that not everything is allowed in these functions.
You should just limit yourself to calling
:py:mod:`openeo.processes` functions,
:py:class:`openeo.processes.ProcessBuilder` methods
and basic math operators.
Don't call functions from other libraries like numpy or scipy.
Don't use Python control flow statements like ``if/else`` constructs
or ``for`` loops.

The reason for this is that the openEO Python Client Library
does not translate the function source code itself
to an openEO process graph.
Instead, when building the openEO process graph,
it passes a special object to the function
and keeps track of which :py:mod:`openeo.processes` functions
were called to assemble the corresponding process graph.
If you use control flow statements or use numpy functions for example,
this procedure will incorrectly detect what you want to do in the callback.


Callback as ``PGNode``
-----------------------

You can also pass a ``PGNode`` object as callback.
This method is used internally and could be useful for more
advanced use cases, but it requires more in-depth knowledge of
the openEO API and openEO Python Client Library to construct correctly.
Some examples:

.. code-block:: python

    from openeo.internal.graph_building import PGNode

    cube.apply(PGNode(
        "add",
        x=PGNode(
            "cos",
            x=PGNode("absolute", x={"from_parameter": "x"})
        ),
        y=1.23
    ))

    cube.reduce_dimension(
        reducer=PGNode("max", data={"from_parameter": "data"}),
        dimension="bands"
    )
