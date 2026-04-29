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


A process is similar to a *function* in common programming languages,
and likewise, multiple processes can be combined or chained together
into new, more complex operations.

A bit of terminology
====================

A **pre-defined process** is a process provided out of the box by a given *back-end*.
These are often the `centrally defined openEO processes <https://openeo.org/documentation/1.0/processes.html>`_,
such as common mathematical (``sum``, ``divide``, ``sqrt``, ...),
statistical (``mean``, ``max``, ...) and
image processing (``mask``, ``apply_kernel``, ...)
operations.
Back-ends are expected to support most of these standard ones,
but are free to pre-define additional ones too.


Processes can be combined into a larger pipeline, parameterized
and stored on the back-end as a so called **user-defined process**.
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

The listing of pre-defined processes provided by a back-end
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

A single pre-defined process can be retrieved with
:func:`~openeo.rest.connection.Connection.describe_process`.

Convenience methods
--------------------

Most of the important pre-defined processes are covered directly by methods
on classes like :class:`~openeo.rest.datacube.DataCube` or
:class:`~openeo.rest.vectorcube.VectorCube`.

.. seealso::
    See :ref:`openeo_process_mapping` for a mapping of openEO processes
    the corresponding methods in the openEO Python Client library.

For example, to apply the ``filter_temporal`` process to a raster data cube::

    cube = cube.filter_temporal("2020-02-20", "2020-06-06")

Being regular Python methods, you get usual function call features
you're accustomed to: default values, keyword arguments, ``kwargs`` usage, ...
For example, to use a bounding box dictionary with ``kwargs``-expansion::

    bbox = {
        "west": 5.05, "south": 51.20, "east": 5.10, "north": 51.23
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


Advanced argument tweaking
---------------------------

.. versionadded:: 0.10.1

In some situations, you may want to finetune what the (convenience) methods generate.
For example, you want to play with non-standard, experimental arguments,
or there is a problem with a automatic argument handling/conversion feature.

You can tweak the arguments of your current result node as follows.
Say, we want to add some non-standard ``feature_flags`` argument to the ``load_collection`` process node.
We first get the current result node with :py:meth:`~openeo.rest.datacube.DataCube.result_node` and use :py:meth:`~openeo.internal.graph_building.PGNode.update_arguments` to add an additional argument to it::

    # `Connection.load_collection` does not support `feature_flags` argument
    cube = connection.load_collection(...)

    # Add `feature_flag` argument `load_collection` process graph node
    cube.result_node().update_arguments(feature_flags="rXPk")

    # The resulting process graph will now contain this non-standard argument:
    #     {
    #         "process_id": "load_collection",
    #         "arguments": {
    #             ...
    #             "feature_flags": "rXPk",


Generic API for adding processes
=================================

An openEO back-end may offer processes that are not part of the core API,
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


Passing results from other process calls as arguments
------------------------------------------------------

Another use case of generically applying (custom) processes is
passing a process result as argument to another process working on a cube.
For example, assume we have a custom process ``load_my_vector_cube``
to load a vector cube from an online resource.
We can use this vector cube as geometry for
:py:meth:`DataCube.aggregate_spatial() <openeo.rest.datacube.DataCube.aggregate_spatial>`
using :py:func:`openeo.processes.process()` as follows:


.. code-block:: python

    from openeo.processes import process

    res = cube.aggregate_spatial(
        geometries=process("load_my_vector_cube", url="https://geo.example/features.db"),
        reducer="mean"
    )


.. _callbackfunctions:

Processes with child "callbacks"
================================

Some openEO processes expect some kind of sub-process
to be invoked on a subset or slice of the datacube.
For example:

*   process ``apply`` requires a transformation that will be applied
    to each pixel in the cube (separately), e.g. in pseudocode

    .. code-block:: text

        cube.apply(
            given a pixel value
            => scale it with factor 0.01
        )

*   process ``reduce_dimension`` requires an aggregation function to convert
    an array of pixel values (along a given dimension) to a single value,
    e.g. in pseudocode

    .. code-block:: text

        cube.reduce_dimension(
            given a pixel timeseries (array) for a (x,y)-location
            => temporal mean of that array
        )

*   process ``aggregate_spatial`` requires a function to aggregate the values
    in one or more geometries

These transformation functions are usually called "**callbacks**"
because instead of being called explicitly by the user,
they are called and managed by their "parent" process
(the ``apply``, ``reduce_dimension`` and ``aggregate_spatial`` in the examples)


The openEO Python Client Library currently provides a couple of DataCube methods
that expect such a callback, most commonly:

- :py:meth:`openeo.rest.datacube.DataCube.aggregate_spatial`
- :py:meth:`openeo.rest.datacube.DataCube.aggregate_temporal`
- :py:meth:`openeo.rest.datacube.DataCube.apply`
- :py:meth:`openeo.rest.datacube.DataCube.apply_dimension`
- :py:meth:`openeo.rest.datacube.DataCube.apply_neighborhood`
- :py:meth:`openeo.rest.datacube.DataCube.reduce_dimension`

The openEO Python Client Library supports several ways
to specify the desired callback for these functions:


.. contents::
   :depth: 1
   :local:
   :backlinks: top

Callback as string
------------------

The easiest way is passing a process name as a string,
for example:

.. code-block:: python

    # Take the absolute value of each pixel
    cube.apply("absolute")

    # Reduce a cube along the temporal dimension by taking the maximum value
    cube.reduce_dimension(reducer="max", dimension="t")

This approach is only possible if the desired transformation is available
as a single process. If not, use one of the methods below.

It's also important to note that the "signature" of the provided callback process
should correspond properly with what the parent process expects.
For example: ``apply`` requires a callback process that receives a
number and returns one (like ``absolute`` or ``sqrt``),
while ``reduce_dimension`` requires a callback process that receives
an array of numbers and returns a single number (like ``max`` or ``mean``).


.. _child_callback_callable:

Callback as a callable
-----------------------

You can also specify the callback as a "callable":
which is a fancy word for a Python object that can be called,
but just think of it like a function you can call.

You can use a regular Python function, like this:

.. code-block:: python

    def transform(x):
        return x * 2 + 3

    cube.apply(transform)

or, more compactly, a "lambda"
(a construct in Python to create anonymous inline functions):

.. code-block:: python

    cube.apply(lambda x: x * 2 + 3)


The openEO Python Client Library implements most of the official openEO processes as
:ref:`functions in the "openeo.processes" module <openeo_processes_functions>`,
which can be used directly as callback:

.. code-block:: python

    from openeo.processes import absolute, max

    cube.apply(absolute)
    cube.reduce_dimension(reducer=max, dimension="t")


The argument that will be passed to all these callback functions is
a :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` instance.
This is a helper object with predefined methods for all standard openEO processes,
allowing to use an object oriented coding style to define the callback.
For example:

.. code-block:: python

    from openeo.processes import ProcessBuilder

    def avg(data: ProcessBuilder):
        return data.mean()

    cube.reduce_dimension(reducer=avg, dimension="t")


These methods also return :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` objects,
which also allows writing callbacks in chained fashion:

.. code-block:: python

    cube.apply(
        lambda x: x.absolute().cos().add(y=1.23)
    )


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
:py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` methods
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

For example, if you mistakenly use the Python builtin :py:func:`sum` function
in a callback instead of :py:func:`openeo.processes.sum`, you will run into trouble.
Luckily the openEO Python client Library should raise an error if it detects that::

    >>> # Wrongly using builtin `sum` function
    >>> cube.reduce_dimension(dimension="t", reducer=sum)
    RuntimeError: Exceeded ProcessBuilder iteration limit.
    Are you mistakenly using a builtin like `sum()` or `all()` in a callback
    instead of the appropriate helpers from `openeo.processes`?

    >>> # Explicit usage of `openeo.processes.sum`
    >>> import openeo.processes
    >>> cube.reduce_dimension(dimension="t", reducer=openeo.processes.sum)
    <openeo.rest.datacube.DataCube at 0x7f6505a40d00>



Callback as ``PGNode``
-----------------------

You can also pass a :py:class:`~openeo.internal.graph_building.PGNode` object as callback.

.. attention::
    This approach should generally not be used in normal use cases.
    The other options discussed above should be preferred.
    It's mainly intended for internal use and an occasional, advanced use case.
    It requires in-depth knowledge of the openEO API
    and openEO Python Client Library to construct correctly.

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
