.. _user-defined-processes:

***********************
User-Defined Processes
***********************


Code reuse with user-defined processes
=======================================

As explained before, processes can be chained together in a process graph
to build a certain algorithm.
Often, you have certain (sub)chains that reoccur in the same process graph
of even in different process graphs or algorithms.

The openEO API enables you to store such (sub)chains
on the backend as a so called **user-defined process**.
This allows you to build your own *library of reusable building blocks*.

.. note::

    Don not confuse user-defined processes (sometimes abbreviated as UDP) with
    **user-defined functions** (UDF) in openEO, which is a mechanism to
    inject Python or R scripts as process nodes in a process graph.
    see :ref:`user-defined-functions` for more information.

A user-defined process can not only be constructed from
pre-defined process, provided by the backend, but also
other user-defined processes.

Ultimately, the openEO API allows you to publicly expose your user-defined process,
so that other users can invoke it as a service.
This turns your process into a web application that can be run using the regular openEO
support for synchronous and asynchronous jobs.


Process Parameters
-------------------

User-defined processes are usually **parameterized**,
meaning certain inputs are expected when calling the process.

For example, if you often have to convert Fahrenheit to Celcius (``c = (f - 32) / 1.8``),
you could define a user-defined process ``fahrenheit_to_celcius``,
consisting of two simple mathematical operations
(pre-defined processes ``subtract`` and ``divide``).
We can represent this in openEO's JSON based format as follows
(don't worry too much about the syntax details of this representation,
the openEO Python client will hide this normally)::


    {
        "subtract32": {
            "process_id": "subtract",
            "arguments": {"x": {"from_parameter": "f"}, "y": 32}
        },
        "divide18": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract32"}, "y": 1.8},
            "result": true
        }
    }


The important point here is the parameter reference ``{"from_parameter": "f"}`` in the subtraction.
When we call this user-defined process, we will have to provide a Fahrenheit value,
for example with 70 degrees Fahrenheit (again in openEO JSON format here)::

    {
        "process_id": "fahrenheit_to_celcius",
        "arguments" {"f": 70}
    }


Declaring Parameters
---------------------

It's good style to declare what parameters your user-defined process expects and supports.
It allows you to document your parameters, define the data type(s) you expect
(the "schema" in openEO-speak) and define default values.

The openEO Python client lets you define parameters as
:class:`~openeo.api.process.Parameter` instances.
In general you have to specify at least the parameter name,
a description and a schema, for example take the "fahrenheit" parameter from the example above::

    from openeo.api.process import Parameter

    fahrenheit_param = Parameter(
        name="fahrenheit",
        description="Degrees Fahrenheit",
        schema={"type": "number"}
    )

The openEO Python client also defines some helper functions
to create parameters in a compact way.
For example, the "fahrenheit" parameter, which is of type "number",
can be created with the :func:`~openeo.api.process.Parameter.number` helper::

    fahrenheit_param = Parameter.number(name="fahrenheit", description="Degrees Fahrenheit")

Very often you will need a "raster-cube" type parameter,
easily created with the :func:`~openeo.api.process.Parameter.raster_cube` helper::

    cube_param = Parameter.raster_cube()

Another example of an integer parameter with a default value::

    size_param = Parameter.integer(name="size", description="Kernel size", default=4)


How you have to use these parameter instances will be explained below.

Building and storing user-defined process
=============================================

To store a user-defined process in a openEO backend,
we first have to express its process graph as a Python dictionary,
that follows the specific structure as the openEO JSON examples above.
This dictionary format can be obtained in several ways.


Predefined dictionary
----------------------

.. TODO: move this as last, most advanced option?

You might already have the process graph in dictionary format
(or JSON format, which is very close and easy to transform).
For example, another developer  prepared it for you,
or you prefer to author and finetune process graphs
in some JSON tooling environment.

Say we start from this Python dictionary, representing the Fahrenheit to Celcius conversion
(note the ``{"from_parameter": "f"}``, which refers to the input of the process)::

    fahrenheit_to_celcius = {
        "subtract1": {
            "process_id": "subtract", 
            "arguments": {"x": {"from_parameter": "f"}, "y": 32}
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": 1.8},
            "result": True
        }

We can store that directly (not that we declare the parameter too here)::

    connection.save_user_defined_process(
        user_defined_process_id="fahrenheit_to_celcius",
        process_graph=fahrenheit_to_celcius,
        parameters=[Parameter.number(name="f", description="Degrees Fahrenheit")
    )


Through "process functions"
----------------------------

The openEO Python Client Library defines the
official processes in the :py:mod:`openeo.processes` module,
which can be used to build a process graph as follows::

    from openeo.processes import subtract, divide

    # Define the input parameter.
    f = Parameter.number("f", description="Degrees Fahrenheit.")

    # Do the calculations, using the parameter and other values
    fahrenheit_to_celcius = divide(x=subtract(x=f, y=32), y=1.8)

    # Store user-defined process in openEO backend.
    connection.save_user_defined_process("fahrenheit_to_celcius", fahrenheit_to_celcius, parameters=[f])


The ``fahrenheit_to_celcius`` object encapsulates the subtract and divide calculations in a symbolic way.
We can pass it directly to :func:`~openeo.rest.connection.Connection.save_user_defined_process`.
To inspect its openEO-style process graph as a dictionary, use the ``.flatten()`` method::

    print(fahrenheit_to_celcius.flatten())
    # Prints: {
    #   'subtract1': {'process_id': 'subtract', 'arguments': {'x': {'from_parameter': 'f'}, 'y': 32}},
    #   'divide1': {'process_id': 'divide', 'arguments': {'x': {'from_node': 'subtract1'}, 'y': 1.8}, 'result': True}
    # }

Working with data cubes
------------------------

.. TODO
TODO workflow to extract UDP from working DataCube code

Evaluate user-define processes
================================

Let's evaluate the user-defined process. Because there is no pre-defined
wrapper function for our user-defined process, we use the
generic :func:`openeo.processes.process` function to build a simple
process graph, only consisting of a call to the ``fahrenheit_to_celcius`` process::


    pg = openeo.processes.process("fahrenheit_to_celcius", f=70)
    print(pg.flatten())
    # Prints: {'fahrenheittocelcius1': {'process_id': 'fahrenheit_to_celcius', 'arguments': {'f': 70}, 'result': True}}

    res = connection.execute(pg)
    print(res)
    # Prints: 21.11111111111111

Publishing your process as a service
====================================

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

To make your process usable by other users,
you can set the 'public' flag in ``save_user_defined_process`` to True.

.. warning::
    Beta feature - while the support for storing processes is defined in the API, there is
    still some work ongoing concerning how to publicly share those processes, so this is subject
    to small changes in the future. Nevertheless, we foresee that this support will be further improved.
    Related `issue <https://github.com/Open-EO/openeo-api/issues/310>`_.

This user-defined process can now be applied to a data cube as follows::

    res = cube.process("blur", arguments={"data": THIS})


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




TODO: parameter types
TODO: how to build process graph to save as udp