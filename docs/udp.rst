.. _user-defined-processes:

############################
User-Defined Processes (UDP)
############################


Code reuse with user-defined processes
=======================================

As explained before, processes can be chained together in a process graph
to build a certain algorithm.
Often, you have certain (sub)chains that reoccur in the same process graph
of even in different process graphs or algorithms.

The openEO API enables you to store such (sub)chains
on the back-end as a so called **user-defined process**.
This allows you to build your own *library of reusable building blocks*.

.. warning::

    Do not confuse **user-defined processes** (sometimes abbreviated as UDP) with
    **user-defined functions** (UDF) in openEO, which is a mechanism to
    inject Python or R scripts as process nodes in a process graph.
    See :ref:`user-defined-functions` for more information.

A user-defined process can not only be constructed from
pre-defined processes provided by the back-end,
but also other user-defined processes.

Ultimately, the openEO API allows you to publicly expose your user-defined process,
so that other users can invoke it as a service.
This turns your openEO process into a web application
that can be executed using the regular openEO
support for synchronous and asynchronous jobs.


Process Parameters
====================

User-defined processes are usually **parameterized**,
meaning certain inputs are expected when calling the process.

For example, if you often have to convert Fahrenheit to Celsius::

    c = (f - 32) / 1.8

you could define a user-defined process ``fahrenheit_to_celsius``,
consisting of two simple mathematical operations
(pre-defined processes ``subtract`` and ``divide``).

We can represent this in openEO's JSON based format as follows
(don't worry too much about the syntax details of this representation,
the openEO Python client will hide this usually)::


    {
        "subtract32": {
            "process_id": "subtract",
            "arguments": {"x": {"from_parameter": "fahrenheit"}, "y": 32}
        },
        "divide18": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract32"}, "y": 1.8},
            "result": true
        }
    }


The important point here is the parameter reference ``{"from_parameter": "fahrenheit"}`` in the subtraction.
When we call this user-defined process we will have to provide a Fahrenheit value.
For example with 70 degrees Fahrenheit (again in openEO JSON format here)::

    {
        "process_id": "fahrenheit_to_celsius",
        "arguments" {"fahrenheit": 70}
    }


.. _udp-declaring-parameters:

Declaring Parameters
---------------------

It's good style to declare what parameters your user-defined process expects and supports.
It allows you to document your parameters, define the data type(s) you expect
(the "schema" in openEO-speak) and define default values.

The openEO Python client lets you define parameters as
:class:`~openeo.api.process.Parameter` instances.
In general you have to specify at least the parameter name,
a description and a schema (to declare the expected parameter type).
The "fahrenheit" parameter from the example above can be defined like this::

    from openeo.api.process import Parameter

    fahrenheit_param = Parameter(
        name="fahrenheit",
        description="Degrees Fahrenheit",
        schema={"type": "number"}
    )

To simplify working with parameter schemas, the :class:`~openeo.api.process.Parameter` class
provides a couple of helpers to create common types of parameters.
In the example above, the "fahrenheit" parameter (a number) can also be created more compactly
with the :py:meth:`Parameter.number() <openeo.api.process.Parameter.number>` helper::

    fahrenheit_param = Parameter.number(
        name="fahrenheit", description="Degrees Fahrenheit"
    )

Some useful parameter helpers (class methods of the :py:class:`~openeo.api.process.Parameter` class):

-   :py:meth:`Parameter.string() <openeo.api.process.Parameter.string>`
    to create a string parameter,
    e.g. to parameterize the collection id in a ``load_collection`` call in your UDP.
-   :py:meth:`Parameter.integer() <openeo.api.process.Parameter.integer>`,
    :py:meth:`Parameter.number() <openeo.api.process.Parameter.number>`,
    and :py:meth:`Parameter.boolean() <openeo.api.process.Parameter.boolean>`
    to create integer, floating point, or boolean parameters respectively.
-   :py:meth:`Parameter.array() <openeo.api.process.Parameter.array>`
    to create an array parameter,
    e.g. to parameterize the a band selection  in a ``load_collection`` call in your UDP.
-   :py:meth:`Parameter.datacube() <openeo.api.process.Parameter.datacube>`
    (or its legacy, deprecated cousin :py:meth:`Parameter.raster_cube() <openeo.api.process.Parameter.raster_cube>`)
    to create a data cube parameter.
-   :py:meth:`Parameter.bounding_box() <openeo.api.process.Parameter.bounding_box>` to create
    a parameter for specifying a spatial extent with "west", "south", "east", "north" bounds.
-   :py:meth:`Parameter.date() <openeo.api.process.Parameter.date>` and
    :py:meth:`Parameter.date_time() <openeo.api.process.Parameter.date_time>`
    to create date or date+time parameters.
-   :py:meth:`Parameter.temporal_interval() <openeo.api.process.Parameter.temporal_interval>` to create
    a parameter for specifying a temporal interval with "start" and "end" dates.
-   :py:meth:`Parameter.geojson() <openeo.api.process.Parameter.geojson>` to create
    a parameter for specifying a GeoJSON geometry.



Consult the documentation of these helper class methods for additional features.
For example, declaring a default value for an integer parameter::

    size_param = Parameter.integer(
        name="size", description="Kernel size", default=4
    )



More advanced parameter schemas
--------------------------------

While the helper class methods of :py:class:`~openeo.api.process.Parameter` (discussed above)
cover the most common parameter usage,
you also might need to declare some parameters with a more special or specific schema.
You can do that through the ``schema`` argument
of the basic :py:class:`~openeo.api.process.Parameter()` constructor.
This "schema" argument follows the `JSON Schema draft-07 <https://json-schema.org/>`_ specification,
which we will briefly illustrate here.

Basic primitives can be declared through a (required) "type" field, for example:
``{"type": "string"}`` for strings, ``{"type": "integer"}`` for integers, etc.

Likewise, arrays can be defined with a minimal ``{"type": "array"}``.
In addition, the expected type of the array items can also be specified,
e.g. an array of integers::

    {
        "type": "array",
        "items": {"type": "integer"}
    }

Another, more complex type is ``{"type": "object"}`` for parameters
that are like Python dictionaries (or mappings).
For example, to define a bounding box parameter
that should contain certain fields with certain type::

    {
        "type": "object",
        "properties": {
            "west": {"type": "number"},
            "south": {"type": "number"},
            "east": {"type": "number"},
            "north": {"type": "number"},
            "crs": {"type": "string"}
        }
    }

Check the documentation and examples of `JSON Schema draft-07 <https://json-schema.org/>`_
for even more features.

On top of these generic types, the openEO API also defines a couple of custom (sub)types
in the `openeo-processes project <https://github.com/Open-EO/openeo-processes>`_
(see the ``meta/subtype-schemas.json`` listing).
For example, the schema of an openEO data cube is::

    {
        "type": "object",
        "subtype": "datacube"
    }



.. _build_and_store_udp:

Building and storing user-defined process
=============================================

There are a couple of ways to build and store user-defined processes:

- using predefined :ref:`process functions <create_udp_through_process_functions>`
- :ref:`parameterized building of a data cube <create_udp_parameterized_cube>`
- :ref:`directly from a well-formatted dictionary <create_udp_from_dict>` process graph representation



.. _create_udp_through_process_functions:

Through "process functions"
----------------------------

The openEO Python Client Library defines the
official processes in the :py:mod:`openeo.processes` module,
which can be used to build a process graph as follows::

    from openeo.processes import subtract, divide
    from openeo.api.process import Parameter

    # Define the input parameter.
    f = Parameter.number("f", description="Degrees Fahrenheit.")

    # Do the calculations, using the parameter and other values
    fahrenheit_to_celsius = divide(x=subtract(x=f, y=32), y=1.8)

    # Store user-defined process in openEO back-end.
    connection.save_user_defined_process(
        "fahrenheit_to_celsius",
        fahrenheit_to_celsius,
        parameters=[f]
    )


The ``fahrenheit_to_celsius`` object encapsulates the subtract and divide calculations in a symbolic way.
We can pass it directly to :py:meth:`~openeo.rest.connection.Connection.save_user_defined_process`.


If you want to inspect its openEO-style process graph representation,
use the :meth:`~openeo.rest.datacube.DataCube.to_json()`
or :meth:`~openeo.rest.datacube.DataCube.print_json()` method::

    >>> fahrenheit_to_celsius.print_json()
    {
      "process_graph": {
        "subtract1": {
          "process_id": "subtract",
          "arguments": {
            "x": {
              "from_parameter": "f"
            },
            "y": 32
          }
        },
        "divide1": {
          "process_id": "divide",
          "arguments": {
            "x": {
              "from_node": "subtract1"
            },
            "y": 1.8
          },
          "result": true
        }
      }
    }


.. _create_udp_parameterized_cube:

From a parameterized data cube
-------------------------------

It's also possible to work with a :class:`~openeo.rest.datacube.DataCube` directly
and parameterize it.
Let's create, as a simple but functional example, a custom ``load_collection``
with hardcoded collection id and band name
and a parameterized spatial extent (with default)::

    spatial_extent = Parameter(
        name="bbox",
        schema="object",
        default={"west": 3.7, "south": 51.03, "east": 3.75, "north": 51.05}
    )

    cube = connection.load_collection(
        "SENTINEL2_L2A_SENTINELHUB",
        spatial_extent=spatial_extent,
        bands=["B04"]
    )

Note how we just can pass :class:`~openeo.api.process.Parameter` objects as arguments
while building a :class:`~openeo.rest.datacube.DataCube`.

.. note::

    Not all :class:`~openeo.rest.datacube.DataCube` methods/processes properly support
    :class:`~openeo.api.process.Parameter` arguments.
    Please submit a bug report when you encounter missing or wrong parameterization support.

We can now store this as a user-defined process called "fancy_load_collection" on the back-end::

    connection.save_user_defined_process(
        "fancy_load_collection",
        cube,
        parameters=[spatial_extent]
    )

If you want to inspect its openEO-style process graph representation,
use the :meth:`~openeo.rest.datacube.DataCube.to_json()`
or :meth:`~openeo.rest.datacube.DataCube.print_json()` method::

    >>> cube.print_json()
    {
      "loadcollection1": {
        "process_id": "load_collection",
        "arguments": {
          "id": "SENTINEL2_L2A_SENTINELHUB",
          "bands": [
            "B04"
          ],
          "spatial_extent": {
            "from_parameter": "bbox"
          },
          "temporal_extent": null
        },
        "result": true
      }
    }



.. _create_udp_from_dict:

Using a predefined dictionary
------------------------------

In some (advanced) situation, you might already have
the process graph in dictionary format
(or JSON format, which is very close and easy to transform).
Another developer already prepared it for you,
or you prefer to fine-tune process graphs in a JSON editor.
It is very straightforward to submit this as a user-defined process.

Say we start from the following Python dictionary,
representing the Fahrenheit to Celsius conversion we discussed before::

    fahrenheit_to_celsius = {
        "subtract1": {
            "process_id": "subtract",
            "arguments": {"x": {"from_parameter": "f"}, "y": 32}
        },
        "divide1": {
            "process_id": "divide",
            "arguments": {"x": {"from_node": "subtract1"}, "y": 1.8},
            "result": True
        }}

We can store this directly, taking into account that we have to define
a parameter named ``f`` corresponding with the ``{"from_parameter": "f"}`` argument
from the dictionary above::

    connection.save_user_defined_process(
        user_defined_process_id="fahrenheit_to_celsius",
        process_graph=fahrenheit_to_celsius,
        parameters=[Parameter.number(name="f", description="Degrees Fahrenheit")]
    )


Store to a file
---------------

Some use cases might require storing the user-defined process in,
for example, a JSON file instead of storing it directly on a back-end.
Use :py:func:`~openeo.rest.udp.build_process_dict` to build a dictionary
compatible with the "process graph with metadata" format of the openEO API
and dump it in JSON format to a file::

    import json
    from openeo.rest.udp import build_process_dict
    from openeo.processes import subtract, divide
    from openeo.api.process import Parameter

    fahrenheit = Parameter.number("f", description="Degrees Fahrenheit.")
    fahrenheit_to_celsius = divide(x=subtract(x=fahrenheit, y=32), y=1.8)

    spec = build_process_dict(
        process_id="fahrenheit_to_celsius",
        process_graph=fahrenheit_to_celsius,
        parameters=[fahrenheit]
    )

    with open("fahrenheit_to_celsius.json", "w") as f:
        json.dump(spec, f, indent=2)

This results in a JSON file like this::

    {
      "id": "fahrenheit_to_celsius",
      "process_graph": {
        "subtract1": {
          "process_id": "subtract",
           ...
      "parameters": [
        {
          "name": "f",
          ...


.. _evaluate_udp:

Evaluate user-defined processes
================================

Let's evaluate the user-defined processes we defined.

Because there is no pre-defined
wrapper function for our user-defined process, we use the
generic :func:`openeo.processes.process` function to build a simple
process graph that calls our ``fahrenheit_to_celsius`` process::

    >>> pg = openeo.processes.process("fahrenheit_to_celsius", f=70)
    >>> pg.print_json(indent=None)
    {"process_graph": {"fahrenheittocelsius1": {"process_id": "fahrenheit_to_celsius", "arguments": {"f": 70}, "result": true}}}

    >>> res = connection.execute(pg)
    >>> print(res)
    21.11111111111111


To use our custom ``fancy_load_collection`` process,
we only have to specify a temporal extent,
and let the predefined and default values do their work.
We will use :func:`~openeo.rest.connection.Connection.datacube_from_process`
to construct a :class:`~openeo.rest.datacube.DataCube` object
which we can process further and download::

    cube = connection.datacube_from_process("fancy_load_collection")
    cube = cube.filter_temporal("2020-09-01", "2020-09-10")
    cube.download("fancy.tiff", format="GTiff")

See :ref:`datacube_from_process` for more information on :func:`~openeo.rest.connection.Connection.datacube_from_process`.


.. _udp_example_evi:

UDP Example: EVI timeseries
==========================================

In this UDP example, we'll build a reusable UDP ``evi_timeseries``
to calculate the EVI timeseries for a given geometry.
It's a simplified version of the EVI workflow laid out in :ref:`basic_example_evi_map_and_timeseries`,
focussing on the UDP-specific aspects: defining and using parameters;
building, storing, and finally executing the UDP.

.. code-block:: python

    import openeo
    from openeo.api.process import Parameter

    # Create connection to openEO back-end
    connection = openeo.connect("...").authenticate_oidc()

    # Declare the UDP parameters
    temporal_extent = Parameter(
        name="temporal_extent",
        description="The date range to calculate the EVI for.",
        schema={"type": "array", "subtype": "temporal-interval"},
        default =["2018-06-15", "2018-06-27"]
    )
    geometry = Parameter(
        name="geometry",
        description="The geometry (a single (multi)polygon or a feature collection of (multi)polygons) of to calculate the EVI for.",
        schema={"type": "object", "subtype": "geojson"}
    )

    # Load raw SENTINEL2_L2A data
    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        temporal_extent=temporal_extent,
        bands=["B02", "B04", "B08"],
    )

    # Extract spectral bands and calculate EVI with the "band math" feature
    blue = sentinel2_cube.band("B02") * 0.0001
    red = sentinel2_cube.band("B04") * 0.0001
    nir = sentinel2_cube.band("B08") * 0.0001
    evi = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)

    evi_aggregation = evi.aggregate_spatial(
        geometries=geometry,
        reducer="mean",
    )

    # Store the parameterized user-defined process at openEO back-end.
    process_id = "evi_timeseries"
    connection.save_user_defined_process(
        user_defined_process_id=process_id,
        process_graph=evi_aggregation,
        parameters=[temporal_interval, geometry],
    )

When this UDP ``evi_timeseries`` is successfully stored on the back-end,
we can use it through :func:`~openeo.rest.connection.Connection.datacube_from_process`
to get the EVI timeseries of a desired geometry and time window:

.. code-block:: python

    time_window = ["2020-01-01", "2021-12-31"]
    geometry = {
        "type": "Polygon",
        "coordinates": [[[5.1793, 51.2498], [5.1787, 51.2467], [5.1852, 51.2450], [5.1867, 51.2453], [5.1873, 51.2491], [5.1793, 51.2498]]],
      }

    evi_timeseries = connection.datacube_from_process(
        process_id="evi_timeseries",
        temporal_extent=time_window,
        geometry=geometry,
    )

    evi_timeseries.download("evi-aggregation.json")
