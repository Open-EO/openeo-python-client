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
meaning specific inputs might be needed when calling the process. 
It allows the user to perform specific tasks based on the provided inputs.

For example, let us consider that we want to define a UDP ``download_subset`` as a service, 
so that anyone can use it to download their choice of collection availble in openEO for a 
defined area of interest. 

.. code-block:: python
    datacube = connection.load_collection(
                collection_id = collection_param
                )


Nevertheless, based on our application we can also define area of interest, 
temporal extent or even bands as parameter as shown below:

.. code-block:: python
    datacube = connection.load_collection(
                collection_id = collection,
                temporal_extent = temporal_interval
                spatial_extent = aoi,
                bands = bands
                )


Moreover, you have the flexibility to pre-define any of these 
parmeters as fixed. Similar is the case if you want to define 
as constant variable.

As shown above, the only pre-defined process used here is 
``load_collection``. 

We can represent this in openEO's JSON based format as follows
(don't worry too much about the syntax details of this representation,
the openEO Python client will hide this normally).


.. code-block:: json
    {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {
                "from_parameter": "collection_id"
                }
            },
            "result": true
        }
    }


The important point here is the parameter reference ``{"from_parameter": "collection_id"}`` or 
``{"from_parameter": "aoi"}`` in the above process graph.
When we call this user-defined process we will have to provide a value.

.. code-block:: json
    {
        "process_id": "download_subset",
        "arguments": {
            "collection_id": "SENTINEL2_L2A"
        },
        "result": true
    }

Declaring Parameters
---------------------

It's good style to declare what parameters your user-defined process expects and supports.
It allows you to document your parameters, define the data type(s) you expect
(the "schema" in openEO-speak) and define default values.

The openEO Python client lets you define parameters as
:class:`~openeo.api.process.Parameter` instances.
In general you have to specify at least the parameter name,
a description and a schema (to declare the expected parameter type).

The ``collection_id`` parameters from the above example can be defined like this:

.. code-block:: python
    collection = Parameter(
                name="collection_id",
                description="The openEO collecion_id. ",
                schema={"type": "string", "subtype": "collection-id", "enum": ["SENTINEL2_L2A"]},
                optional="true",
                default="SENTINEL2_L2A",
    )



########################### Have to find what is the option for subtype for following example


To simplify working with parameter schemas, the :class:`~openeo.api.process.Parameter` class
provides a couple of helpers to create common types of parameters.

In the example above, the "collection_id" parameter (a string) can also be created more compactly
with the :py:meth:`Parameter.string() <openeo.api.process.Parameter.string>` helper.

.. code-block:: python
    collection = Parameter.string(
                name = "collection_id",
                description = "The interested openEO collecion_id.",
                default = "SENTINEL2_L2A"
    )
.. _build_and_store_udp:

Building and storing user-defined process
=============================================

There are a couple of ways to build and store user-defined processes:

- using predefined :ref:`process functions <create_udp_through_process_functions>`
- :ref:`parameterized building of a datacube <create_udp_parameterized_cube>`
- :ref:`directly from a well-formatted dictionary <create_udp_from_dict>` process graph representation



.. _create_udp_through_process_functions:

Through "process functions"
----------------------------

The openEO Python Client Library defines the
official processes in the :py:mod:`openeo.processes` module,
which can be used to build a process graph as follows::

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
    to create a datacube parameter.

Consult the documentation of these helper class methods for additional features.



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
e.g. an array of integers:

.. code-block:: json
    {
        "type": "array",
        "items": {"type": "integer"}
    }

Another, more complex type is ``{"type": "object"}`` for parameters
that are like Python dictionaries (or mappings).
For example, to define a bounding box parameter
that should contain certain fields with certain type::

.. code-block:: json
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

For example, as defined for the ``download_subset``

.. code-block:: python
    schema = {
        "type": "string",
        "subtype": "collection-id",
    }
    
Additionally, the schema of an openEO datacube is:

.. code-block:: json
    {
        "type": "object",
        "subtype": "datacube"
    }



.. _build_and_store_udp:

Building, saving and storing user-defined process
=============================================

There are a couple of ways to build and store user-defined processes:

- using predefined :ref:`process functions <create_udp_through_process_functions>`
- :ref:`parameterized building of a datacube <create_udp_parameterized_cube>`
- :ref:`directly from a well-formatted dictionary <create_udp_from_dict>` process graph representation



.. _create_udp_through_process_functions:

Build and save using "process functions"
----------------------------

The openEO Python Client Library defines the
official processes in the :py:mod:`openeo.processes` module,
which can be used to build a process graph as follows:

.. code-block:: python
    import openeo
    from openeo.api.process import Parameter

    # setup the connection
    connection = openeo.connect("openeo.cloud").authenticate_oidc()

    # define the input parameter
    collection = Parameter(
                name="collection_id",
                description="The openEO collection ID. ",
                schema={"type": "string", "subtype": "collection-id", "enum": ["SENTINEL2_L2A"]},
                optional="true",
                default="SENTINEL2_L2A",
            )

    # define the process
    datacube = connection.load_collection(
                collection,
                temporal_extent=["2018-06-15", "2018-06-27"],
                spatial_extent={
                    "west": 5.09,
                    "south": 51.18,
                    "east": 5.15,
                    "north": 51.21,
                    "crs": 4326,
                },
            )

    # Store user-defined process in openEO back-end.
    connection.save_user_defined_process(
                user_defined_process_id = "Hello_openEO",
                process_graph = datacube,
                parameters = [collection],
                public = "true",
            )


In the above example the ``datacube`` object encapsulates our entire process. Whereas,
if your task includes multiples processes, the final datacube should be passed.
Thus, we can pass datacube directly to :py:meth:`~openeo.rest.connection.Connection.save_user_defined_process`.

Furthermore, If you want to inspect its openEO-style process graph representation,
use the :meth:`~openeo.rest.datacube.DataCube.to_json()`
or :meth:`~openeo.rest.datacube.DataCube.print_json()` method:

.. code-block:: python
    datacube.print_json()
.. code-block:: json
    {
    "process_graph": {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": {
                "from_parameter": "collection_id"
                },
                "spatial_extent": {
                "west": 5.09,
                "south": 51.18,
                "east": 5.15,
                "north": 51.21,
                "crs": 4326
                },
                "temporal_extent": [
                "2018-06-15",
                "2018-06-27"
                ]
            },
            "result": true
            }
    	 }
    }

.. _create_udp_parameterized_cube:

From a parameterized datacube
-------------------------------

It's also possible to work with a :class:`~openeo.rest.datacube.DataCube` directly
and parameterize it.

Let's create, as a simple but functional example, a custom ``load_collection``
with hardcoded collection id and band name
and a parameterized spatial extent (with default):


.. code-block:: python
    #define the parameters
    spatial_extent = Parameter(
        name="bbox",
        schema="object",
        default={"west": 3.7, "south": 51.03, "east": 3.75, "north": 51.05}
    )
    temporal_interval = Parameter(
        name="temporal_interval",
        description="The date range to load.",
        schema={"type": "array", "subtype": "temporal-interval"},
        default=["2018-06-15", "2018-06-27"]
    )
    #define the datacube
    datacube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=spatial_extent,
        temporal_extent=temporal_interval
    )

Note how we just can pass :class:`~openeo.api.process.Parameter` objects as arguments
while building a :class:`~openeo.rest.datacube.DataCube`.

.. note::

    Not all :class:`~openeo.rest.datacube.DataCube` methods/processes properly support
    :class:`~openeo.api.process.Parameter` arguments.
    Please submit a bug report when you encounter missing or wrong parameterization support.

We can now store this as a user-defined process called "Hello_openEO" on the back-end::

.. code-block:: python
    connection.save_user_defined_process(
        "Hello_openEO",
        datacube,
        parameters=[spatial_extent,temporal_interval]
    )

If you want to inspect its openEO-style process graph representation,
use the :meth:`~openeo.rest.datacube.DataCube.to_json()`
or :meth:`~openeo.rest.datacube.DataCube.print_json()` method::

.. code-block:: python
    datacube.print_json()
    
.. code-block:: json   
    {
      "loadcollection1": {
        "process_id": "load_collection",
        "arguments": {
          "id": "SENTINEL2_L2A",
          "bands": [
            "B04"
          ],
          "spatial_extent": {
            "from_parameter": "bbox"
          },
          "temporal_extent": {
            "from_parameter": "temporal_interval"
          }
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

.. code-block:: python
    datacube =     {
      "loadcollection1": {
        "process_id": "load_collection",
        "arguments": {
          "id": "SENTINEL2_L2A",
          "bands": [
            "B04"
          ],
          "spatial_extent": {
            "from_parameter": "bbox"
          },
          "temporal_extent": {
            "from_parameter": "temporal_interval"
          }
        },
        "result": true
      }
    }

We can store this directly, taking into account that we have to defined 
the bbox and temporal_interval as a parameters as done earlier. Then,
pass datacube directly to :py:meth:`~openeo.rest.connection.Connection.save_user_defined_process`.

Store to a file
---------------

Some use cases might require storing the user-defined process in,
for example, a JSON file instead of storing it directly on a back-end.
Use :py:func:`~openeo.rest.udp.build_process_dict` to build a dictionary
compatible with the "process graph with metadata" format of the openEO API
and dump it in JSON format to a file:

.. code-block:: python
    import json
    from openeo.rest.udp import build_process_dict

    spec = build_process_dict(
        process_id="Hello openEO",
        process_graph=datacube,
        parameters=[spatial_extent,temporal_interval]
    )

    with open("Hello_openEO.json", "w") as f:
        json.dump(spec, f, indent=2)


.. _evaluate_udp:

Evaluate user-defined processes
================================

Let's evaluate the user-defined processes we defined.

Because there is no pre-defined
wrapper function for our user-defined process, we use the
generic :func:`openeo.processes.process` function to build a simple
process graph that calls our ``Hello_openEO`` process:

.. code-block:: python
    pg = openeo.processes.process("Hello_openEO", temporal_interval=["2018-06-15", "2018-06-27"], bbox={"west": 3.7, "south": 51.03, "east": 3.75, "north": 51.05})

Alternatively, we can also use :func:`~openeo.rest.connection.Connection.datacube_from_process`
to construct a :class:`~openeo.rest.datacube.DataCube` object
which we can process further and download::

    datacube = connection.datacube_from_process("Hello_openEO", temporal_interval=["2018-06-15", "2018-06-27"], bbox={"west": 3.7, "south": 51.03, "east": 3.75, "north": 51.05})

See :ref:`datacube_from_process` for more information on :func:`~openeo.rest.connection.Connection.datacube_from_process`.
