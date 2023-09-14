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
predefined processes provided by the back-end,
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

For example, let us consider that we want to recalculate the EVI-timeseries
as done in :ref:`example-use-case-evi-map-and-timeseries` but for a different area.
Then we can either redefine the whole process or simply define it as a UDP ``EVI_timeseries``, 
that can be easily shared and reused without dealing with the entire workflow each time.


So, let's define temporal_extent,spatial_extent, and 
geometry/polygon used for pixel aggregation as parameters. 
The resulting code block will appear as follows:

.. code-block:: python

    # Load data collection
    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent = polygon,
        temporal_extent = temporal_interval,
        bands = ["B02", "B04", "B08", "SCL"],
    )
    # Extract spectral bands and calculate EVI with the "band math" feature
    blue = sentinel2_cube.band("B02") * 0.0001
    red = sentinel2_cube.band("B04") * 0.0001
    nir = sentinel2_cube.band("B08") * 0.0001
    evi = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)

    # Use the scene classification layer to mask out non-vegetation pixels
    scl = sentinel2_cube.band("SCL")
    evi_masked = evi.mask(scl != 4)

    evi_aggregation = evi_masked.aggregate_spatial(
        geometries = feature,
        reducer = "mean",
    )


Additionally, based on our application, we can also define the collection,
or even bands as parameters as shown below:

.. code-block:: python

    datacube = connection.load_collection(
            collection_id = collection,
            temporal_extent = temporal_interval
            spatial_extent = polygon,
            bands = bands
            )


Moreover, you have the flexibility to predefine any of these 
parameters as constants, just as we did earlier.

As shown above, one of the predefined processes used here is 
``load_collection``. 

We can represent this in openEO's JSON-based format as follows
(don't worry too much about the syntax details of this representation,
the openEO Python client will hide this usually).


.. code-block:: json

      {
        "process_id": "load_collection",
        "arguments": {
          "bands": [
            "B02",
            "B04",
            "B08",
            "SCL"
          ],
          "id": "SENTINEL2_L2A",
          "spatial_extent": {
            "from_parameter": "bbox"
          },
          "temporal_extent": {
            "from_parameter": "temporal_interval"
          }
        }
      }


The important point here is the parameter reference ``{"from_parameter": "bbox"}``
and ``{"from_parameter": "temporal_interval"}`` in the above process graph.
When we call this UDP, we will have to provide a value as shown below:

.. code-block:: json

    {
        "process_id": "EVI_timeseries",
        "arguments": {
          "temporal_interval": [
            "2022-09-01T00:00:00Z",
            "2022-10-01T00:00:00Z"
          ],
          "bbox": {
            "type": "Polygon",
            "coordinates": [
              [
                ...
              ]
            ]
          }
        },
        "result": true
      }

Declaring Parameters
---------------------

It's a good style to declare what parameters your user-defined process expects and supports.
It allows you to document your parameters, define the data type(s) you expect
(the "schema" in openEO-speak) and define default values.

The openEO Python client lets you define parameters as
:class:`~openeo.api.process.Parameter` instances.
In general, you have to specify at least the parameter name,
a description and a schema (to declare the expected parameter type).

The ``bbox`` and the ``temporal_interval`` parameters from the above example can be defined like this:

.. code-block:: python

    temporal_interval = Parameter(
        name = "temporal_interval",
        description = "The date range to load.",
        schema = {"type": "array", "subtype": "temporal-interval"},
        default = ["2018-06-15", "2018-06-27"]
    )
    polygon = Parameter(
        name = "bbox",
        description = "The bounding box to load.",
        schema = {"type": "object", "subtype": "geojson"}
    )

The ``feature`` parameter can be defined the same as the polygon. 

.. have_to_find_what_is_the_option_for_subtype_for_following_example:


To simplify working with parameter schemas, the :class:`~openeo.api.process.Parameter` class
provides a couple of helpers to create common types of parameters.

In the example above, the "collection_id" parameter (a string) can also be created more compactly
with the :py:meth:`Parameter.string() <openeo.api.process.Parameter.string>` helper.

.. code-block:: python

    polygon = Parameter.array(
        name = "bbox",
        description = "The bounding box to load.",
        subtype: "geojson"
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

For example, as defined for the ``EVI_timeseries``

    schema = {
        "type": "object",
        "subtype": "geojson"
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

From a parameterized datacube
-------------------------------

It's also possible to work with a :class:`~openeo.rest.datacube.DataCube` directly
and parameterize it.

Let's create, as a functional example, a custom ``load_collection``
with hardcoded collection id and band name
and a parameterized spatial extent, temporal extent and feature:


.. code-block:: python

    import openeo
    from openeo.api.process import Parameter

    # setup the connection
    connection = openeo.connect("openeo.vito.be").authenticate_oidc()

    # define the input parameter
    temporal_interval = Parameter(
        name="temporal_interval",
        description="The date range to load.",
        schema={"type": "array", "subtype": "temporal-interval"},
        default =["2018-06-15", "2018-06-27"]
    )
    polygon = Parameter(
        name="bbox",
        description="The bounding box to load.",
        schema={"type": "object", "subtype": "geojson"}
    )
    feature = Parameter(
        name="feature",
        description="Provide polygon as FeatureCollection for extracting the EVI (Enhanced Vegetation Index) timeseries for these regions.",
        schema={"type": "object", "subtype": "geojson"}
    )

    # define the process

    sentinel2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent = polygon,
        temporal_extent = temporal_interval,
        bands = ["B02", "B04", "B08", "SCL"],
    )
    # Extract spectral bands and calculate EVI with the "band math" feature
    blue = sentinel2_cube.band("B02") * 0.0001
    red = sentinel2_cube.band("B04") * 0.0001
    nir = sentinel2_cube.band("B08") * 0.0001
    evi = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)

    # Use the scene classification layer to mask out non-vegetation pixels
    scl = sentinel2_cube.band("SCL")
    evi_masked = evi.mask(scl != 4)

    evi_aggregation = evi_masked.aggregate_spatial(
        geometries = feature,
        reducer = "mean",
    )


Note how we just can pass :class:`~openeo.api.process.Parameter` objects as arguments
while building a :class:`~openeo.rest.datacube.DataCube`.

.. note::

    Not all :class:`~openeo.rest.datacube.DataCube` methods/processes properly support
    :class:`~openeo.api.process.Parameter` arguments.
    Please submit a bug report when you encounter missing or wrong parameterization support.

We can now store this as a user-defined process called "Hello_openEO" on the back-end:

.. code-block:: python         

    # Store user-defined process in openEO back-end.
    connection.save_user_defined_process(
        user_defined_process_id="EVI_timeseries",
        process_graph=evi_aggregation,
        parameters=[temporal_interval,polygon,feature],
        public="true",
    )

If you want to inspect its openEO-style process graph representation,
use the :meth:`~openeo.rest.datacube.DataCube.to_json()`
or :meth:`~openeo.rest.datacube.DataCube.print_json()` method.

.. code-block:: python

    datacube.print_json()
    
.. code-block:: json 

    {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                "bands": [
                    "B02",
                    "B04",
                    "B08",
                    "SCL"
                ],
                "id": "SENTINEL2_L2A",
                "spatial_extent": {
                    "from_parameter": "bbox"
                },
                "temporal_extent": {
                    "from_parameter": "temporal_interval"
                }
                }
            },
            "reducedimension1": {
                ...
            },
            "reducedimension2": {
                ...
            },
            "mask1": {
                ...
            },
            "aggregatespatial1": {
                ...
            }
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


Say we start from the above Python dictionary saved as JSON,
then you can simply use it using the following code and specifying the 
needed parameters:

.. code-block:: python

    shared_graph = connection.datacube_from_json(
        'shared_process_graph.json',
        parameters={
            "temporal_interval": date,
            "bbox": aoi
        }
    )

We can store this directly, taking into account that we have to define 
the bbox and temporal_interval as a parameters as done earlier. 


Store to a file
---------------

Some use cases might require storing the user-defined process,
for example, a JSON file instead of storing it directly on a back-end.
Use :py:func:`~openeo.rest.udp.build_process_dict` to build a dictionary
compatible with the "process graph with metadata" format of the openEO API
and dump it in JSON format to a file:

.. code-block:: python

    import json
    from openeo.rest.udp import build_process_dict

    spec = build_process_dict(
        process_id = "EVI_timeseries",
        process_graph = evi_aggregation,
        parameters = [temporal_interval,polygon,feature]
    )

    with open("Hello_openEO.json", "w") as f:
        json.dump(spec, f, indent=2)


.. _evaluate_udp:

Evaluate user-defined processes
================================

Let's evaluate the user-defined processes we defined.

Because there is no predefined
wrapper function for our user-defined process, we use the
generic :func:`openeo.processes.process` function to build a simple
process graph that calls our ``Hello_openEO`` process:

.. code-block:: python

    pg = openeo.processes.process(
            "EVI_timeseries",
            temporal_interval=["2018-06-15", "2018-06-27"],
            bbox={"west": 3.7, "south": 51.03, "east": 3.75, "north": 51.05},
            features = {"type": "FeatureCollection", "features": [
                        {
                            "type": "Feature", "properties": {},
                            "geometry": {"type": "Polygon", "coordinates": [[
                                [5.1417, 51.1785], [5.1414, 51.1772], [5.1444, 51.1768], [5.1443, 51.179], [5.1417, 51.1785]
                            ]]}
                        },
                        {
                            "type": "Feature", "properties": {},
                            "geometry": {"type": "Polygon", "coordinates": [[
                                [5.156, 51.1892], [5.155, 51.1855], [5.163, 51.1855], [5.163, 51.1891], [5.156, 51.1892]
                            ]]}
                        }
                    ]}
                )

Alternatively, we can also use :func:`~openeo.rest.connection.Connection.datacube_from_process`
to construct a :class:`~openeo.rest.datacube.DataCube` object
which we can process further and download:

.. code-block:: python

    created_process = conn.datacube_from_process(
        process_id = "EVI_timeseries",
        temporal_interval = date,
        box = bbox,
        feature = aoi
    )
See :ref:`datacube_from_process` for more information on :func:`~openeo.rest.connection.Connection.datacube_from_process`.

Additionally, if you wish to share your UDP with a wider audience,
you can register it in the `openEO Algorithm Plaza <https://marketplace-portal.dataspace.copernicus.eu/catalogue>`_.
The two main information you'll need to provide are the ``process_id`` and ``namespace``.
The namespace is simply a unique identifier for your process,
which you obtain as a link when using the save_process function.

And if anyone wish to use your service they will execute the following code:

.. code-block:: python

    # the public url that I received when saving the above ``EVI_timeseries`` is:
    public_url = "https://openeo.vito.be/openeo/1.1/processes/u:ecce9fea04b8c9c76ac76b45b6ba00c20f211bda4856c14aa4475b8e8ed433cd%40egi.eu/EVI_timeseries"

    created_process = conn.datacube_from_process(
        process_id = "EVI_timeseries",
        namespace = public_url
        temporal_interval = date,
        box = bbox,
        feature = aoi
    )


