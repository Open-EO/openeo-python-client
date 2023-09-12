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
        "from_parameter": "collection"
        },
        "spatial_extent": {
        "from_parameter": "aoi"
        },
        "temporal_extent": {
        "from_parameter": "temporal_interval"
        },
        "bands": {
        "from_parameter": "bands"
        }
    },
    "result": true
    }
}


The important point here is the parameter reference ``{"from_parameter": "collection"}`` or 
``{"from_parameter": "aoi"}`` in the above process graph.
When we call this user-defined process we will have to provide a value.
For example with  degrees Fahrenheit (again in openEO JSON format here)

.. code-block:: json

{
    "process_id": "download_subset",
    "arguments": {
        "bbox": {
            "west": 5.09,
            "south": 51.18,
            "east": 5.15,
            "north": 51.21
        }
    },
    "result": true
}

