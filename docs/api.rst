=============
API (General)
=============

High level Interface
--------------------

The high-level interface tries to provide an opinionated, Pythonic, API
to interact with openEO back-ends. It's aim is to hide some of the details
of using a web service, so the user can produce concise and readable code.

Users that want to interact with openEO on a lower level, and have more control, can
use the lower level classes.


openeo
--------

.. autofunction:: openeo.connect


openeo.rest.datacube
-----------------------

.. automodule:: openeo.rest.datacube
   :members: DataCube
   :inherited-members:
   :special-members: __init__

.. automodule:: openeo.rest._datacube
   :members: UDF


openeo.rest.vectorcube
------------------------

.. automodule:: openeo.rest.vectorcube
   :members: VectorCube
   :inherited-members:


openeo.rest.mlmodel
---------------------

.. automodule:: openeo.rest.mlmodel
   :members: MlModel
   :inherited-members:


openeo.metadata
----------------

.. automodule:: openeo.metadata
   :members: CollectionMetadata, BandDimension, SpatialDimension, TemporalDimension


openeo.api.process
--------------------

.. automodule:: openeo.api.process
    :members: Parameter


openeo.api.logs
-----------------

.. automodule:: openeo.api.logs
    :members: LogEntry, normalize_log_level


openeo.rest.connection
----------------------

.. automodule:: openeo.rest.connection
    :members: Connection


openeo.rest.job
------------------

.. automodule:: openeo.rest.job
    :members: BatchJob, RESTJob, JobResults, ResultAsset


openeo.rest.conversions
-------------------------

.. automodule:: openeo.rest.conversions
    :members:


openeo.rest.udp
-----------------

.. automodule:: openeo.rest.udp
    :members: RESTUserDefinedProcess, build_process_dict


openeo.rest.userfile
----------------------

.. automodule:: openeo.rest.userfile
    :members:


openeo.udf
-------------

.. automodule:: openeo.udf.udf_data
    :members: UdfData

.. automodule:: openeo.udf.xarraydatacube
    :members: XarrayDataCube

.. automodule:: openeo.udf.structured_data
    :members: StructuredData

.. automodule:: openeo.udf.run_code
    :members: execute_local_udf, extract_udf_dependencies

.. automodule:: openeo.udf.debug
    :members: inspect


openeo.util
-------------

.. automodule:: openeo.util
    :members: to_bbox_dict, BBoxDict, load_json_resource, normalize_crs


openeo.processes
----------------

..  Note that only openeo.processes.process is included here
    the rest of openeo.processes is included from api-processes.rst

.. autofunction:: openeo.processes.process


Graph building
----------------

Various utilities and helpers to simplify the construction of openEO process graphs.

.. automodule:: openeo.rest.graph_building
    :members: collection_property, CollectionProperty

.. automodule:: openeo.internal.graph_building
    :members: PGNode, FlatGraphableMixin


Testing
--------

Various utilities for testing use cases (unit tests, integration tests, benchmarking, ...)

openeo.testing
``````````````

.. automodule:: openeo.testing
    :members:

openeo.testing.results
``````````````````````

.. automodule:: openeo.testing.results
    :members:
