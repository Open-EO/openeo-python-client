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
    :members: execute_local_udf

.. automodule:: openeo.udf.debug
    :members: inspect


openeo.util
-------------

.. automodule:: openeo.util
    :members: to_bbox_dict, BBoxDict, load_json_resource, normalize_crs


openeo.processes
----------------

.. automodule:: openeo.processes
    :members: process


openeo.internal
----------------

.. automodule:: openeo.internal.graph_building
    :members: PGNode, FlatGraphableMixin
