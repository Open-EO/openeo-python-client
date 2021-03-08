===
API
===

High level Interface
--------------------

The high-level interface tries to provide an opinionated, Pythonic, API
to interact with OpenEO backends. It's aim is to hide some of the details
of using a web service, so the user can produce concise and readable code.

Users that want to interact with OpenEO on a lower level, and have more control, can
use the lower level classes.

.. autofunction:: openeo.connect


openeo.rest.datacube
----------------------------------

.. automodule:: openeo.rest.datacube
   :members: DataCube

.. _datacube-api:


openeo.api
-----------

.. automodule:: openeo.api.process
    :members: Parameter


openeo.rest.connection
----------------------

.. automodule:: openeo.rest.connection
    :members: Connection


openeo.rest.job
----------------------

.. automodule:: openeo.rest.job
    :members: RESTJob, JobLogEntry, JobResults, ResultAsset