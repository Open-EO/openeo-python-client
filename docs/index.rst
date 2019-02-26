.. openeo documentation master file, created by
   sphinx-quickstart on Fri Oct  6 13:02:27 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Open-EO Client
==============


.. image:: https://img.shields.io/badge/Status-proof--of--concept-yellow.svg

Usage
==================================

.. automodule:: openeo

.. toctree::
   :maxdepth: 3
   :caption: Contents:

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

.. automodule:: openeo.connection
   :members:

.. automodule:: openeo.imagecollection
   :members:

.. automodule:: openeo.job
   :members:


Authentication
--------------

.. automodule:: openeo.auth.auth
   :members:

.. automodule:: openeo.auth.auth_bearer
   :members:

.. automodule:: openeo.auth.auth_none

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
