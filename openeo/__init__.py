"""
Python Client API for OpenEO backends. This client is a lightweight implementation with limited dependencies on other modules.
The aim of OpenEO is to process remote sensing data on dedicated processing resources close to the source data.

This client allows users to communicate with OpenEO backends, in a way that feels familiar for Python programmers.

.. literalinclude:: ../examples/download_composite.py
    :caption: Basic example
    :name: basic_example

.. image:: ../docs/images/basic_example.png
  :width: 500
  :alt: script result


In this example, a 'datacube' is created using `load_collection`. Datacubes can be transformed by applying operations directly.

"""

__title__ = 'openeo'
__author__ = 'Jeroen Dries'

from openeo._version import __version__
from openeo.imagecollection import ImageCollection
from openeo.rest.connection import connect, session, Connection
from openeo.job import Job
from openeo.internal.graph_building import UDF


def client_version() -> str:
    return __version__
