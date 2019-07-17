"""
Python Client API for OpenEO backends. This client is a lightweight implementation with limited dependencies on other modules.
The aim of OpenEO is to process remote sensing data on dedicated processing resources close to the source data.

This client allows users to communicate with OpenEO backends, in a way that feels familiar for Python programmers.

.. literalinclude:: ../examples/download_composite.py
    :caption: Basic example
    :name: basic_example

"""

__title__ = 'openeo'
__author__ = 'Jeroen Dries'

from openeo._version import __version__
from openeo.catalog import EOProduct
from openeo.imagecollection import ImageCollection
from openeo.rest.rest_connection import connection as connect
from openeo.rest.rest_connection import session
from openeo.job import Job
from openeo.auth.auth import Auth
from openeo.process.process import *


def client_version():
    return __version__
