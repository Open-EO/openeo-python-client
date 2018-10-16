"""
Python Client API for OpenEO backends. This client is a lightweight implementation with limited dependencies on other modules.
The aim of OpenEO is to process remote sensing data on dedicated processing resources close to the source data.

This client allows users to communicate with OpenEO backends, in a way that feels familiar for Python programmers.

.. literalinclude:: ../examples/download_composite.py
    :caption: Basic example
    :name: basic_example

"""

__title__ = 'openeo'
__version__ = '0.0.3'
__author__ = 'Jeroen Dries'

from .catalog import EOProduct
from .imagecollection import ImageCollection
from .rest.rest_connection import connection as connect
from .job import Job
from .auth.auth import Auth
from .process.process import *

# TODO: make some kind of config file for the client version
CLIENT_VERSION = "0.3.0"


def version():
    return CLIENT_VERSION
