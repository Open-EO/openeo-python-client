"""


"""

__title__ = 'openeo'
__author__ = 'Jeroen Dries'


class BaseOpenEoException(Exception):
    pass


from openeo._version import __version__
from openeo.imagecollection import ImageCollection
from openeo.rest.datacube import DataCube
from openeo.rest.connection import connect, session, Connection
from openeo.rest.job import BatchJob, RESTJob
from openeo.internal.graph_building import UDF


def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("openeo")
    except Exception:
        return __version__
