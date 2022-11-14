"""


"""

__title__ = 'openeo'
__author__ = 'Jeroen Dries'


class BaseOpenEoException(Exception):
    pass


from openeo._version import __version__
from openeo.imagecollection import ImageCollection
from openeo.rest.datacube import DataCube, UDF
from openeo.rest.connection import connect, session, Connection
from openeo.rest.job import BatchJob, RESTJob


def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("openeo")
    except Exception:
        return __version__
