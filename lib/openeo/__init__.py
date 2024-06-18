"""


"""

__title__ = 'openeo'
__author__ = 'Jeroen Dries'


class BaseOpenEoException(Exception):
    pass


from openeo._version import __version__
from openeo.rest.connection import Connection, connect, session
from openeo.rest.datacube import UDF, DataCube
from openeo.rest.graph_building import collection_property
from openeo.rest.job import BatchJob, RESTJob


def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("openeo")
    except Exception:
        return __version__
