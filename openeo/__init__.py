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
import requests
import warnings

def client_version() -> str:
    try:
        import importlib.metadata
        return importlib.metadata.version("openeo")
    except Exception:
        return __version__

def check_if_latest_version():
    try:
        package = 'openeo'
        response = requests.get(f'https://pypi.org/pypi/{package}/json')
        latest_version = response.json()['info']['version']
    except:
        # Probably no internet connection available, pass
        return
    installed_version = client_version()
    if latest_version != client_version():
        warnings.warn(f'WARNING: You are using {package} version {installed_version}; however, version {latest_version} is available. You should consider upgrading via the \'pip install --upgrade {package}\' command.', Warning)
    return

check_if_latest_version()
        