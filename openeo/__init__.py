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
    from openeo.capabilities import ComparableVersion
    try:
        package = 'openeo'
        response = requests.get(f'https://pypi.org/pypi/{package}/json',timeout=2)
        latest_version = response.json()['info']['version']
    except:
        # Probably no internet connection available, pass
        return
    installed_version = client_version()
    if ComparableVersion(latest_version).__gt__(installed_version):
        warnings.warn(f'You are using {package} version {installed_version}; however, version {latest_version} is available. You should consider upgrading via the \'pip install --upgrade {package}\' command.', Warning)
    return

# TODO: Perform this once a week using a cached file. pip is doing something similar here https://github.com/pypa/pip/blob/bad03ef931d9b3ff4f9e75f35f9c41f45839e2a1/src/pip/_internal/self_outdated_check.py
check_if_latest_version()
        