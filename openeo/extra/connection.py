# TODO: find the correct place for this stuff. This file was moved from openeo-classification.

from functools import partial, lru_cache

import openeo
from openeo import Connection

_default_url = "openeo-dev.vito.be"


def set_backend(url):
    _default_url = url


@lru_cache(maxsize=None)
def _cached_connection(url) -> Connection:
    return openeo.connect(url).authenticate_oidc()


def connection(url=_default_url) -> Connection:
    """
    Returns an authenticated openEO connection.
    Connects to openEO platform by default, but others can be used as well by specifying the url.

    @param url:
    @return:
    """
    return _cached_connection(url)


terrascope_dev = partial(connection, "openeo-dev.vito.be")
openeo_platform = partial(connection, "openeo.cloud")
creo = partial(connection, "openeo-dev.creo.vito.be")
