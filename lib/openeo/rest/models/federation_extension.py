import logging
from typing import Dict, List, Union

_log = logging.getLogger(__name__)


def get_backend_details(data: dict) -> Union[Dict[str, dict], None]:
    """
    Get federated backend details from capabilities document (``GET /``)
    at "federation" field
    """
    # TODO: return a richer object instead of raw dicts?
    return data.get("federation", None)


def get_federation_missing(data: dict, *, resource_name: str, auto_warn: bool = False) -> Union[List[str], None]:
    """
    Get "federation:missing" field from response data, if present.

    :param data: response data
    :param resource_name: name of the requested resource (listing)
    :param auto_warn: whether to automatically log a warning if missing federation components are detected.
    """
    # TODO: options to return richer objects (e.g. resolve backend id to URL/title)
    missing = data.get("federation:missing", None)
    if auto_warn and missing:
        _log.warning(f"Partial {resource_name}: missing federation components: {missing!r}.")
    return missing


def get_federation_backends(data: dict) -> Union[List[str], None]:
    """
    Get "federation:backends" field from response data, if present.
    :param data: response data
    :return:
    """
    backends = data.get("federation:backends", None)
    return backends
