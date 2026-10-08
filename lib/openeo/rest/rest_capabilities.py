import warnings

from openeo.internal.warnings import UserDeprecationWarning
from openeo.rest.capabilities import OpenEoCapabilities

warnings.warn(
    message="`RESTCapabilities` from `openeo.rest.rest_capabilities` is deprecated. Instead use `OpenEoCapabilities` from `openeo.rest.capabilities`.",
    category=UserDeprecationWarning,
    stacklevel=2,
)

__all__ = ["RESTCapabilities"]

# Legacy alias
RESTCapabilities = OpenEoCapabilities
