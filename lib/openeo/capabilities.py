import warnings

from openeo.internal.warnings import UserDeprecationWarning
from openeo.utils.version import ApiVersionException, ComparableVersion

warnings.warn(
    message="Submodule `openeo.capabilities` is deprecated. Find `ComparableVersion` and related at `openeo.utils.version`.",
    category=UserDeprecationWarning,
    stacklevel=2,
)

__all__ = ["ComparableVersion", "ApiVersionException"]
