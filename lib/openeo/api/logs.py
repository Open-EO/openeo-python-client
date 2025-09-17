import warnings

from openeo.internal.warnings import UserDeprecationWarning
from openeo.rest.models.logs import LogEntry, log_level_name, normalize_log_level

warnings.warn(
    message="Submodule `openeo.api.logs` is deprecated in favor of `openeo.rest.models.logs`.",
    category=UserDeprecationWarning,
    stacklevel=2,
)


__all__ = ["LogEntry", "normalize_log_level", "log_level_name"]
