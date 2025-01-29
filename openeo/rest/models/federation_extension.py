import logging
from typing import List, Union

_log = logging.getLogger(__name__)

class FederationExtension:
    """
    Wrapper the openEO Federation extension as defined by
    https://github.com/Open-EO/openeo-api/tree/draft/extensions/federation
    """

    __slots__ = ["_data"]

    def __init__(self, data: dict):
        self._data = data

    @property
    def missing(self) -> Union[List[str], None]:
        """
        Get the ``federation:missing`` property (if any) of the resource,
        which lists back-ends that were not available during the request.

        :return: list of back-end IDs that were not available.
            Or None, when ``federation:missing`` is not present in response.
        """
        return self._data.get("federation:missing", None)

    def warn_on_missing(self, resource_name: str) -> None:
        """
        Warn about presence of ``federation:missing`` in the resource.
        """
        missing = self.missing
        if missing:
            _log.warning(f"Partial {resource_name}: missing federation components: {missing!r}.")
