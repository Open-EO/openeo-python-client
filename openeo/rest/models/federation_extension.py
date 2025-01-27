from typing import List, Union


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
