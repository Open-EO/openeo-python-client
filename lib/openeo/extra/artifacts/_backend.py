from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TypedDict

from openeo import Connection
from openeo.extra.artifacts.exceptions import (
    ArtifactsException,
    InvalidProviderConfig,
    NoAdvertisedProviders,
    NoDefaultConfig,
)

_capabilities_cache: Dict[str, Dict] = {}
_log = logging.getLogger(__name__)


class TProviderConfig(TypedDict):
    """The ID of the provider config not used in MVP"""

    id: str
    """The type of artifacts storage which defines how to interact with the storage"""
    type: str
    """The config for the artifacts storage this will depend on the type"""
    config: Dict[str, Any]


class ProviderConfig:
    """
    Configuration as provided by the OpenEO backend.
    It holds an exception if no such configuration is retrievable.
    """

    def __init__(self, id: str, type: str, config: dict, *, exc: Optional[Exception] = None):
        self.id = id
        self.type = type
        self.config: dict = config
        self.exc: Exception = exc

    @classmethod
    def from_typed_dict(cls, d: TProviderConfig) -> ProviderConfig:
        try:
            return cls(
                id=d["id"],
                type=d["type"],
                config=d["config"],
            )
        except KeyError as ke:
            raise InvalidProviderConfig("Provider config needs id, type and config fields.") from ke

    @classmethod
    def from_exception(cls, exc: Exception) -> ProviderConfig:
        return cls(id="undefined", type="undefined", config={}, exc=exc)

    def raise_if_needed(self, operation: str):
        """Check if operation can be done if not raise an exception"""
        if self.exc is not None:
            _log.warning(f"Trying to {operation} for backend config which was not available")
            raise self.exc

    def get_key(self, key: str) -> Any:
        try:
            self.raise_if_needed(f"get key {key}")
            return self.config[key]
        except ArtifactsException as ae:
            raise NoDefaultConfig(key) from ae
        except KeyError as ke:
            raise NoDefaultConfig(key) from ke

    def get_type(self) -> str:
        self.raise_if_needed("get type")
        return self.type

    def __getitem__(self, key):
        return self.get_key(key)


class ProvidersConfig(TypedDict):
    providers: List[TProviderConfig]


class TArtifactsCapabilty(TypedDict):
    artifacts: TypedDict


class ArtifactCapabilities:
    def __init__(self, connection: Connection):
        self._connection = connection

    def _get_artifacts_capabilities(self) -> ProvidersConfig:
        """
        Get the artifacts capabilities corresponding to the OpenEO connection
        """
        url = self._connection.root_url
        if url not in _capabilities_cache:
            try:
                _capabilities_cache[url] = self._connection.get("/").json()["artifacts"]
            except KeyError:
                raise NoAdvertisedProviders()
        return _capabilities_cache[url]

    def _get_artifacts_providers(self) -> List[TProviderConfig]:
        try:
            return self._get_artifacts_capabilities()["providers"]
        except KeyError as e:
            raise NoAdvertisedProviders() from e

    def get_preferred_artifacts_provider(self) -> ProviderConfig:
        try:
            return ProviderConfig.from_typed_dict(self._get_artifacts_providers()[0])
        except IndexError:
            return ProviderConfig("n/a", "n/a", {}, exc=NoAdvertisedProviders())
        except ArtifactsException as e:
            return ProviderConfig("n/a", "n/a", {}, exc=e)
