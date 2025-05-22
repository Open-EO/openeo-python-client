from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict

from openeo import Connection
from openeo.extra.artifacts.exceptions import (
    ArtifactsException,
    InvalidProviderCfg,
    NoAdvertisedProviders,
    NoDefaultConfig,
)

_capabilities_cache: Dict[str, Dict] = {}


class TProviderCfg(TypedDict):
    """The ID of the provider config not used in MVP"""

    id: str
    """The type of artifacts storage which defines how to interact with the storage"""
    type: str
    """The config for the artifacts storage this will depend on the type"""
    cfg: Dict[str, Any]


class ProviderCfg:
    """
    Configuration as provided by the OpenEO backend.
    It also holds exceptions if no such configuration is retrievable.
    """

    def __init__(self, id: str, type: str, cfg: dict, *, exc: Optional[Exception] = None):
        self.id = id
        self.type = type
        self.cfg: dict = cfg
        self.exc: Exception = exc

    @classmethod
    def from_typed_dict(cls, d: TProviderCfg) -> ProviderCfg:
        try:
            return cls(
                id=d["id"],
                type=d["type"],
                cfg=d["cfg"],
            )
        except KeyError as ke:
            raise InvalidProviderCfg("Provider config needs id, type and cfg fields.") from ke

    @classmethod
    def from_exception(cls, exc: Exception) -> ProviderCfg:
        return cls(id="undefined", type="undefined", cfg={}, exc=exc)

    def raise_if_needed(self, operation: str):
        """Check if operation can be done if not raise an exception"""
        if self.exc is not None:
            raise RuntimeError(f"Trying to {operation} for provider config which was not available") from self.exc

    def get_key(self, key: str) -> Any:
        self.raise_if_needed(f"get key {key}")
        try:
            return self.cfg[key]
        except KeyError as ke:
            raise NoDefaultConfig(key) from ke

    def get_type(self) -> str:
        self.raise_if_needed("get type")
        return self.type

    def __getitem__(self, key):
        return self.get_key(key)



class ProvidersCfg(TypedDict):
    providers: List[TProviderCfg]


class TArtifactsCapabilty(TypedDict):
    artifacts: TypedDict


class ArtifactCapabilities:
    def __init__(self, conn: Connection):
        self.conn = conn

    def _get_artifacts_capabilities(self) -> ProvidersCfg:
        """
        Get the artifacts capabilities corresponding to the OpenEO connection
        """
        url = self.conn.root_url
        if url not in _capabilities_cache:
            try:
                _capabilities_cache[url] = self.conn.get("/").json()["artifacts"]
            except KeyError:
                raise NoAdvertisedProviders()
        return _capabilities_cache[url]

    def _get_artifacts_providers(self) -> List[TProviderCfg]:
        try:
            return self._get_artifacts_capabilities()["providers"]
        except KeyError as e:
            raise NoAdvertisedProviders() from e

    def get_preferred_artifacts_provider(self) -> ProviderCfg:
        try:
            return ProviderCfg.from_typed_dict(self._get_artifacts_providers()[0])
        except (IndexError, ArtifactsException) as e:
            return ProviderCfg("id", "type", {}, exc=e)
