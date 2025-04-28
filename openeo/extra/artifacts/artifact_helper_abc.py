from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from openeo.extra.artifacts.backend import ProviderCfg
from openeo.extra.artifacts.config import StorageConfig
from openeo.extra.artifacts.uri import StorageURI
from openeo.rest.connection import Connection


class ArtifactHelperBuilderABC(ABC):
    @classmethod
    @abstractmethod
    def from_openeo_connection(cls, conn: Connection, config: Optional[StorageConfig] = None) -> ArtifactHelperABC:
        """
        Builder pattern, only used for implementing support for other Artifact stores.
        """
        raise NotImplementedError("ArtifactHelperBuilders must have their own implementation")


class ArtifactHelperABC(ABC):
    @classmethod
    def from_openeo_connection(
        cls, conn: Connection, provider_cfg: ProviderCfg, *, config: Optional[StorageConfig] = None
    ) -> ArtifactHelperABC:
        """
        Create a new Artifact helper from the OpenEO connection. This is the starting point to upload artifacts.
        Each implementation has its own builder
        """
        if config is None:
            config = cls._get_default_storage_config()
        config.load_connection_provided_cfg(provider_cfg)
        return cls._from_openeo_connection(conn, config)

    @abstractmethod
    def upload_file(self, path: str | Path, object_name: str = "") -> StorageURI:
        """
        A method to store an artifact remotely and get a URI understandable by the OpenEO processor
        """

    @abstractmethod
    def get_presigned_url(self, storage_uri: StorageURI, expires_in_seconds: int) -> str:
        """
        A method to convert a StorageURI to a signed https URL which can be accessed via normal http libraries.

        These URIs should be kept secret as they provide access to the data.
        """

    def __init__(self, config: StorageConfig):
        if not config.is_openeo_connection_metadata_loaded():
            raise RuntimeError("config should have openeo connection metadata loaded prior to initialization.")
        self._config = config

    @classmethod
    @abstractmethod
    def _get_default_storage_config(cls) -> StorageConfig:
        """
        A method that provides a default storage config for the Artifact Helper
        """

    @classmethod
    @abstractmethod
    def _from_openeo_connection(cls, conn: Connection, config: StorageConfig) -> ArtifactHelperABC:
        """
        The implementation that creates an artifact helper. This method takes a config which has already been
        initialized from the metadata of the OpenEO connection.
        """
