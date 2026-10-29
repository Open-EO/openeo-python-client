from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from openeo.extra.artifacts._backend import ProviderConfig
from openeo.extra.artifacts._config import ArtifactsStorageConfigABC
from openeo.extra.artifacts._uri import StorageURI
from openeo.rest.connection import Connection


class ArtifactHelperABC(ABC):
    """
    This class defines the *interface* that an artifact helper should implement and support. This is used by OpenEO users
    willing to manage artifacts.

    Instances that implement it get created by the `openeo.extra.artifacts.build_artifact_helper` factory
    """

    @classmethod
    def from_openeo_connection(
        cls,
        connection: Connection,
        provider_config: ProviderConfig,
        *,
        config: Optional[ArtifactsStorageConfigABC] = None,
    ) -> ArtifactHelperABC:
        """
        Create a new Artifact helper from the OpenEO connection. This is the starting point to upload artifacts.
        Each implementation has its own builder
        """
        if config is None:
            config = cls._get_default_storage_config()
        config.load_connection_provided_config(provider_config)
        return cls._from_openeo_connection(connection, config)

    @abstractmethod
    def upload_file(self, path: str | Path, object_name: str = "") -> StorageURI:
        """
        A method to store an artifact remotely and get a StorageURI which points to the stored data.

        :param path: Location of the file to be uploaded absolute path or relative to current
                     working directory.
        :param object_name: Optional name you want to give to the object. If not specified the filename will be
                            used.

        :return: If you want to use the StorageURI in a processgraph convert it using Python's built-in `str()`
                 function which is understood by the OpenEO processor.
        """

    @abstractmethod
    def get_presigned_url(self, storage_uri: StorageURI, expires_in_seconds: int = 7 * 3600 * 24) -> str:
        """
        A method to get a signed https URL for a given StorageURI which can be accessed via normal http libraries.

        These URIs should be kept secret as they provide access to the data.

        :param storage_uri: URI to the artifact that is stored by a previous `upload_file` call
        :param expires_in_seconds: Optional how long expressed in seconds before the returned signed URL becomes invalid

        :return: The signed https URI.

        """

    def __init__(self, config: ArtifactsStorageConfigABC):
        if not config.is_openeo_connection_metadata_loaded():
            raise RuntimeError("config should have openeo connection metadata loaded prior to initialization.")
        self._config = config

    @classmethod
    @abstractmethod
    def _get_default_storage_config(cls) -> ArtifactsStorageConfigABC:
        """
        A method that provides a default storage config for the Artifact Helper. It will return a class that
        extends `ArtifactsStorageConfigABC` and just provides default values which are defined in code no fancy
        resolvement from the backend yet. The config does not need to be usable by itself yet.

        If a config value can be advertised by the backend it should be initialized to a sentinel value and the actual
        value should be put in place if not advertised by the backend which happens in an implementation of
        :func:`~openeo.extra.artifacts._artifact_helper_abc.ArtifactsStorageConfigABC._load_connection_provided_config`
        """

    @classmethod
    @abstractmethod
    def _from_openeo_connection(cls, connection: Connection, config: ArtifactsStorageConfigABC) -> ArtifactHelperABC:
        """
        The implementation that creates an artifact helper. This method takes a config which has already been
        initialized from the metadata of the OpenEO connection.

        This method is internal as it is always called via `ArtifactHelperABC.from_openeo_connection`

        :param connection: A valid instance of a connection object to an OpenEOBackend
        :param config: object that specifies configuration for Artifact storage.
        """
