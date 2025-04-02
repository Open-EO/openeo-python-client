from __future__ import annotations

from abc import ABC, abstractmethod

from openeo import Connection

_METADATA_LOADED = "_sc_metadata_loaded"


class StorageConfig(ABC):
    """
    Storage config allows overriding configuration for the interaction with the backend storage.
    It greatly depends on the type of storage so the enforced API is limited to load metadata using the connection.
    """
    @abstractmethod
    def _load_openeo_connection_metadata(self, conn: Connection) -> None:
        """
        Implementations implement their logic of adapting config based on metadata from the current OpenEO connection
        in this method.
        """

    def load_openeo_connection_metadata(self, conn: Connection) -> None:
        """
        This is the method that is actually used to load metadata. Metadata is only loaded once.
        """
        if not self.is_openeo_connection_metadata_loaded():
            self._load_openeo_connection_metadata(conn)
            object.__setattr__(self, _METADATA_LOADED, True)

    def is_openeo_connection_metadata_loaded(self) -> bool:
        """
        This is a helper to check whether metadata is loaded.
        """
        if not hasattr(self, _METADATA_LOADED):
            return False
        return getattr(self, _METADATA_LOADED)
