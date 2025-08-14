from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from openeo.extra.artifacts._backend import ProviderConfig

_METADATA_LOADED = "_sc_metadata_loaded"


class ArtifactsStorageConfigABC(ABC):
    """
    Storage config allows overriding configuration for the interaction with the backend storage.
    It greatly depends on the type of storage so the enforced API is limited to load metadata using the connection.
    """

    @abstractmethod
    def _load_connection_provided_config(self, provider_config: ProviderConfig) -> None:
        """
        Implementations implement their logic of adapting config based on metadata from the current OpenEO connection.

        The config depends on the storage type so here it can deal with settings specific for this type of storage.

        :param provider_config: The provider config that is available for this type of storage config.
        """

    @staticmethod
    def get_type_from(cls: Any) -> str:
        """The type is the name of the implementing class so that is the first object used for method resolution"""
        return cls.__mro__[0].__name__

    @classmethod
    def get_type(cls) -> str:
        """Return the storage config type"""
        return cls.get_type_from(cls)

    def load_connection_provided_config(self, provider_config: ProviderConfig) -> None:
        """
        This is the method that is actually used to load metadata. Metadata is only loaded once.
        """
        if not self.is_openeo_connection_metadata_loaded():
            self._load_connection_provided_config(provider_config)
            object.__setattr__(self, _METADATA_LOADED, True)

    def is_openeo_connection_metadata_loaded(self) -> bool:
        """
        This is a helper to check whether metadata is loaded.
        """
        if not hasattr(self, _METADATA_LOADED):
            return False
        return getattr(self, _METADATA_LOADED)
