from __future__ import annotations

from abc import ABC, abstractmethod


class StorageURI(ABC):
    """A URI that is specific to a storage backend. The protocol determines what this URL looks like"""
    @classmethod
    @abstractmethod
    def from_str(cls, uri: str) -> StorageURI:
        """factory method to create a typed object from its string representation"""

    @abstractmethod
    def __str__(self):
        """The __str__ method is expected to be implemented"""
