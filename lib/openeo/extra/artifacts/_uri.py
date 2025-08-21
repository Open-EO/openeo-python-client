from __future__ import annotations

from abc import ABC, abstractmethod


class StorageURI(ABC):
    """A URI that is specific to a storage backend. The protocol determines what this URL looks like"""

    @classmethod
    @abstractmethod
    def from_str(cls, uri: str) -> StorageURI:
        """factory method to create a typed object from its string representation"""

    def __str__(self):
        return self.to_string()

    @abstractmethod
    def to_string(self) -> str:
        raise NotImplementedError("Implementation must implement explicit handling.")
