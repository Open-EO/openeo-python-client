from abc import ABC, abstractmethod

from .imagecollection import ImageCollection

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""


class Session(ABC):

    @property
    @abstractmethod
    def auth(self) -> str:
        pass

    @abstractmethod
    def imagecollection(self, image_collection_id:str) -> ImageCollection:
        """
        Retrieves an Image Collection object based on the id of a given layer.

        :param image_collection_id: The id of the image collection to retrieve.
        :return An image collection corresponding to the given id.
        """
        pass
