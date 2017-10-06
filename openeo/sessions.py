from abc import ABC, abstractmethod

"""
openeo.sessions
~~~~~~~~~~~~~~~~
This module provides a Session object to manage and persist settings when interacting with the OpenEO API.
"""

class Session():

    @property
    @abstractmethod
    def auth(self) -> str:
        pass

def session(username=None,endpoint:str="https://openeo.org/endpoint"):
    """
    Returns a :class:`Session` for context-management.

    :rtype: Session
    """

    return Session()