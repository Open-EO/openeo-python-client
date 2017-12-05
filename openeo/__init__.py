"""
The OpenEO client API.
"""

__title__ = 'openeo'
__version__ = '0.0.1'
__author__ = 'Jeroen Dries'

from .catalog import EOProduct
from .imagecollection import ImageCollection
from .rest.rest_session import session
