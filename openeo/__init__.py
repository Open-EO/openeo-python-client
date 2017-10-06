"""
The OpenEO client API.
"""

__title__ = 'openeo'
__version__ = '0.0.1'
__author__ = 'Jeroen Dries'

from .api import apply_to_products,apply_to_pixels, zonal_statistics
from .catalog import EOProduct