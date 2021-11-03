"""
Calculate spectral indices (vegetation indices, water indices, etc.) in one line

The indices implemented have been derived from the eemont package created by davemlz:
https://github.com/davemlz/eemont/blob/master/eemont/data/spectral-indices-dict.json
and further supplemented with indices that were necessary for use cases
"""

class BaseOpenEoException(Exception):
    pass

from openeo._version import __version__

def client_version() -> str:
    return __version__
