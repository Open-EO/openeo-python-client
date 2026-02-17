"""
Backward compatibility wrapper for openeo.testing.backend

This module is deprecated. Import from openeo.testing instead.
"""

# Import from new location for backward compatibility
from openeo.testing.backend import (
    DummyBackend,
    OpeneoTestingException,
    build_capabilities,
)

# OPENEO_BACKEND constant (not part of DummyBackend, kept here for backward compatibility)
OPENEO_BACKEND = "https://openeo.test/"
