"""
Backward compatibility wrapper for openeo.testing.backend

This module is deprecated. Import from openeo.testing instead.
"""

# Import from new location for backward compatibility
from openeo.testing.backend import (
    OPENEO_BACKEND,
    DummyBackend,
    OpeneoTestingException,
    build_capabilities,
)
