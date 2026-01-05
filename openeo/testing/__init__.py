"""
Utilities for testing of openEO client workflows.
"""

# Backend testing utilities
from openeo.testing.backend import (
    OPENEO_BACKEND,
    DummyBackend,
    OpeneoTestingException,
    build_capabilities,
)

# Legacy import for backwards compatibility
from openeo.testing.io import TestDataLoader
