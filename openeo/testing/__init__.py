"""
Utilities for testing of openEO client workflows.
"""

import json
from pathlib import Path
from typing import Callable, Optional, Union


class TestDataLoader:
    """
    Helper to resolve paths to test data files, load them as JSON, optionally preprocess them, etc.

    It's intended to be used as a pytest fixture, e.g. from ``conftest.py``:

    .. code-block:: python

        @pytest.fixture
        def test_data() -> TestDataLoader:
            return TestDataLoader(root=Path(__file__).parent / "data")

    .. versionadded:: 0.30.0
    """

    def __init__(self, root: Union[str, Path]):
        self.data_root = Path(root)

    def get_path(self, filename: Union[str, Path]) -> Path:
        """Get absolute path to a test data file"""
        return self.data_root / filename

    def load_json(self, filename: Union[str, Path], preprocess: Optional[Callable[[str], str]] = None) -> dict:
        """Parse data from a test JSON file"""
        data = self.get_path(filename).read_text(encoding="utf8")
        if preprocess:
            data = preprocess(data)
        return json.loads(data)
