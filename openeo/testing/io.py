import json
import re
from pathlib import Path
from typing import Callable, Optional, Union


class TestDataLoader:
    """
    Test data helper: easily resolve paths to test data files,
    load them as bytes, text, JSON,
    optionally preprocess them, etc.

    It's intended to be used as a pytest fixture, e.g. from ``conftest.py``:

    .. code-block:: python

        @pytest.fixture
        def test_data() -> TestDataLoader:
            return TestDataLoader(root=Path(__file__).parent / "data")

    .. versionadded:: 0.30.0

    .. versionchanged:: 0.42.0
        Moved to ``openeo.testing.io``.
        Added ``load_bytes()`` and ``load_text()``.
        Improved ``preprocess``: can now be a replacement dict (with regex support).
    """

    def __init__(self, root: Union[str, Path]):
        self.data_root = Path(root)

    def get_path(self, filename: Union[str, Path]) -> Path:
        """Get absolute path to a test data file"""
        return self.data_root / filename

    def load_bytes(self, filename: Union[str, Path]) -> bytes:
        return self.get_path(filename).read_bytes()

    def _get_preprocess(self, preprocess: Union[None, dict, Callable[[str], str]]) -> Callable[[str], str]:
        """Normalize preprocess argument to a callable"""
        if preprocess is None:
            return lambda x: x
        elif isinstance(preprocess, dict):

            def replace(text: str) -> str:
                for key, value in preprocess.items():
                    if isinstance(key, re.Pattern):
                        text = key.sub(value, text)
                    elif isinstance(key, str):
                        text = text.replace(key, value)
                    else:
                        raise ValueError(key)
                return text

            return replace
        else:
            return preprocess

    def load_text(
        self,
        filename: Union[str, Path],
        *,
        preprocess: Union[None, dict, Callable[[str], str]] = None,
        encoding: str = "utf8",
    ) -> str:
        """
        Load text file, optionally with some text based preprocessing

        :param filename: Path to the file relative to the test data root
        :param preprocess: Optional preprocessing to do on the text, given as

            - Callable that takes a string and returns a string
            - Dictionary mapping needles to replacements.
              Needle can be a simple string that will be replaced with the replacement value,
              or it can be a ``re.Pattern`` that will be used in ``re.sub()`` style
              (which supports group references, e.g. "\1" for first group in match)
        :param encoding: Encoding to use when reading the file
        """
        text = self.get_path(filename).read_text(encoding=encoding)
        text = self._get_preprocess(preprocess)(text)
        return text

    def load_json(
        self,
        filename: Union[str, Path],
        *,
        preprocess: Union[None, dict, Callable[[str], str]] = None,
    ) -> dict:
        """
        Load data from a JSON file, optionally with some text based preprocessing

        :param filename: Path to the file relative to the test data root
        :param preprocess: Optional preprocessing to do on the text, given as

            - Callable that takes a string and returns a string
            - Dictionary mapping needles to replacements.
              Needle can be a simple string that will be replaced with the replacement value,
              or it can be a ``re.Pattern`` that will be used in ``re.sub()`` style
              (which supports group references, e.g. "\1" for first group in match)
        """
        raw = self.get_path(filename).read_text(encoding="utf8")
        raw = self._get_preprocess(preprocess)(raw)
        return json.loads(raw)
