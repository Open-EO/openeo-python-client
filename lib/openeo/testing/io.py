import json
import re
from pathlib import Path
from typing import Callable, Optional, Union

from openeo.util import repr_truncate


class PreprocessError(ValueError):
    pass

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

    def _get_preprocess(
        self,
        preprocess: Union[None, dict, Callable[[str], str]],
        verify_dict_keys: bool = True,
    ) -> Callable[[str], str]:
        """Normalize preprocess argument to a callable"""
        if preprocess is None:
            return lambda x: x
        elif isinstance(preprocess, dict):

            def replace(text: str) -> str:
                for needle, replacement in preprocess.items():
                    if isinstance(needle, re.Pattern):
                        if verify_dict_keys and not needle.search(text):
                            raise PreprocessError(f"{needle!r} not found in {repr_truncate(text, width=265)}")
                        text = needle.sub(repl=replacement, string=text)
                    elif isinstance(needle, str):
                        if verify_dict_keys and needle not in text:
                            raise PreprocessError(f"{needle!r} not found in {repr_truncate(text, width=256)}")
                        text = text.replace(needle, replacement)
                    else:
                        raise ValueError(needle)
                return text

            return replace
        else:
            return preprocess

    def load_text(
        self,
        filename: Union[str, Path],
        *,
        preprocess: Union[None, dict, Callable[[str], str]] = None,
        verify_dict_keys: bool = True,
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
        :param verify_dict_keys: when ``preprocess`` is specified as dict:
            whether to verify that the keys actually exist in the text before replacing them.
        :param encoding: Encoding to use when reading the file
        """
        text = self.get_path(filename).read_text(encoding=encoding)
        text = self._get_preprocess(preprocess, verify_dict_keys=verify_dict_keys)(text)
        return text

    def load_json(
        self,
        filename: Union[str, Path],
        *,
        preprocess: Union[None, dict, Callable[[str], str]] = None,
        verify_dict_keys: bool = True,
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
        :param verify_dict_keys: when ``preprocess`` is specified as dict:
            whether to verify that the keys actually exist in the text before replacing them.
        """
        raw = self.get_path(filename).read_text(encoding="utf8")
        raw = self._get_preprocess(preprocess, verify_dict_keys=verify_dict_keys)(raw)
        return json.loads(raw)
