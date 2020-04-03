import json
import os
from pathlib import Path


def get_test_resource(relative_path):
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return str(dir / relative_path)


def load_json_resource(relative_path):
    with open(get_test_resource(relative_path), 'r+') as f:
        return json.load(f)


def as_path(path) -> Path:
    """Workaround for Python 3.5 where pytest `tmp_path` fixture objects are not compatible with `pathlib.Path()`"""
    return Path(str(path))
