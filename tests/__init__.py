import json
import os
from pathlib import Path
from typing import Callable


def get_test_resource(relative_path: str) -> Path:
    dir = Path(os.path.dirname(os.path.realpath(__file__)))
    return dir / relative_path


def load_json_resource(relative_path, preprocess: Callable = None):
    with get_test_resource(relative_path).open('r+') as f:
        data = f.read()
        if preprocess:
            data = preprocess(data)
        return json.loads(data)
