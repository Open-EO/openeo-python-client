"""
Generic data structure related utilities
"""

import itertools
from typing import Container


def make_new_key(container: Container[str], key: str, *, start: int = 1):
    """
    Construct a new key that doesn't exist yet in the given container (dict, set, ...),
    based on given base key (to use directly if possible, or by appending an auto-increment-style suffix).
    """
    if key not in container:
        return key
    for i in itertools.count(start):
        if (k := f"{key}-{i}") not in container:
            return k
