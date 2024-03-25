"""
Compatibility layer and small backports.
"""

import contextlib

try:
    from contextlib import nullcontext
except ImportError:
    # nullcontext for pre-3.7 python
    @contextlib.contextmanager
    def nullcontext(enter_result=None):
        yield enter_result
