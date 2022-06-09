"""
Utilities to build/automate/extend documentation
"""
import collections
import textwrap
from functools import partial
from typing import Callable, Optional

# TODO: give this a proper public API?
_process_registry = collections.defaultdict(list)


def openeo_process(f: Callable = None, process_id: Optional[str] = None, mode: Optional[str] = None):
    """
    Decorator for function or method to associate it with a standard openEO process

    :param f: function or method
    :param process_id: openEO process_id (to be given when it can not be guessed from function name)
    :return:
    """
    # TODO: include openEO version?
    # TODO: support non-standard/proposed/experimental?
    # TODO: handling of `mode` (or something alike): apply/reduce_dimension/... callback, (band) math operator, ...?
    # TODO: documentation test that "seealso" urls are valid
    # TODO: inject more references/metadata in __doc__
    if f is None:
        # Parameterized decorator call
        return partial(openeo_process, process_id=process_id)

    process_id = process_id or f.__name__
    url = f"https://processes.openeo.org/#{process_id}"
    seealso = f'.. seealso:: openeo.org documentation on `process "{process_id}" <{url}>`_.'
    f.__doc__ = textwrap.dedent(f.__doc__ or "") + "\n\n" + seealso

    _process_registry[process_id].append((f, mode))
    return f
