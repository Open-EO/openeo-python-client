"""
Utilities to build/automate/extend documentation
"""

import collections
import inspect
import textwrap
from functools import partial
from typing import Callable, Optional, Tuple, TypeVar

# TODO: give this a proper public API?
_process_registry = collections.defaultdict(list)


T = TypeVar("T", bound=Callable)


def openeo_process(f: Optional[T] = None, process_id: Optional[str] = None, mode: Optional[str] = None) -> T:
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
    seealso = f'.. seealso::\n    openeo.org documentation on `process "{process_id}" <{url}>`_.'
    f.__doc__ = textwrap.dedent(f.__doc__ or "") + "\n\n" + seealso

    _process_registry[process_id].append((f, mode))
    return f


def openeo_endpoint(endpoint: str) -> Callable[[Callable], Callable]:
    """
    Parameterized decorator to annotate given function or method with the openEO endpoint it interacts with

    :param endpoint: REST endpoint (e.g. "GET /jobs", "POST /result", ...)
    :return:
    """
    # TODO: automatically parse/normalize endpoint (to method+path)
    # TODO: wrap this in some markup/directive to make this more a "small print" note.

    def decorate(f: Callable) -> Callable:
        is_method = list(inspect.signature(f).parameters.keys())[:1] == ["self"]
        seealso = f"This {'method' if is_method else 'function'} uses openEO endpoint ``{endpoint}``"
        f.__doc__ = textwrap.dedent(f.__doc__ or "") + "\n\n" + seealso + "\n"
        return f

    return decorate
