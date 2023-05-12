import functools
import inspect
import warnings
from typing import Callable, Optional
from deprecated.sphinx import deprecated as _deprecated


class UserDeprecationWarning(Warning):
    """
    Python has a built-in `DeprecationWarning` class to warn about deprecated features,
    but as the docs state (https://docs.python.org/3/library/warnings.html):

        when those warnings are intended for other Python developers

    Consequently, the default warning filters are set up to ignore (hide) these warnings
    to the software end user. The developer is expected to explicitly set up
    the warning filters to show the deprecation warnings again.

    In case of the openeo Python client however, this does not work because the client user
    is usually the developer, but probably won't bother setting up warning filters properly.

    This custom warning class can be used as drop in replacement for `DeprecationWarning`,
    where the deprecation warning should be visible by default.
    """
    pass


def test_warnings(stacklevel=1):
    """Trigger some warnings (for test contexts)."""
    for warning in [UserWarning, DeprecationWarning, UserDeprecationWarning]:
        warnings.warn(
            f"This is a {warning.__name__} (stacklevel {stacklevel})",
            category=warning, stacklevel=stacklevel
        )


def legacy_alias(orig: Callable, name: str, since: str):
    """
    Create legacy alias of given function/method/classmethod/staticmethod

    :param orig: function/method to create legacy alias for
    :param name: name of the alias
    :param since: version since when this is alias is deprecated
    :return:
    """
    post_process = None
    if isinstance(orig, classmethod):
        post_process = classmethod
        orig = orig.__func__
        kind = "class method"
    elif isinstance(orig, staticmethod):
        post_process = staticmethod
        orig = orig.__func__
        kind = "static method"
    elif inspect.ismethod(orig) or "self" in inspect.signature(orig).parameters:
        kind = "method"
    elif inspect.isfunction(orig):
        kind = "function"
    else:
        raise ValueError(orig)

    # Create a "copy" by wrapping the original
    @functools.wraps(orig)
    def wrapper(*args, **kwargs):
        return orig(*args, **kwargs)

    # Drop original doc block, just show deprecation note.
    wrapper.__doc__ = ""
    ref = f":py:{'meth' if 'method' in kind else 'func'}:`.{orig.__name__}`"
    wrapper = deprecated(
        reason=f"Use of this legacy {kind} is deprecated, use {ref} instead.",
        version=since,
    )(wrapper)

    if post_process:
        wrapper = post_process(wrapper)
    return wrapper


def deprecated(reason: str, version: str):
    """Wrapper around `deprecated.sphinx.deprecated` to explicitly set the warning category."""
    return _deprecated(reason=reason, version=version, category=UserDeprecationWarning)
