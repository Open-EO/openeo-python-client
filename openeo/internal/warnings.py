import functools
import inspect
import warnings
from typing import Callable
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


def legacy_alias(orig: Callable, name: str):
    """
    Create legacy alias of given function/method/classmethod/staticmethod

    :param orig: function/method to create legacy alias for
    :param name: name of the alias
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

    msg = "Call to deprecated {k} `{n}`, use `{o}` instead.".format(k=kind, n=name, o=orig.__name__)

    @functools.wraps(orig)
    def wrapper(*args, **kwargs):
        warnings.warn(msg, category=UserDeprecationWarning, stacklevel=2)
        return orig(*args, **kwargs)

    # TODO: make this more Sphinx aware
    wrapper.__doc__ = "Use of this legacy {k} is deprecated, use :py:{r}:`.{o}` instead.".format(
        k=kind, r="meth" if "method" in kind else "func", o=orig.__name__
    )

    if post_process:
        wrapper = post_process(wrapper)
    return wrapper


def deprecated(reason: str, version: str):
    """Wrapper around `deprecated.sphinx.deprecated` to explicitly set the warning category."""
    return _deprecated(reason=reason, version=version, category=UserDeprecationWarning)
