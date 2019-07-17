"""
Various utilities and helpers.
"""


def first_not_none(*args):
    """Return first item from given arguments that is not None."""
    for item in args:
        if item is not None:
            return item
    raise ValueError("No not-None values given.")
