from __future__ import annotations

import typing


class _NonConcreteExtent:
    """
    Special marker class for non-concrete spatial, temporal, or other extents.
    There can be several reasons for an extent to be non-concrete at a given time:
    being unknown (in general, or specifically client-side),
    being undefined or unspecified by the user,
    being (unintendedly) unavailable/unresolvable,
    being dynamic or parameterized,
    being a placeholder for an unsupported or unimplemented feature,
    etc.

    The essential aspects this class signals:
    - it is known that there is an extent
    - but one or more actual bounds are non-concrete in some sense (unknown, dynamic, ...)

    Note that this is different from, so this class should not be used for:
    - the absence of an extent (e.g. no limits) or partial open bounds
    - an empty extent
    """

    __slots__ = ("reason",)

    def __init__(self, reason: str | None = None):
        self.reason = reason

    def __repr__(self) -> str:
        if self.reason:
            return f"_NonConcreteExtent(reason={self.reason!r})"
        return "_NonConcreteExtent()"

    def __eq__(self, value: object, /) -> bool:
        if isinstance(value, _NonConcreteExtent):
            # Non-concrete extents are not equal to each other
            # even not to themselves (because they are non-concrete).
            return False
        # Still let third-party equality tooling hook in (e.g. dirty_equals.IsInstance)
        return NotImplemented


class _EmptyExtent:
    """
    Special marker class for an empty extent (spatial, temporal, or other), like the empty set.
    The essential aspects this class signals: there is a known, concrete extent,
    but has no coverage: nothing matches or falls within it.

    Typical result of successive filter operations that have no overlap.

    Note that this is different from:
    - the absence of an extent (e.g. no limits) or partial open bounds
    - a non-concrete extent (which is known to have coverage, but the actual bounds are unknown, dynamic, etc.)
    """

    def __repr__(self) -> str:
        return "_EmptyExtent()"

    def __eq__(self, other) -> bool:
        if isinstance(other, _EmptyExtent):
            return True
        return NotImplemented


class NumericalInterval(typing.NamedTuple):
    """
    Simple numerical interval defined by a start and end point
    each of type float/int;
    or None to indicate being open-ended in that direction (no limit).
    """

    start: float | int | None
    end: float | int | None

    @classmethod
    def try_from(cls, x: typing.Any) -> NumericalInterval | None:
        if isinstance(x, (list, tuple)) and len(x) == 2 and all(isinstance(v, (int, float)) or v is None for v in x):
            return cls(*x)
        return None

    def intersection(self, other: NumericalInterval) -> NumericalInterval | _EmptyExtent:
        """
        Compute the intersection of this interval with another interval.
        If the intervals do not overlap, return an _EmptyExtent.
        """
        if self.start is None:
            start = other.start
        elif other.start is None:
            start = self.start
        else:
            start = max(self.start, other.start)

        if self.end is None:
            end = other.end
        elif other.end is None:
            end = self.end
        else:
            end = min(self.end, other.end)

        if start is not None and end is not None and end < start:
            return _EmptyExtent()

        return NumericalInterval(start, end)
