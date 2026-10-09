from __future__ import annotations

import dataclasses
import datetime
import logging
from typing import Any, Tuple, Union

from util import rfc3339
from utils.datetime import DateTimeLike, to_datetime_utc_unless_none

logger = logging.getLogger(__name__)


class _NonConcreteExtent:
    """
    Special marker class for non-concrete spatial, temporal, or other extents.
    There can be several reasons for an extent to be non-concrete at a given time:
    being generally unknown, being undefined or unspecified by the user,
    being (unintendedly) unavailable/unresolvable, being dynamic or parameterized, etc.

    The essential aspects this class signals:
    - it is known that there is an extent
    - but one or more actual bounds are non-concrete in some sense (unknown, dynamic, ...)

    Note that this is different from, so this class should not be used for:
    - the absence of an extent (e.g. no limits) or partial open bounds
    - an empty extent
    """

    pass


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

    pass


@dataclasses.dataclass(frozen=True)
class TemporalExtent:
    """
    Simple temporal extent: a pair of two (UTC) datetime instants, start and end,
    each of which can be ``None`` to indicate being open-ended in that direction (no limit).

    Note on including or excluding the end instant:
    this is not a property of the extent itself, but is an aspect of what the extent describes or how it is used:
    - when the extent describes the range of a certain data set, the end instant is inclusive
    - when the extent is used as search window for a query, the end instant is typically handled as exclusive
    """

    start: Union[datetime.datetime, None]
    end: Union[datetime.datetime, None]

    def __init__(self, start: DateTimeLike | None, end: DateTimeLike | None):
        object.__setattr__(self, "start", to_datetime_utc_unless_none(start))
        object.__setattr__(self, "end", to_datetime_utc_unless_none(end))

    @classmethod
    def try_from(cls, x: Any) -> TemporalExtent | None:
        """Try to build a TemporalExtent from the given object, or return None if not possible."""
        if isinstance(x, (list, tuple)) and len(x) == 2:
            try:
                return TemporalExtent(*x)
            except ValueError:
                return None
        return None

    def as_tuple(self) -> Tuple[datetime.datetime | None, datetime.datetime | None]:
        return self.start, self.end

    def as_isoformat(self, **kwargs) -> Tuple[str | None, str | None]:
        return (
            self.start.isoformat(**kwargs) if self.start else None,
            self.end.isoformat(**kwargs) if self.end else None,
        )

    def as_rfc3339(self) -> Tuple[str | None, str | None]:
        return (
            rfc3339.datetime(self.start),
            rfc3339.datetime(self.end),
        )

    def intersection(self, other: TemporalExtent) -> TemporalExtent | _EmptyExtent:
        if not isinstance(other, TemporalExtent):
            raise ValueError(f"Expected TemporalExtent, but got: {other}")

        # Start with naive start and end calculation
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

        # Double check naive calculation on emptiness
        if start is not None and end is not None and end <= start:
            return _EmptyExtent()

        return TemporalExtent(start, end)
