from __future__ import annotations

import dataclasses
import datetime
from typing import Any, Tuple, Union

import dateutil

from openeo.util import rfc3339

# Some type aliases for convenience
DateLike = Union[str, datetime.date]
DateTimeLike = Union[str, datetime.datetime, datetime.date]


def to_datetime_utc(d: DateTimeLike) -> datetime.datetime:
    """Parse/convert to datetime in UTC."""
    if isinstance(d, str):
        d = dateutil.parser.parse(d)
    elif isinstance(d, datetime.datetime):
        pass
    elif isinstance(d, datetime.date):
        d = datetime.datetime.combine(d, datetime.time.min)
    else:
        raise ValueError(f"Expected str/datetime, but got {type(d)}")
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    else:
        d = d.astimezone(datetime.timezone.utc)
    return d


def to_datetime_naive(d: DateTimeLike) -> datetime.datetime:
    """Convert to datetime, assuming UTC where necessary, but return as naive."""
    return to_datetime_utc(d).replace(tzinfo=None)


def to_datetime_utc_unless_none(d: DateTimeLike | None) -> datetime.datetime | None:
    """Parse/convert to datetime in UTC, but preserve None."""
    return None if d is None else to_datetime_utc(d)


@dataclasses.dataclass(frozen=True)
class DateTimeInterval:
    """
    Datetime interval, defined by a start and end datetime instant (UTC)
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
    def try_from(cls, x: Any) -> DateTimeInterval | None:
        """Try to build a TemporalExtent from the given object, or return None if not possible."""
        try:
            if isinstance(x, (list, tuple)) and len(x) == 2:
                return DateTimeInterval(*x)
            elif isinstance(x, dict):
                return DateTimeInterval(**x)
        except ValueError:
            pass
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

    def intersection(self, other: DateTimeInterval) -> DateTimeInterval | None:
        """
        Compute the intersection of this interval with another interval.
        If the intervals do not overlap, return None.
        :param other:
        :return:
        """
        if not isinstance(other, DateTimeInterval):
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
            return None

        return DateTimeInterval(start, end)
