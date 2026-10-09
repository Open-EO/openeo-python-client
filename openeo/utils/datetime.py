from __future__ import annotations

import datetime
from typing import Tuple, Union

import dateutil

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
