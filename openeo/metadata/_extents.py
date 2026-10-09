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
