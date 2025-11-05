import dataclasses
from typing import Any, Mapping

import pandas as pd


@dataclasses.dataclass(frozen=True)
class _ColumnProperties:
    """Expected/required properties of a column in the job manager related dataframes"""

    dtype: str = "object"
    default: Any = None


# Expected columns in the job DB dataframes.
# TODO: make this part of public API when settled?
# TODO: move non official statuses to seperate column (not_started, queued_for_start)
_COLUMN_REQUIREMENTS: Mapping[str, _ColumnProperties] = {
    "id": _ColumnProperties(dtype="str"),
    "backend_name": _ColumnProperties(dtype="str"),
    "status": _ColumnProperties(dtype="str", default="not_started"),
    # TODO: use proper date/time dtype instead of legacy str for start times?
    "start_time": _ColumnProperties(dtype="str"),
    "running_start_time": _ColumnProperties(dtype="str"),
    # TODO: these columns "cpu", "memory", "duration" are not referenced explicitly from MultiBackendJobManager,
    #       but are indirectly coupled through handling of VITO-specific "usage" metadata in `_track_statuses`.
    #       Since bfd99e34 they are not really required to be present anymore, can we make that more explicit?
    "cpu": _ColumnProperties(dtype="str"),
    "memory": _ColumnProperties(dtype="str"),
    "duration": _ColumnProperties(dtype="str"),
    "costs": _ColumnProperties(dtype="float64"),
}


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize given pandas dataframe (creating a new one):
    ensure we have the required columns.

    :param df: The dataframe to normalize.
    :return: a new dataframe that is normalized.
    """
    new_columns = {col: req.default for (col, req) in _COLUMN_REQUIREMENTS.items() if col not in df.columns}
    df = df.assign(**new_columns)

    return df
