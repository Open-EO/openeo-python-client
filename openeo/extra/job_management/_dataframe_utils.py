import pandas as pd

class _ColumnProperties:
    def __init__(self, dtype: str, default=None):
        self.dtype = dtype
        self.default = default

# Expected columns in the job DB dataframes.
# TODO: make this part of public API when settled?
# TODO: move non official statuses to seperate column (not_started, queued_for_start)
COLUMN_REQUIREMENTS = {
    "id": _ColumnProperties(dtype="str"),
    "backend_name": _ColumnProperties(dtype="str"),
    "status": _ColumnProperties(dtype="str", default="not_started"),
    "start_time": _ColumnProperties(dtype="str"),
    "running_start_time": _ColumnProperties(dtype="str"),
    "cpu": _ColumnProperties(dtype="str"),
    "memory": _ColumnProperties(dtype="str"),
    "duration": _ColumnProperties(dtype="str"),
    "costs": _ColumnProperties(dtype="float64"),
}

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize given pandas dataframe (creating a new one):
    ensure we have the required columns.

    :param df: The dataframe to normalize.
    :return: a new dataframe that is normalized.
    """
    new_columns = {col: req.default for (col, req) in COLUMN_REQUIREMENTS.items() if col not in df.columns}
    df = df.assign(**new_columns)
    return df