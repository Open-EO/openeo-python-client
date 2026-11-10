import contextlib
import logging
import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Union

import numpy as np
import pandas as pd

import openeo.extra.job_management._manager
from openeo.extra.job_management._job_db import FullDataFrameJobDatabase

_log = logging.getLogger(__name__)


def _quote_identifier(name) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _to_sql_value(value):
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or (isinstance(value, float) and math.isnan(value)):
        return None
    if not isinstance(value, (int, float, str, bytes, type(None))):
        # Like CSV: store unsupported types (timestamps, lists, ...) as their string representation.
        return str(value)
    return value


class SqliteJobDatabase(FullDataFrameJobDatabase):
    """
    Persist/load job metadata with a SQLite database file.

    Unlike the CSV and Parquet based job databases,
    updates are written per row in a transaction (no full file rewrite)
    and filtering by status happens in SQL.

    :implements: :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface`
    :param path: Path to the SQLite database file.

    .. note::
        Support for GeoPandas dataframes depends on the ``geopandas`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.
    """

    _TABLE = "jobs"
    _INDEX = "_job_db_index"
    # Declared column type to mark geometry columns (stored as WKT).
    _GEOMETRY_TYPE = "GEOMETRY"

    def __init__(self, path: Union[str, Path]):
        super().__init__()
        self.path = Path(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.exists()

    @contextlib.contextmanager
    def _connect(self, write: bool = False):
        # Manual transaction control (`isolation_level=None`): sqlite3's implicit transactions
        # would not cover schema changes (CREATE/ALTER TABLE).
        with contextlib.closing(sqlite3.connect(self.path, isolation_level=None, timeout=30)) as connection:
            # Connection as context manager: commit on success, rollback on error.
            with connection:
                if write:
                    # WAL: readers don't block on the writer. Persistent in the db file, no-op once set.
                    connection.execute("PRAGMA journal_mode=WAL")
                    # Take the write lock before inspecting the schema, so concurrent writers can't race.
                    connection.execute("BEGIN IMMEDIATE")
                yield connection

    def _query(self, where: str = "", params=(), limit=None) -> pd.DataFrame:
        with self._connect() as connection:
            geometry_columns = [
                row[1]
                for row in connection.execute(f"PRAGMA table_info({self._TABLE})")
                if row[2].upper() == self._GEOMETRY_TYPE
            ]
            df = pd.read_sql_query(
                f"SELECT * FROM {self._TABLE} {where} ORDER BY rowid LIMIT ?",
                connection,
                params=[*params, -1 if limit is None else limit],
                index_col=self._INDEX,
            )
        df.index.name = None
        # String columns already come back as str/None, only coerce other (e.g. float) dtypes.
        dtypes = openeo.extra.job_management._manager.MultiBackendJobManager._column_requirements.dtype_mapping()
        df = df.astype({c: t for c, t in dtypes.items() if c in df.columns and t != "str"})
        if geometry_columns:
            import geopandas

            for column in geometry_columns:
                df[column] = geopandas.GeoSeries.from_wkt(df[column])
            df = geopandas.GeoDataFrame(df, geometry=geometry_columns[0])
        return df

    def read(self) -> pd.DataFrame:
        return self._query()

    def persist(self, df: pd.DataFrame):
        if self._INDEX in df.columns:
            raise ValueError(f"Column name {self._INDEX!r} is reserved by {self.__class__.__name__}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        geometry_columns = [c for c in df.columns if df[c].dtype.name == "geometry"]
        df = pd.DataFrame(df).assign(**{c: df[c].to_wkt() for c in geometry_columns})
        # Convert values before touching the database, so conversion errors leave no half-created database.
        rows = [
            [_to_sql_value(i)] + [_to_sql_value(v) for v in row]
            for i, row in zip(df.index, df.itertuples(index=False, name=None))
        ]
        index = _quote_identifier(self._INDEX)
        with self._connect(write=True) as connection:
            connection.execute(f"CREATE TABLE IF NOT EXISTS {self._TABLE} ({index} PRIMARY KEY)")
            existing = {row[1] for row in connection.execute(f"PRAGMA table_info({self._TABLE})")}
            for column in df.columns:
                if column not in existing:
                    column_type = self._GEOMETRY_TYPE if column in geometry_columns else ""
                    connection.execute(
                        f"ALTER TABLE {self._TABLE} ADD COLUMN {_quote_identifier(column)} {column_type}"
                    )
            columns = [index] + [_quote_identifier(c) for c in df.columns]
            updates = ", ".join(f"{c} = excluded.{c}" for c in columns[1:])
            sql = (
                f"INSERT INTO {self._TABLE} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})"
                f" ON CONFLICT({index}) DO " + (f"UPDATE SET {updates}" if updates else "NOTHING")
            )
            connection.executemany(sql, rows)
        # Invalidate cached dataframe (from `df` property)
        self._df = None

    def update(self, updates: Dict[Union[int, str], Dict[str, Any]]):
        if not updates or not self.exists():
            return
        with self._connect(write=True) as connection:
            existing = {row[1] for row in connection.execute(f"PRAGMA table_info({self._TABLE})")}
            for i, values in updates.items():
                values = {c: _to_sql_value(v) for c, v in values.items() if c != self._INDEX}
                values = {c: v for c, v in values.items() if v is not None}
                for column in values.keys() - existing:
                    connection.execute(f"ALTER TABLE {self._TABLE} ADD COLUMN {_quote_identifier(column)}")
                    existing.add(column)
                if values:
                    assignments = ", ".join(f"{_quote_identifier(c)} = ?" for c in values)
                    connection.execute(
                        f"UPDATE {self._TABLE} SET {assignments} WHERE {_quote_identifier(self._INDEX)} = ?",
                        [*values.values(), _to_sql_value(i)],
                    )
        self._df = None

    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        statuses = list(statuses)
        where = f"WHERE status IN ({', '.join('?' * len(statuses))})" if statuses else ""
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT status, COUNT(*) FROM {self._TABLE} {where} GROUP BY status", statuses
            ).fetchall()
        return dict(rows)

    def get_by_status(self, statuses, max=None) -> pd.DataFrame:
        statuses = list(statuses)
        return self._query(where=f"WHERE status IN ({', '.join('?' * len(statuses))})", params=statuses, limit=max)

    def get_by_indices(self, indices: Iterable[Union[int, str]]) -> pd.DataFrame:
        indices = [_to_sql_value(i) for i in set(indices)]
        df = self._query(
            where=f"WHERE {_quote_identifier(self._INDEX)} IN ({', '.join('?' * len(indices))})", params=indices
        )
        unknown = set(indices).difference(df.index)
        if unknown:
            _log.warning(f"Ignoring unknown DataFrame indices {unknown}")
        return df
