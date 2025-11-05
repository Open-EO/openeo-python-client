import abc
import logging
from pathlib import Path
from typing import Iterable, Union

import pandas as pd
import shapely.errors
import shapely.wkt

import openeo.extra.job_management._manager
from openeo.extra.job_management._interface import JobDatabaseInterface

_log = logging.getLogger(__name__)


class FullDataFrameJobDatabase(JobDatabaseInterface):
    def __init__(self):
        super().__init__()
        self._df = None

    def initialize_from_df(self, df: pd.DataFrame, *, on_exists: str = "error"):
        """
        Initialize the job database from a given dataframe,
        which will be first normalized to be compatible
        with :py:class:`~openeo.extra.job_management._manager.MultiBackendJobManager` usage.

        :param df: dataframe with some columns your ``start_job`` callable expects
        :param on_exists: what to do when the job database already exists (persisted on disk):
            - "error": (default) raise an exception
            - "skip": work with existing database, ignore given dataframe and skip any initialization

        :return: initialized job database.

        .. versionadded:: 0.33.0
        """
        # TODO: option to provide custom MultiBackendJobManager subclass with custom normalize?
        if self.exists():
            if on_exists == "skip":
                return self
            elif on_exists == "error":
                raise FileExistsError(f"Job database {self!r} already exists.")
            else:
                # TODO handle other on_exists modes: e.g. overwrite, merge, ...
                raise ValueError(f"Invalid on_exists={on_exists!r}")
        df = openeo.extra.job_management._manager.MultiBackendJobManager._column_requirements.normalize_df(df)
        self.persist(df)
        # Return self to allow chaining with constructor.
        return self

    @abc.abstractmethod
    def read(self) -> pd.DataFrame:
        """
        Read job data from the database as pandas DataFrame.

        :return: loaded job data.
        """
        ...

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.read()
        return self._df

    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        status_histogram = self.df.groupby("status").size().to_dict()
        statuses = set(statuses)
        if statuses:
            status_histogram = {k: v for k, v in status_histogram.items() if k in statuses}
        return status_histogram

    def get_by_status(self, statuses, max=None) -> pd.DataFrame:
        """
        Returns a dataframe with jobs, filtered by status.

        :param statuses: List of statuses to include.
        :param max: Maximum number of jobs to return.

        :return: DataFrame with jobs filtered by status.
        """
        df = self.df
        filtered = df[df.status.isin(statuses)]
        return filtered.head(max) if max is not None else filtered

    def _merge_into_df(self, df: pd.DataFrame):
        if self._df is not None:
            unknown_indices = set(df.index).difference(df.index)
            if unknown_indices:
                _log.warning(f"Merging DataFrame with {unknown_indices=} which will be lost.")
            self._df.update(df, overwrite=True)
        else:
            self._df = df

    def get_by_indices(self, indices: Iterable[Union[int, str]]) -> pd.DataFrame:
        indices = set(indices)
        known = indices.intersection(self.df.index)
        unknown = indices.difference(self.df.index)
        if unknown:
            _log.warning(f"Ignoring unknown DataFrame indices {unknown}")
        return self._df.loc[list(known)]


class CsvJobDatabase(FullDataFrameJobDatabase):
    """
    Persist/load job metadata with a CSV file.

    :implements: :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface`
    :param path: Path to local CSV file.

    .. note::
        Support for GeoPandas dataframes depends on the ``geopandas`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

    .. versionadded:: 0.31.0
    """

    def __init__(self, path: Union[str, Path]):
        super().__init__()
        self.path = Path(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.exists()

    def _is_valid_wkt(self, wkt: str) -> bool:
        try:
            shapely.wkt.loads(wkt)
            return True
        except shapely.errors.WKTReadingError:
            return False

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.path,
            # TODO: possible to avoid hidden coupling with MultiBackendJobManager here?
            dtype=openeo.extra.job_management._manager.MultiBackendJobManager._column_requirements.dtype_mapping(),
        )
        if (
            "geometry" in df.columns
            and df["geometry"].dtype.name != "geometry"
            and self._is_valid_wkt(df["geometry"].iloc[0])
        ):
            import geopandas

            # `df.to_csv()` in `persist()` has encoded geometries as WKT, so we decode that here.
            df.geometry = geopandas.GeoSeries.from_wkt(df["geometry"])
            df = geopandas.GeoDataFrame(df)
        return df

    def persist(self, df: pd.DataFrame):
        self._merge_into_df(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(self.path, index=False)


class ParquetJobDatabase(FullDataFrameJobDatabase):
    """
    Persist/load job metadata with a Parquet file.

    :implements: :py:class:`~openeo.extra.job_management._interface.JobDatabaseInterface`
    :param path: Path to the Parquet file.

    .. note::
        Support for Parquet files depends on the ``pyarrow`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

        Support for GeoPandas dataframes depends on the ``geopandas`` package
        as :ref:`optional dependency <installation-optional-dependencies>`.

    .. versionadded:: 0.31.0
    """

    def __init__(self, path: Union[str, Path]):
        super().__init__()
        self.path = Path(path)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self.path)!r})"

    def exists(self) -> bool:
        return self.path.exists()

    def read(self) -> pd.DataFrame:
        # Unfortunately, a naive `pandas.read_parquet()` does not easily allow
        # reconstructing geometries from a GeoPandas Parquet file.
        # And vice-versa, `geopandas.read_parquet()` does not support reading
        # Parquet file without geometries.
        # So we have to guess which case we have.
        # TODO is there a cleaner way to do this?
        import pyarrow.parquet

        metadata = pyarrow.parquet.read_metadata(self.path)
        if b"geo" in metadata.metadata:
            import geopandas

            return geopandas.read_parquet(self.path)
        else:
            return pd.read_parquet(self.path)

    def persist(self, df: pd.DataFrame):
        self._merge_into_df(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_parquet(self.path, index=False)


def get_job_db(path: Union[str, Path]) -> JobDatabaseInterface:
    """
    Factory to get a job database at a given path,
    guessing the database type from filename extension.

    :param path: path to job database file.

    .. versionadded:: 0.33.0
    """
    path = Path(path)
    if path.suffix.lower() in {".csv"}:
        job_db = CsvJobDatabase(path=path)
    elif path.suffix.lower() in {".parquet", ".geoparquet"}:
        job_db = ParquetJobDatabase(path=path)
    else:
        raise ValueError(f"Could not guess job database type from {path!r}")
    return job_db


def create_job_db(path: Union[str, Path], df: pd.DataFrame, *, on_exists: str = "error"):
    """
    Factory to create a job database at given path,
    initialized from a given dataframe,
    and its database type guessed from filename extension.

    :param path: Path to the job database file.
    :param df: DataFrame to store in the job database.
    :param on_exists: What to do when the job database already exists:
        - "error": (default) raise an exception
        - "skip": work with existing database, ignore given dataframe and skip any initialization

    .. versionadded:: 0.33.0
    """
    job_db = get_job_db(path)
    if isinstance(job_db, FullDataFrameJobDatabase):
        job_db.initialize_from_df(df=df, on_exists=on_exists)
    else:
        raise NotImplementedError(f"Initialization of {type(job_db)} is not supported.")
    return job_db
