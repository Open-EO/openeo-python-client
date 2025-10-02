import abc
import logging
from pathlib import Path
from typing import (
    Iterable,
    List,
    Union,
)

import pandas as pd
import shapely.errors
import shapely.wkt

_log = logging.getLogger(__name__)
from openeo.extra.job_management import MultiBackendJobManager

class JobDatabaseInterface(metaclass=abc.ABCMeta):
    """
    Interface for a database of job metadata to use with the :py:class:`MultiBackendJobManager`,
    allowing to regularly persist the job metadata while polling the job statuses
    and resume/restart the job tracking after it was interrupted.

    .. versionadded:: 0.31.0
    """

    @abc.abstractmethod
    def exists(self) -> bool:
        """Does the job database already exist, to read job data from?"""
        ...

    @abc.abstractmethod
    def persist(self, df: pd.DataFrame):
        """
        Store (now or updated) job data to the database.

        The provided dataframe may only cover a subset of all the jobs ("rows") of the whole database,
        so it should be merged with the existing data (if any) instead of overwriting it completely.

        :param df: job data to store.
        """
        ...

    @abc.abstractmethod
    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        """
        Retrieve the number of jobs per status.

        :param statuses: List/set of statuses to include. If empty, all statuses are included.

        :return: dictionary with status as key and the count as value.
        """
        ...

    @abc.abstractmethod
    def get_by_status(self, statuses: List[str], max=None) -> pd.DataFrame:
        """
        Returns a dataframe with jobs, filtered by status.

        :param statuses: List of statuses to include.
        :param max: Maximum number of jobs to return.

        :return: DataFrame with jobs filtered by status.
        """
        ...

    @abc.abstractmethod
    def get_by_indices(self, indices: Iterable[Union[int, str]]) -> pd.DataFrame:
        """
        Returns a dataframe with jobs based on their (dataframe) index

        :param indices: List of indices to include.

        :return: DataFrame with jobs filtered by indices.
        """
        ...

class FullDataFrameJobDatabase(JobDatabaseInterface):
    def __init__(self):
        super().__init__()
        self._df = None

    def initialize_from_df(self, df: pd.DataFrame, *, on_exists: str = "error"):
        """
        Initialize the job database from a given dataframe,
        which will be first normalized to be compatible
        with :py:class:`MultiBackendJobManager` usage.

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
        df = MultiBackendJobManager._normalize_df(df)
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

    :implements: :py:class:`JobDatabaseInterface`
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
            dtype={c: r.dtype for (c, r) in MultiBackendJobManager._COLUMN_REQUIREMENTS.items()},
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

    :implements: :py:class:`JobDatabaseInterface`
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


