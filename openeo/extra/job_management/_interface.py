import abc
import math
from typing import Any, Dict, Iterable, List, Union

import pandas as pd


class JobDatabaseInterface(metaclass=abc.ABCMeta):
    """
    Interface for a database of job metadata to use with the
    :py:class:`~openeo.extra.job_management._manager.MultiBackendJobManager`,
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

    def update(self, updates: Dict[Union[int, str], Dict[str, Any]]):
        """
        Update some fields of existing jobs.
        New columns are added, unknown indices and ``None``/NaN values are ignored.

        Default implementation: read the affected rows (matched on dataframe index),
        set the new values and persist them.
        Implementations can override this with a native partial update.

        :param updates: mapping of (dataframe) index to a dictionary of column name to new value.
        """
        if not updates:
            return
        df = self.get_by_indices(indices=updates.keys()).copy()
        for i, values in updates.items():
            if i not in df.index:
                continue
            for column, value in values.items():
                if value is None or (isinstance(value, float) and math.isnan(value)):
                    continue
                if column not in df.columns:
                    df[column] = None
                elif isinstance(value, str) and df[column].dtype != object:
                    # E.g. empty legacy columns loaded as float64
                    df[column] = df[column].astype(object)
                df.at[i, column] = value
        self.persist(df)
