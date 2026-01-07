import abc
from typing import Iterable, List, Union

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
