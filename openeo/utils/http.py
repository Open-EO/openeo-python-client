"""
openEO-oriented HTTP utilities
"""

from typing import Collection, Union

import requests
import requests.adapters

DEFAULT_RETRIES_TOTAL = 5

# On `backoff_factor`: it influences how much to sleep according to the formula:
#     sleep = {backoff factor} * (2 ** ({consecutive errors - 1}))
# The sleep before the first retry will be skipped however.
# For example with backoff_factor=2.5, the sleeps between consecutive attempts would be:
#     0, 5, 10, 20, 40, ...
DEFAULT_BACKOFF_FACTOR = 2.5


DEFAULT_RETRY_FORCELIST = frozenset(
    [
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    ]
)


def retry_adapter(
    *,
    total: int = DEFAULT_RETRIES_TOTAL,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Collection[int] = DEFAULT_RETRY_FORCELIST,
    **kwargs,
) -> requests.adapters.Retry:
    """
    Factory for creating a `requests.adapters.Retry` configuration object with
    openEO-oriented retry settings.

    :param total: Total number of retries to allow
    :param backoff_factor: scaling factor for sleeps between retries
    :param status_forcelist: A set of integer HTTP status codes that we should force a retry on.
    :param kwargs: additional kwargs to pass to `requests.adapters.Retry`
    :return:

    Inspiration and references:
    - https://requests.readthedocs.io/en/latest/api/#requests.adapters.HTTPAdapter
    - https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry
    - https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/#retry-on-failure
    """
    retry = requests.adapters.Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        **kwargs,
    )
    return retry


def _to_retry(
    retry: Union[requests.adapters.Retry, dict, None],
) -> requests.adapters.Retry:
    """
    Convert a retry specification to a `requests.adapters.Retry` object.
    """
    if isinstance(retry, requests.adapters.Retry):
        return retry
    elif isinstance(retry, dict):
        adapter = retry_adapter(**retry)
    elif retry in {None, True}:
        adapter = retry_adapter()
    else:
        raise ValueError(f"Invalid retry setting: {retry!r}")
    return adapter


def session_with_retries(
    retry: Union[requests.adapters.Retry, dict, None] = None,
) -> requests.Session:
    """
    Factory for a requests session with openEO-oriented retry settings.

    :param retry: The retry configuration, can be specified as:
        - :py:class:`requests.adapters.Retry`
        - a dictionary with :py:class:`requests.adapters.Retry` arguments,
          e.g. ``total``, ``backoff_factor``, ``status_forcelist``, ...
        - ``None`` for default openEO-oriented retry settings
    """
    session = requests.Session()
    retry = _to_retry(retry)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
