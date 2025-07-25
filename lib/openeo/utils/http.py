"""
openEO-oriented HTTP utilities
"""

from typing import Collection, Union

import requests
import requests.adapters
from urllib3.util import Retry

# Commonly used subset of HTTP response status codes
HTTP_100_CONTINUE = 100
HTTP_200_OK = 200
HTTP_201_CREATED = 201
HTTP_202_ACCEPTED = 202
HTTP_204_NO_CONTENT = 204
HTTP_301_MOVED_PERMANENTLY = 301
HTTP_302_FOUND = 302
HTTP_400_BAD_REQUEST = 400
HTTP_401_UNAUTHORIZED = 401
HTTP_402_PAYMENT_REQUIRED = 402
HTTP_403_FORBIDDEN = 403
HTTP_404_NOT_FOUND = 404
HTTP_408_REQUEST_TIMEOUT = 408
HTTP_429_TOO_MANY_REQUESTS = 429
HTTP_500_INTERNAL_SERVER_ERROR = 500
HTTP_501_NOT_IMPLEMENTED = 501
HTTP_502_BAD_GATEWAY = 502
HTTP_503_SERVICE_UNAVAILABLE = 503
HTTP_504_GATEWAY_TIMEOUT = 504


DEFAULT_RETRIES_TOTAL = 3

# On `backoff_factor`: it influences how much to sleep according to the formula:
#     sleep = {backoff factor} * (2 ** ({consecutive errors - 1}))
# The sleep before the first retry will be skipped however.
# For example with backoff_factor=2.5, the sleeps between consecutive attempts would be:
#     0, 5, 10, 20, 40, ...
DEFAULT_BACKOFF_FACTOR = 2.5


DEFAULT_RETRY_FORCELIST = frozenset(
    [
        HTTP_429_TOO_MANY_REQUESTS,
    ]
)


def retry_configuration(
    *,
    total: int = DEFAULT_RETRIES_TOTAL,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Collection[int] = DEFAULT_RETRY_FORCELIST,
    **kwargs,
) -> Retry:
    """
    Factory for creating a :py:class:`urllib3.util.retry.Retry` configuration object with
    openEO-oriented retry settings.

    :param total: Total number of retries to allow
    :param backoff_factor: scaling factor for sleeps between retries
    :param status_forcelist: A set of integer HTTP status codes that we should force a retry on.
    :param kwargs: additional kwargs to pass to :py:class:`urllib3.util.retry.Retry`
    :return:

    Inspiration and references:
    - https://requests.readthedocs.io/en/latest/api/#requests.adapters.HTTPAdapter
    - https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry
    - https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/#retry-on-failure
    """
    retry = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        **kwargs,
    )
    return retry


def _to_retry(retry: Union[Retry, dict, None]) -> Retry:
    """
    Convert a retry specification to a :py:class:`urllib3.util.retry.Retry` object.
    """
    if isinstance(retry, Retry):
        pass
    elif isinstance(retry, dict):
        retry = retry_configuration(**retry)
    elif retry in {None, True}:
        retry = retry_configuration()
    else:
        raise ValueError(f"Invalid retry setting: {retry!r}")
    return retry


def session_with_retries(retry: Union[Retry, dict, None] = None) -> requests.Session:
    """
    Factory for a requests session with openEO-oriented retry settings.

    :param retry: The retry configuration, can be specified as:
        - :py:class:`urllib3.util.retry.Retry`
        - a dictionary with :py:class:`urllib3.util.retry.Retry` arguments,
          e.g. ``total``, ``backoff_factor``, ``status_forcelist``, ...
        - ``None`` for default openEO-oriented retry settings
    """
    session = requests.Session()
    retry = _to_retry(retry)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
