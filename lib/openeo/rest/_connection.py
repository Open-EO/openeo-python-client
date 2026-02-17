from __future__ import annotations

import logging
import sys
from typing import Iterable, Optional, Union

import requests
import urllib3.util
from requests import Response
from requests.auth import AuthBase

import openeo
from openeo.rest import OpenEoApiError, OpenEoApiPlainError, OpenEoRestError
from openeo.rest.auth.auth import NullAuth
from openeo.util import ContextTimer, ensure_list, str_truncate, url_join
from openeo.utils.http import HTTP_502_BAD_GATEWAY, session_with_retries

_log = logging.getLogger(__name__)

# Default timeouts for requests
# TODO: get default_timeout from config?
DEFAULT_TIMEOUT = 20 * 60


class RestApiConnection:
    """Base connection class implementing generic REST API request functionality"""

    def __init__(
        self,
        root_url: str,
        *,
        auth: Optional[AuthBase] = None,
        session: Optional[requests.Session] = None,
        default_timeout: Optional[int] = None,
        slow_response_threshold: Optional[float] = None,
        retry: Union[urllib3.util.Retry, dict, bool, None] = None,
    ):
        self._root_url = root_url
        self._auth = None
        self.auth = auth or NullAuth()
        if session:
            self.session = session
        elif retry is not False:
            self.session = session_with_retries(retry=retry)
        else:
            self.session = requests.Session()
        self.default_timeout = default_timeout or DEFAULT_TIMEOUT
        self.default_headers = {
            "User-Agent": "openeo-python-client/{cv} {py}/{pv} {pl}".format(
                cv=openeo.client_version(),
                py=sys.implementation.name,
                pv=".".join(map(str, sys.version_info[:3])),
                pl=sys.platform,
            )
        }
        self.slow_response_threshold = slow_response_threshold

    @property
    def root_url(self):
        return self._root_url

    @property
    def auth(self) -> Union[AuthBase, None]:
        return self._auth

    @auth.setter
    def auth(self, auth: Union[AuthBase, None]):
        self._auth = auth
        self._on_auth_update()

    def _on_auth_update(self):
        pass

    def build_url(self, path: str):
        return url_join(self._root_url, path)

    def _merged_headers(self, headers: dict) -> dict:
        """Merge default headers with given headers"""
        result = self.default_headers.copy()
        if headers:
            result.update(headers)
        return result

    def _is_external(self, url: str) -> bool:
        """Check if given url is external (not under root url)"""
        root = self.root_url.rstrip("/")
        return not (url == root or url.startswith(root + "/"))

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        auth: Optional[AuthBase] = None,
        check_error: bool = True,
        expected_status: Optional[Union[int, Iterable[int]]] = None,
        **kwargs,
    ):
        """Generic request send"""
        url = self.build_url(path)
        # Don't send default auth headers to external domains.
        auth = auth or (self.auth if not self._is_external(url) else None)
        slow_response_threshold = kwargs.pop("slow_response_threshold", self.slow_response_threshold)
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug(
                "Request `{m} {u}` with params {p}, headers {h}, auth {a}, kwargs {k}".format(
                    m=method.upper(),
                    u=url,
                    p=params,
                    h=headers and headers.keys(),
                    a=type(auth).__name__,
                    k=list(kwargs.keys()),
                )
            )
        with ContextTimer() as timer:
            resp = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=self._merged_headers(headers),
                auth=auth,
                timeout=kwargs.pop("timeout", self.default_timeout),
                **kwargs,
            )
        if slow_response_threshold and timer.elapsed() > slow_response_threshold:
            _log.warning(
                "Slow response: `{m} {u}` took {e:.2f}s (>{t:.2f}s)".format(
                    m=method.upper(), u=str_truncate(url, width=64), e=timer.elapsed(), t=slow_response_threshold
                )
            )
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug(
                f"openEO request `{resp.request.method} {resp.request.path_url}` -> response {resp.status_code} headers {resp.headers!r}"
            )
        # Check for API errors and unexpected HTTP status codes as desired.
        status = resp.status_code
        expected_status = ensure_list(expected_status) if expected_status else []
        if check_error and status >= 400 and status not in expected_status:
            self._raise_api_error(resp)
        if expected_status and status not in expected_status:
            raise OpenEoRestError(
                "Got status code {s!r} for `{m} {p}` (expected {e!r}) with body {body}".format(
                    m=method.upper(), p=path, s=status, e=expected_status, body=resp.text
                )
            )
        return resp

    def _raise_api_error(self, response: requests.Response):
        """Convert API error response to Python exception"""
        status_code = response.status_code
        try:
            info = response.json()
        except Exception:
            info = None

        # Valid JSON object with "code" and "message" fields indicates a proper openEO API error.
        if isinstance(info, dict):
            error_code = info.get("code")
            error_message = info.get("message")
            if error_code and isinstance(error_code, str) and error_message and isinstance(error_message, str):
                raise OpenEoApiError(
                    http_status_code=status_code,
                    code=error_code,
                    message=error_message,
                    id=info.get("id"),
                    url=info.get("url"),
                )

        # Failed to parse it as a compliant openEO API error: show body as-is in the exception.
        text = response.text
        error_message = None
        _log.warning(f"Failed to parse API error response: [{status_code}] {text!r} (headers: {response.headers})")

        # TODO: eliminate this VITO-backend specific error massaging?
        if status_code == HTTP_502_BAD_GATEWAY and "Proxy Error" in text:
            error_message = (
                "Received 502 Proxy Error."
                " This typically happens when a synchronous openEO processing request takes too long and is aborted."
                " Consider using a batch job instead."
            )

        raise OpenEoApiPlainError(message=text, http_status_code=status_code, error_message=error_message)

    def get(
        self,
        path: str,
        *,
        params: Optional[dict] = None,
        stream: bool = False,
        auth: Optional[AuthBase] = None,
        **kwargs,
    ) -> Response:
        """
        Do GET request to REST API.

        :param path: API path (without root url)
        :param params: Additional query parameters
        :param stream: True if the get request should be streamed, else False
        :param auth: optional custom authentication to use instead of the default one
        :return: response: Response
        """
        return self.request("get", path=path, params=params, stream=stream, auth=auth, **kwargs)

    def head(
        self,
        path: str,
        *,
        params: Optional[dict] = None,
        auth: Optional[AuthBase] = None,
        **kwargs,
    ) -> Response:
        """
        Do HEAD request to REST API.

        :param path: API path (without root url)
        :param params: Additional query parameters
        :param auth: optional custom authentication to use instead of the default one
        :return: response: Response
        """
        return self.request("head", path=path, params=params, auth=auth, **kwargs)

    def post(self, path: str, json: Optional[dict] = None, **kwargs) -> Response:
        """
        Do POST request to REST API.

        :param path: API path (without root url)
        :param json: Data (as dictionary) to be posted with JSON encoding)
        :return: response: Response
        """
        return self.request("post", path=path, json=json, allow_redirects=False, **kwargs)

    def delete(self, path: str, **kwargs) -> Response:
        """
        Do DELETE request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("delete", path=path, allow_redirects=False, **kwargs)

    def patch(self, path: str, **kwargs) -> Response:
        """
        Do PATCH request to REST API.

        :param path: API path (without root url)
        :return: response: Response
        """
        return self.request("patch", path=path, allow_redirects=False, **kwargs)

    def put(self, path: str, headers: Optional[dict] = None, data: Optional[dict] = None, **kwargs) -> Response:
        """
        Do PUT request to REST API.

        :param path: API path (without root url)
        :param headers: headers that gets added to the request.
        :param data: data that gets added to the request.
        :return: response: Response
        """
        return self.request("put", path=path, data=data, headers=headers, allow_redirects=False, **kwargs)

    def __repr__(self):
        return "<{c} to {r!r} with {a}>".format(c=type(self).__name__, r=self._root_url, a=type(self.auth).__name__)
