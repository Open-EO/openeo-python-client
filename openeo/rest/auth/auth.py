from requests import Request
from requests.auth import AuthBase


class OpenEoApiAuthBase(AuthBase):
    """
    Base class for authentication with the OpenEO REST API.

    Follows the authentication approach of the requests library:
    an auth object is a callable object that can be passed with get/post request
    to manipulate this request (typically setting headers).
    """

    def __call__(self, req: Request) -> Request:
        # Do nothing by default
        return req


class NullAuth(OpenEoApiAuthBase):
    """No authentication"""
    pass


class BearerAuth(OpenEoApiAuthBase):
    """
    Requests are authenticated through a bearer token
    https://open-eo.github.io/openeo-api/apireference/#section/Authentication/Bearer
    """

    def __init__(self, bearer: str):
        self.bearer = bearer

    def __call__(self, req: Request) -> Request:
        # Add bearer authorization header.
        req.headers['Authorization'] = "Bearer {b}".format(b=self.bearer)
        return req
