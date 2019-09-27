import base64
import json
import urllib.parse
from typing import Tuple, Callable

import pytest
import requests
import requests_mock

from openeo.rest.auth.oidc import OpenIdAuthenticator


@pytest.fixture()
def oidc_test_setup(requests_mock: requests_mock.Mocker):
    """
    Fixture that generates a function to set up an environment
    of mocked requests and request handlers to test OIDC flows
    """

    def setup(oidc_discovery_url: str, client_id: str = "myclient") -> Tuple[dict, Callable]:
        # Simple state dict for cross-checking some values between the fake OIDC handling entities.
        state = {}
        authorization_endpoint = "https://auth.example.com/auth"
        token_endpoint = "https://auth.example.com/token"

        def webbrowser_open(url):
            """Doing fake browser and Oauth Provider handling here"""
            assert url.startswith(authorization_endpoint)
            params = _get_query_params(url=url)
            assert params["client_id"] == client_id
            assert params["response_type"] == "code"
            for key in ["state", "nonce", "code_challenge", "redirect_uri"]:
                state[key] = params[key]
            redirect_uri = params["redirect_uri"]
            # Don't mock the request to the redirect URI (it is hosted by the temporary web server in separate thread)
            requests_mock.get(redirect_uri, real_http=True)
            requests.get(redirect_uri, params={"state": params["state"], "code": "6uthc0d3"})

        def token_callback(request, context):
            """Fake code to token exchange by Oauth Provider"""
            params = _get_query_params(query=request.text)
            assert params["client_id"] == client_id
            assert params["grant_type"] == "authorization_code"
            assert state["code_challenge"] == OpenIdAuthenticator.hash_code_verifier(params["code_verifier"])
            assert params["code"] == "6uthc0d3"
            assert params["redirect_uri"] == state["redirect_uri"]
            state["access_token"] = _jwt_encode({}, {"sub": "123", "name": "john", "nonce": state["nonce"]})
            return json.dumps({
                "access_token": state["access_token"],
                "id_token": _jwt_encode({}, {"sub": "123", "name": "john", "nonce": state["nonce"]}),
                "refresh_token": _jwt_encode({}, {"nonce": state["nonce"]}),
            })

        requests_mock.get(oidc_discovery_url, text=json.dumps({
            # Rudimentary OpenID Connect discovery document
            "authorization_endpoint": authorization_endpoint,
            "token_endpoint": token_endpoint,
        }))
        requests_mock.post(token_endpoint, text=token_callback)

        return state, webbrowser_open

    return setup


def _get_query_params(*, url=None, query=None):
    """Helper to extract query params from an url or query string"""
    if not query:
        query = urllib.parse.urlparse(url).query
    params = {}
    for param, values in urllib.parse.parse_qs(query).items():
        assert len(values) == 1
        params[param] = values[0]
    return params


def _jwt_encode(header: dict, payload: dict, signature="s1gn6tur3"):
    """Poor man's JWT encoding (just for unit testing purposes)"""

    def encode(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode("ascii")).decode("ascii").replace('=', '')

    return ".".join([encode(header), encode(payload), signature])
