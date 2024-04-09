"""
Helpers, mocks for testing (OIDC) authentication
"""

import base64
import contextlib
import json
import urllib.parse
import uuid
from typing import List, Optional, Union
from unittest import mock

import requests
import requests_mock.request

from openeo.rest.auth.oidc import PkceCode, random_string
from openeo.util import dict_no_none, url_join

DEVICE_CODE_POLL_INTERVAL = 2


# Sentinel object to indicate that a field should be absent.
ABSENT = object()


class OidcMock:
    """
    Fixture/mock to act as stand-in OIDC provider to test OIDC flows
    """

    def __init__(
        self,
        requests_mock: requests_mock.Mocker,
        *,
        expected_grant_type: Optional[str] = None,
        oidc_issuer: str = "https://oidc.test",
        expected_client_id: str = "myclient",
        expected_fields: dict = None,
        state: dict = None,
        scopes_supported: List[str] = None,
        device_code_flow_support: bool = True,
        oidc_discovery_url: Optional[str] = None,
        support_verification_uri_complete: bool = False,
    ):
        self.requests_mock = requests_mock
        self.oidc_issuer = oidc_issuer
        self.expected_grant_type = expected_grant_type
        self.grant_request_history = []
        self.expected_client_id = expected_client_id
        self.expected_fields = expected_fields or {}
        self.expected_authorization_code = None
        self.authorization_endpoint = url_join(self.oidc_issuer, "/auth")
        self.token_endpoint = url_join(self.oidc_issuer, "/token")
        self.device_code_endpoint = url_join(self.oidc_issuer, "/device_code") if device_code_flow_support else None
        self.state = state or {}
        self.scopes_supported = scopes_supported or ["openid", "email", "profile"]
        self.support_verification_uri_complete = support_verification_uri_complete
        self.mocks = {}

        oidc_discovery_url = oidc_discovery_url or url_join(oidc_issuer, "/.well-known/openid-configuration")
        self.mocks["oidc_discovery"] = self.requests_mock.get(
            oidc_discovery_url,
            text=json.dumps(
                dict_no_none(
                    {
                        # Rudimentary OpenID Connect discovery document
                        "issuer": self.oidc_issuer,
                        "authorization_endpoint": self.authorization_endpoint,
                        "token_endpoint": self.token_endpoint,
                        "device_authorization_endpoint": self.device_code_endpoint,
                        "scopes_supported": self.scopes_supported,
                    }
                )
            ),
        )
        self.mocks["token_endpoint"] = self.requests_mock.post(self.token_endpoint, text=self.token_callback)

        if self.device_code_endpoint:
            self.mocks["device_code_endpoint"] = self.requests_mock.post(
                self.device_code_endpoint, text=self.device_code_callback
            )

    def webbrowser_open(self, url: str):
        """Doing fake browser and Oauth Provider handling here"""
        assert url.startswith(self.authorization_endpoint)
        params = self._get_query_params(url=url)
        assert params["client_id"] == self.expected_client_id
        assert params["response_type"] == "code"
        assert params["scope"] == self.expected_fields["scope"]
        for key in ["state", "nonce", "code_challenge", "redirect_uri", "scope"]:
            self.state[key] = params[key]
        redirect_uri = params["redirect_uri"]
        # Don't mock the request to the redirect URI (it is hosted by the temporary web server in separate thread)
        self.requests_mock.get(redirect_uri, real_http=True)
        self.expected_authorization_code = "6uthc0d3"
        requests.get(
            redirect_uri,
            params={"state": params["state"], "code": self.expected_authorization_code},
        )

    def token_callback(self, request: requests_mock.request._RequestObjectProxy, context):
        params = self._get_query_params(query=request.text)
        grant_type = params["grant_type"]
        self.grant_request_history.append({"grant_type": grant_type})
        if self.expected_grant_type:
            assert grant_type == self.expected_grant_type
        callback = {
            "authorization_code": self.token_callback_authorization_code,
            "client_credentials": self.token_callback_client_credentials,
            "password": self.token_callback_resource_owner_password_credentials,
            "urn:ietf:params:oauth:grant-type:device_code": self.token_callback_device_code,
            "refresh_token": self.token_callback_refresh_token,
        }[grant_type]
        result = callback(params=params, context=context)
        try:
            result_decoded = json.loads(result)
            self.grant_request_history[-1]["response"] = result_decoded
        except json.JSONDecodeError:
            self.grant_request_history[-1]["response"] = result
        return result

    def token_callback_authorization_code(self, params: dict, context):
        """Fake code to token exchange by Oauth Provider"""
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "authorization_code"
        assert self.state["code_challenge"] == PkceCode.sha256_hash(params["code_verifier"])
        assert params["code"] == self.expected_authorization_code
        assert params["redirect_uri"] == self.state["redirect_uri"]
        return self._build_token_response()

    def token_callback_client_credentials(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "client_credentials"
        assert params["scope"] == self.expected_fields["scope"]
        assert params["client_secret"] == self.expected_fields["client_secret"]
        return self._build_token_response(include_id_token=False, include_refresh_token=False)

    def token_callback_resource_owner_password_credentials(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "password"
        assert params["client_secret"] == self.expected_fields["client_secret"]
        assert params["username"] == self.expected_fields["username"]
        assert params["password"] == self.expected_fields["password"]
        assert params["scope"] == self.expected_fields["scope"]
        return self._build_token_response()

    def device_code_callback(self, request: requests_mock.request._RequestObjectProxy, context):
        params = self._get_query_params(query=request.text)
        assert params["client_id"] == self.expected_client_id
        assert params["scope"] == self.expected_fields["scope"]
        self.state["device_code"] = random_string()
        self.state["user_code"] = random_string(length=6).upper()
        self.state["scope"] = params["scope"]
        if "code_challenge" in self.expected_fields:
            expect_code_challenge = self.expected_fields.get("code_challenge")
            if expect_code_challenge in [True]:
                assert "code_challenge" in params
                self.state["code_challenge"] = params["code_challenge"]
            elif expect_code_challenge in [False, ABSENT]:
                assert "code_challenge" not in params
            else:
                raise ValueError(expect_code_challenge)

        response = {
            # TODO: also verification_url (google tweak)
            "verification_uri": url_join(self.oidc_issuer, "/dc"),
            "device_code": self.state["device_code"],
            "user_code": self.state["user_code"],
            "interval": DEVICE_CODE_POLL_INTERVAL,
        }
        if self.support_verification_uri_complete:
            response["verification_uri_complete"] = (
                response["verification_uri"] + f"?user_code={self.state['user_code']}"
            )
        return json.dumps(response)

    def token_callback_device_code(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        expected_client_secret = self.expected_fields.get("client_secret")
        if expected_client_secret:
            assert params["client_secret"] == expected_client_secret
        else:
            assert "client_secret" not in params
        expect_code_verifier = self.expected_fields.get("code_verifier")
        if expect_code_verifier in [True]:
            assert PkceCode.sha256_hash(params["code_verifier"]) == self.state["code_challenge"]
            self.state["code_verifier"] = params["code_verifier"]
        elif expect_code_verifier in [False, None, ABSENT]:
            assert "code_verifier" not in params
            assert "code_challenge" not in self.state
        else:
            raise ValueError(expect_code_verifier)
        assert params["device_code"] == self.state["device_code"]
        assert params["grant_type"] == "urn:ietf:params:oauth:grant-type:device_code"
        # Fail with pending/too fast?
        try:
            result = self.state["device_code_callback_timeline"].pop(0)
        except Exception:
            result = "rest in peace"
        if result == "great success":
            return self._build_token_response()
        else:
            context.status_code = 400
            return json.dumps({"error": result})

    def token_callback_refresh_token(self, params: dict, context):
        assert params["client_id"] == self.expected_client_id
        assert params["grant_type"] == "refresh_token"
        if "client_secret" in self.expected_fields:
            assert params["client_secret"] == self.expected_fields["client_secret"]
        if params["refresh_token"] != self.expected_fields["refresh_token"]:
            context.status_code = 401
            return json.dumps({"error": "invalid refresh token"})
        assert params["refresh_token"] == self.expected_fields["refresh_token"]
        return self._build_token_response(include_id_token=False, include_refresh_token=False)

    @staticmethod
    def _get_query_params(*, url=None, query=None):
        """Helper to extract query params from an url or query string"""
        if not query:
            query = urllib.parse.urlparse(url).query
        params = {}
        for param, values in urllib.parse.parse_qs(query).items():
            assert len(values) == 1
            params[param] = values[0]
        return params

    @staticmethod
    def _jwt_encode(header: dict, payload: dict, signature="s1gn6tur3"):
        """Poor man's JWT encoding (just for unit testing purposes)"""

        def encode(d):
            return base64.urlsafe_b64encode(json.dumps(d).encode("ascii")).decode("ascii").replace("=", "")

        return ".".join([encode(header), encode(payload), signature])

    def _build_token_response(
        self,
        sub="123",
        name="john",
        include_id_token=True,
        include_refresh_token: Optional[bool] = None,
    ) -> str:
        """Build JSON serialized access/id/refresh token response (and store tokens for use in assertions)"""
        access_token = self._jwt_encode(
            header={},
            payload=dict_no_none(
                sub=sub,
                name=name,
                nonce=self.state.get("nonce"),
                _uuid=uuid.uuid4().hex,
            ),
        )
        res = {"access_token": access_token}

        # Attempt to simulate real world refresh token support.
        if include_refresh_token is None:
            if "offline_access" in self.scopes_supported:
                # "offline_access" scope as suggested in spec
                # (https://openid.net/specs/openid-connect-core-1_0.html#OfflineAccess)
                # Implemented by Microsoft, EGI Check-in
                include_refresh_token = "offline_access" in self.state.get("scope", "").split(" ")
            else:
                # Google OAuth style: no support for "offline_access", return refresh token automatically?
                include_refresh_token = True
        if include_refresh_token:
            res["refresh_token"] = self._jwt_encode(header={}, payload={"foo": "refresh", "_uuid": uuid.uuid4().hex})
        if include_id_token:
            res["id_token"] = access_token
        self.state.update(res)
        self.state.update(name=name, sub=sub)
        return json.dumps(res)

    def validate_access_token(self, access_token: str):
        if access_token == self.state["access_token"]:
            return {"user_id": self.state["name"], "sub": self.state["sub"]}
        raise LookupError("Invalid access token")

    def invalidate_access_token(self):
        self.state["access_token"] = "***invalidated***"

    def get_request_history(
        self, url: Optional[str] = None, method: Optional[str] = None
    ) -> List[requests_mock.request._RequestObjectProxy]:
        """Get mocked request history: requests with given method/url."""
        if url and url.startswith("/"):
            url = url_join(self.oidc_issuer, url)
        return [
            r
            for r in self.requests_mock.request_history
            if (method is None or method.lower() == r.method.lower()) and (url is None or url == r.url)
        ]
