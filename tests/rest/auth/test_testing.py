from openeo.rest.auth.oidc import (
    OidcClientCredentialsAuthenticator,
    OidcClientInfo,
    OidcProviderInfo,
)
from openeo.rest.auth.testing import OidcMock, build_basic_auth_header


class TestOidcMock:
    def test_request_history(self, requests_mock):
        oidc_issuer = "https://oidc.test"
        oidc_mock = OidcMock(
            requests_mock=requests_mock,
            oidc_issuer=oidc_issuer,
            expected_grant_type="client_credentials",
            expected_fields={"client_secret": "$ecr6t", "scope": "openid"},
        )
        assert [r.url for r in oidc_mock.get_request_history()] == []

        oidc_provider = OidcProviderInfo(issuer=oidc_issuer)
        assert [r.url for r in oidc_mock.get_request_history()] == [
            "https://oidc.test/.well-known/openid-configuration"
        ]

        client_info = OidcClientInfo(
            client_id="myclient",
            provider=oidc_provider,
            client_secret="$ecr6t",
        )
        authenticator = OidcClientCredentialsAuthenticator(client_info=client_info)
        authenticator.get_tokens()

        assert [r.url for r in oidc_mock.get_request_history("/token")] == [
            "https://oidc.test/token"
        ]


def test_build_basic_auth_header():
    assert build_basic_auth_header("john", "56(r61!?") == "Basic am9objo1NihyNjEhPw=="
