import logging
from unittest import mock

import pytest

from openeo.rest.auth import cli
from openeo.rest.auth.cli import CliToolException
from openeo.rest.auth.config import AuthConfig, RefreshTokenStore
from .test_oidc import OidcMock, assert_device_code_poll_sleep


def mock_input(*args: str):
    """Mock user input (one or more responses)"""
    return mock.patch("builtins.input", side_effect=list(args))


def mock_secret_input(secret: str):
    """Mocking of user input of password/secret through `getpass`"""
    return mock.patch.object(cli, "getpass", side_effect=[secret])


@pytest.fixture(autouse=True)
def auth_config(tmp_openeo_config_home) -> AuthConfig:
    """Make sure we start with emtpy AuthConfig."""
    config = AuthConfig(tmp_openeo_config_home)
    assert not config.path.exists()
    return config


@pytest.fixture(autouse=True)
def refresh_token_store(tmp_openeo_config_home) -> RefreshTokenStore:
    store = RefreshTokenStore(tmp_openeo_config_home)
    assert not store.path.exists()
    return store


def test_paths(capsys):
    cli.main(["paths"])
    out = capsys.readouterr().out
    assert "/auth-config.json" in out
    assert "/refresh-tokens.json" in out


def test_config_dump(capsys, auth_config):
    auth_config.set_basic_auth("https://oeo.test", "john17", "j0hn123")
    cli.main(["config-dump"])
    out = capsys.readouterr().out
    assert "john17" in out
    assert "j0hn123" not in out
    assert "<redacted>" in out


def test_config_dump_show_secrets(capsys, auth_config):
    auth_config.set_basic_auth("https://oeo.test", "john17", "j0hn123")
    cli.main(["config-dump", "--show-secrets"])
    out = capsys.readouterr().out
    assert "john17" in out
    assert "j0hn123" in out
    assert "<redacted>" not in out


def test_token_clear_no_file(capsys, refresh_token_store):
    assert not refresh_token_store.path.exists()
    cli.main(["token-clear"])
    out = capsys.readouterr().out
    assert "No refresh token file at" in out


def test_token_clear_no(capsys, refresh_token_store):
    refresh_token_store.set_refresh_token(issuer="i", client_id="c", refresh_token="r")
    assert refresh_token_store.path.exists()
    with mock_input("no"):
        cli.main(["token-clear"])
    out = capsys.readouterr().out
    assert "Keeping refresh token file" in out
    assert refresh_token_store.path.exists()


def test_token_clear_yes(capsys, refresh_token_store):
    refresh_token_store.set_refresh_token(issuer="i", client_id="c", refresh_token="r")
    assert refresh_token_store.path.exists()
    with mock_input("yes"):
        cli.main(["token-clear"])
    out = capsys.readouterr().out
    assert "Removed refresh token file" in out
    assert not refresh_token_store.path.exists()


def test_token_clear_force(capsys, refresh_token_store):
    refresh_token_store.set_refresh_token(issuer="i", client_id="c", refresh_token="r")
    assert refresh_token_store.path.exists()
    cli.main(["token-clear", "--force"])
    out = capsys.readouterr().out
    assert "Removed refresh token file" in out
    assert not refresh_token_store.path.exists()


def test_add_basic_auth(auth_config):
    with mock_secret_input("p455w0r6"):
        cli.main(["add-basic", "https://oeo.test", "--username", "user49", "--no-try"])
    assert auth_config.get_basic_auth("https://oeo.test") == ("user49", "p455w0r6")


def test_add_basic_auth_input_username(auth_config):
    with mock_input("user55") as input_mock, mock_secret_input("p455w0r6"):
        cli.main(["add-basic", "https://oeo.test", "--no-try"])
    assert input_mock.call_count == 1
    assert "Enter username" in input_mock.call_args[0][0]
    assert auth_config.get_basic_auth("https://oeo.test") == ("user55", "p455w0r6")


def test_add_oidc_simple(auth_config, requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]}]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    with mock_secret_input(client_secret):
        cli.main(["add-oidc", "https://oeo.test", "--client-id", client_id])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (client_id, client_secret)


def test_add_oidc_no_secret(auth_config, requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]}]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    client_id = "z3-cl13nt"
    cli.main(["add-oidc", "https://oeo.test", "--client-id", client_id, "--no-client-secret"])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (client_id, None)


def test_add_oidc_use_default_client(auth_config, requests_mock, caplog):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{
            "id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"],
            "default_clients": [{
                "id": "d3f6ul7cl13n7",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
            }]
        }]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    cli.main(["add-oidc", "https://oeo.test", "--use-default-client"])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (None, None)
    warnings = [r[2] for r in caplog.record_tuples if r[1] == logging.WARN]
    assert warnings == []


def test_add_oidc_use_default_client_no_default(auth_config, requests_mock, caplog):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{
            "id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"],
        }]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    cli.main(["add-oidc", "https://oeo.test", "--use-default-client"])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (None, None)
    warnings = [r[2] for r in caplog.record_tuples if r[1] == logging.WARN]
    assert warnings == ["No default clients declared for provider 'authit'"]


def test_add_oidc_default_client_interactive(auth_config, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{
            "id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"],
            "default_clients": [{
                "id": "d3f6ul7cl13n7",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
            }]
        }]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    with mock_input("") as input:
        cli.main(["add-oidc", "https://oeo.test"])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (None, None)

    input.assert_called_with("Enter client_id or leave empty to use default client, and press enter: ")
    stdout = capsys.readouterr().out
    assert "Using client ID None" in stdout


def test_add_oidc_use_default_client_overwrite(auth_config, requests_mock, caplog):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={
        "providers": [{
            "id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"],
            "default_clients": [{
                "id": "d3f6ul7cl13n7",
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"]
            }]
        }]
    })
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})

    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    with mock_secret_input(client_secret):
        cli.main(["add-oidc", "https://oeo.test", "--client-id", client_id])
    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (client_id, client_secret)

    cli.main(["add-oidc", "https://oeo.test", "--use-default-client"])
    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (None, None)

    warnings = [r[2] for r in caplog.record_tuples if r[1] == logging.WARN]
    assert warnings == []


def test_add_oidc_04(auth_config, requests_mock):
    requests_mock.get("https://oeo.test/", json={"api_version": "0.4.0"})
    with pytest.raises(CliToolException, match="Backend API version is too low"):
        cli.main(["add-oidc", "https://oeo.test"])


def test_add_oidc_multiple_providers(auth_config, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]},
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    requests_mock.get("https://youauth.test/.well-known/openid-configuration", json={"issuer": "https://youauth.test"})
    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    with mock_secret_input(client_secret):
        cli.main(["add-oidc", "https://oeo.test", "--provider-id", "youauth", "--client-id", client_id])

    assert "youauth" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "youauth") == (client_id, client_secret)
    out = capsys.readouterr().out
    expected = ["Using provider ID 'youauth'", "Using client ID 'z3-cl13nt'"]
    for e in expected:
        assert e in out


def test_add_oidc_no_providers(auth_config, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": []})
    with pytest.raises(CliToolException, match="No OpenID Connect providers listed by backend"):
        cli.main(["add-oidc", "https://oeo.test"])
    with pytest.raises(CliToolException, match="No OpenID Connect providers listed by backend"):
        cli.main(["add-oidc", "https://oeo.test", "--provider-id", "youauth"])


def test_add_oidc_interactive(auth_config, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]},
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})
    requests_mock.get("https://authit.test/.well-known/openid-configuration", json={"issuer": "https://authit.test"})
    requests_mock.get("https://youauth.test/.well-known/openid-configuration", json={"issuer": "https://youauth.test"})
    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    with mock_input("1", client_id), mock_secret_input(client_secret):
        cli.main(["add-oidc", "https://oeo.test"])

    assert "authit" in auth_config.get_oidc_provider_configs("https://oeo.test")
    assert auth_config.get_oidc_client_configs("https://oeo.test", "authit") == (client_id, client_secret)
    out = capsys.readouterr().out
    expected = [
        "Backend 'https://oeo.test' has multiple OpenID Connect providers.",
        "[1] Auth It", "[2] YouAuth",
        "Using provider ID 'authit'",
        "Using client ID 'z3-cl13nt'"
    ]
    for e in expected:
        assert e in out


def test_oidc_auth_device_flow(auth_config, refresh_token_store, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]},
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})

    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    auth_config.set_oidc_client_config("https://oeo.test", "authit", client_id, client_secret)

    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=client_id,
        provider_root_url="https://authit.test",
        oidc_discovery_url="https://authit.test/.well-known/openid-configuration",
        expected_fields={"scope": "openid", "client_secret": client_secret},
        state={"device_code_callback_timeline": ["great success"]},
        scopes_supported=["openid"]
    )

    with assert_device_code_poll_sleep():
        cli.main(["oidc-auth", "https://oeo.test", "--flow", "device"])

    assert refresh_token_store.get_refresh_token("https://authit.test", client_id) == oidc_mock.state["refresh_token"]

    out = capsys.readouterr().out
    expected = [
        "Using provider ID 'authit'",
        "Using client ID 'z3-cl13nt'",
        "To authenticate: visit https://authit.test/dc",
        "enter the user code {c!r}".format(c=oidc_mock.state["user_code"]),
        "Authorized successfully.",
        "The OpenID Connect device flow was successful.",
        "Stored refresh token in {p!r}".format(p=str(refresh_token_store.path)),
    ]
    for e in expected:
        assert e in out


def test_oidc_auth_device_flow_default_client(auth_config, refresh_token_store, requests_mock, capsys):
    """Test device flow with default client (which uses PKCE instead of secret)."""
    default_client_id = "d3f6u17cl13n7"
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {
            "id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"],
            "default_clients": [{
                "id": default_client_id,
                "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
            }]
        },
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})

    auth_config.set_oidc_client_config("https://oeo.test", "authit", client_id=None, client_secret=None)

    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="urn:ietf:params:oauth:grant-type:device_code",
        expected_client_id=default_client_id,
        provider_root_url="https://authit.test",
        oidc_discovery_url="https://authit.test/.well-known/openid-configuration",
        expected_fields={"scope": "openid", "code_verifier": True, "code_challenge": True},
        state={"device_code_callback_timeline": ["great success"]},
        scopes_supported=["openid"]
    )

    with assert_device_code_poll_sleep():
        cli.main(["oidc-auth", "https://oeo.test", "--flow", "device"])

    stored_refresh_token = refresh_token_store.get_refresh_token("https://authit.test", default_client_id)
    assert stored_refresh_token == oidc_mock.state["refresh_token"]

    out = capsys.readouterr().out
    expected = [
        "Using provider ID 'authit'",
        "Will try to use default client.",
        "To authenticate: visit https://authit.test/dc",
        "enter the user code {c!r}".format(c=oidc_mock.state["user_code"]),
        "Authorized successfully.",
        "The OpenID Connect device flow was successful.",
        "Stored refresh token in {p!r}".format(p=str(refresh_token_store.path)),
    ]
    for e in expected:
        assert e in out


@pytest.mark.slow
def test_oidc_auth_auth_code_flow(auth_config, refresh_token_store, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]},
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})

    client_id, client_secret = "z3-cl13nt", "z3-z3cr3t-y6y6"
    auth_config.set_oidc_client_config("https://oeo.test", "authit", client_id, client_secret)
    auth_config.set_oidc_client_config("https://oeo.test", "youauth", client_id + '-tw00', client_secret + '-tw00')

    oidc_mock = OidcMock(
        requests_mock=requests_mock,
        expected_grant_type="authorization_code",
        expected_client_id=client_id,
        expected_fields={"scope": "openid"},
        provider_root_url="https://authit.test",
        oidc_discovery_url="https://authit.test/.well-known/openid-configuration",
        scopes_supported=["openid"]
    )

    with mock_input("1"), mock.patch.object(cli, "_webbrowser_open", new=oidc_mock.webbrowser_open):
        cli.main(["oidc-auth", "https://oeo.test", "--flow", "auth-code", "--timeout", "10"])

    assert refresh_token_store.get_refresh_token("https://authit.test", client_id) == oidc_mock.state["refresh_token"]

    out = capsys.readouterr().out
    expected = [
        "Using provider ID 'authit'",
        "Using client ID 'z3-cl13nt'",
        "a browser window should open allowing you to log in",
        "and grant access to the client 'z3-cl13nt' (timeout: 10s).",
        "The OpenID Connect authorization code flow was successful.",
        "Stored refresh token in {p!r}".format(p=str(refresh_token_store.path)),
    ]
    for e in expected:
        assert e in out


def test_oidc_auth_auth_code_flow_no_provider_configs(auth_config, refresh_token_store, requests_mock, capsys):
    requests_mock.get("https://oeo.test/", json={"api_version": "1.0.0"})
    requests_mock.get("https://oeo.test/credentials/oidc", json={"providers": [
        {"id": "authit", "issuer": "https://authit.test", "title": "Auth It", "scopes": ["openid"]},
        {"id": "youauth", "issuer": "https://youauth.test", "title": "YouAuth", "scopes": ["openid"]}
    ]})

    with pytest.raises(CliToolException, match="No OpenID Connect provider configs found"):
        cli.main(["oidc-auth", "https://oeo.test", "--flow", "auth-code", "--timeout", "10"])
