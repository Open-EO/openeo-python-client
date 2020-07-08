import json
from unittest import mock

import pytest

import openeo.rest.auth.config
from openeo.rest.auth.config import RefreshTokenStore, AuthConfig, PrivateJsonFile


class TestPrivateJsonFile:

    def test_empty(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        assert private.get("foo") is None
        assert not private.path.exists()

    def test_provide_dir_path(self, tmp_path):
        private = PrivateJsonFile(path=tmp_path)
        private.set("foo", "bar", value=42)
        assert private.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == [PrivateJsonFile.DEFAULT_FILENAME]

    def test_provide_file_path(self, tmp_path):
        private = PrivateJsonFile(path=tmp_path / "my_data.secret")
        private.set("foo", "bar", value=42)
        assert private.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == ["my_data.secret"]

    def test_permissions(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        assert not private.path.exists()
        private.set("foo", "bar", value=42)
        assert private.path.exists()
        st_mode = private.path.stat().st_mode
        assert st_mode & 0o777 == 0o600

    def test_wrong_permissions(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        with private.path.open("w") as f:
            json.dump({"foo": "bar"}, f)
        assert private.path.stat().st_mode & 0o077 > 0
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            private.get("foo")
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            private.set("foo", value="lol")

    def test_set_get(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        private.set("foo", "bar", value=42)
        assert private.get("foo", "bar") == 42
        with private.path.open("r") as f:
            data = json.load(f)
        assert data == {"foo": {"bar": 42}}


class TestAuthConfig:

    def test_start_empty(self, tmp_path):
        config = AuthConfig(path=tmp_path)
        assert config.get_basic_auth("foo") == (None, None)
        assert config.get_oidc_client_info("oeo.net", "default") == (None, None)

    def test_basic(self, tmp_path):
        config = AuthConfig(path=tmp_path)
        config.set_basic_auth("oeo.net", "John", "j0hn123")
        assert config.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == [AuthConfig.DEFAULT_FILENAME]
        with config.path.open("r") as f:
            data = json.load(f)
        assert data["backends"] == {"oeo.net": {"basic": {"username": "John", "password": "j0hn123"}}}
        assert config.get_basic_auth("oeo.net") == ("John", "j0hn123")

    def test_oidc(self, tmp_path):
        config = AuthConfig(path=tmp_path)
        with mock.patch.object(openeo.rest.auth.config, "utcnow_rfc3339", return_value="2020-06-08T11:18:27Z"):
            config.set_oidc_client_info("oeo.net", "default", client_id="client123", client_secret="$6cr67")
        assert config.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == [AuthConfig.DEFAULT_FILENAME]
        with config.path.open("r") as f:
            data = json.load(f)
        assert data["backends"] == {"oeo.net": {"oidc": {"providers": {"default": {
            "date": "2020-06-08T11:18:27Z",
            "client_id": "client123",
            "client_secret": "$6cr67"
        }}}}}
        assert config.get_oidc_client_info("oeo.net", "default") == ("client123", "$6cr67")


class TestRefreshTokenStorage:

    def test_public_file(self, tmp_path):
        path = tmp_path / "refresh_tokens.json"
        with path.open("w") as f:
            json.dump({}, f)
        r = RefreshTokenStore(path=path)
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            r.get_refresh_token("foo", "bar")
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            r.set_refresh_token("foo", "bar", "imd6$3cr3t")

    def test_permissions(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set_refresh_token("foo", "bar", "imd6$3cr3t")
        st_mode = (tmp_path / RefreshTokenStore.DEFAULT_FILENAME).stat().st_mode
        assert st_mode & 0o777 == 0o600

    def test_get_set(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set_refresh_token("foo", "bar", "ih6zdaT0k3n")
        assert r.get_refresh_token("foo", "bar") == "ih6zdaT0k3n"
