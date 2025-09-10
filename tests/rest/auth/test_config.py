import json
import logging
import platform
import re

import pytest

from openeo.rest.auth.config import (
    PRIVATE_PERMISSIONS,
    AuthConfig,
    PrivateJsonFile,
    RefreshTokenStore,
    get_file_mode,
    set_file_mode,
)


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

    def test_permissions_on_create(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        assert not private.path.exists()
        private.set("foo", "bar", value=42)
        assert private.path.exists()
        st_mode = get_file_mode(private.path)
        assert st_mode & 0o777 == 0o600

    def test_wrong_permissions_on_load(self, tmp_path, caplog):
        path = tmp_path / "my_data.secret"
        with path.open("w") as f:
            json.dump({"foo": "bar"}, f)
        assert path.stat().st_mode & 0o077 > 0

        private = PrivateJsonFile(path)

        if platform.system() != "Windows":
            with pytest.raises(PermissionError, match="readable by others.*expected: 600"):
                private.get("foo")
            with pytest.raises(PermissionError, match="readable by others.*expected: 600"):
                private.set("foo", value="lol")
        else:
            regex = re.compile("readable by others.*expected: 600")
            caplog.set_level(logging.INFO)

            private.get("foo")
            assert caplog.record_tuples[0][1] == logging.INFO
            message = caplog.record_tuples[0][2]
            assert regex.search(message)

            caplog.clear()
            private.set("foo", value="lol")
            assert caplog.record_tuples[0][1] == logging.INFO
            message = caplog.record_tuples[0][2]
            assert regex.search(message)

    def test_set_get(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        private.set("foo", "bar", value=42)
        assert private.get("foo", "bar") == 42
        with private.path.open("r") as f:
            data = json.load(f)
        assert data == {"foo": {"bar": 42}}

    def test_remove_no_file(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        assert not private.path.exists()
        # Don't complain/fail if it doesn't exist
        private.remove()

    def test_remove(self, tmp_path):
        private = PrivateJsonFile(tmp_path)
        private.set("foo", "bar", value=42)
        assert private.path.exists()
        private.remove()
        assert not private.path.exists()

    def test_load_corrupt_content(self, tmp_path):
        path = tmp_path / "my_data.secret"
        path.write_text("{\nInvalid JSON here!")
        set_file_mode(path, mode=PRIVATE_PERMISSIONS)

        private = PrivateJsonFile(path=path)
        with pytest.raises(
            RuntimeError, match=r"Failed to load PrivateJsonFile from .*my_data\.secret.*JSONDecodeError.*line 2"
        ):
            private.load()



class TestAuthConfig:

    def test_start_empty(self, tmp_path):
        config = AuthConfig(path=tmp_path)
        assert config.get_basic_auth("foo") == (None, None)
        assert config.get_oidc_client_configs("oeo.test", "default") == (None, None)

    def test_basic_auth(self, tmp_path, time_machine):
        time_machine.move_to("2020-06-08T11:18:27Z")
        config = AuthConfig(path=tmp_path)
        config.set_basic_auth("oeo.test", "John", "j0hn123")
        assert config.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == [AuthConfig.DEFAULT_FILENAME]
        with config.path.open("r") as f:
            data = json.load(f)
        assert data["backends"] == {"oeo.test": {
            "basic": {"date": "2020-06-08T11:18:27Z", "username": "John", "password": "j0hn123"}
        }}
        assert config.get_basic_auth("oeo.test") == ("John", "j0hn123")
        assert config.get_basic_auth("oeo.test") == ("John", "j0hn123")

    @pytest.mark.parametrize(["to_set", "to_get"], [
        ("https://oeo.test", "https://oeo.test/"),
        ("https://oeo.test/", "https://oeo.test"),
    ])
    def test_basic_auth_url_normalization(self, tmp_path, to_set, to_get):
        config = AuthConfig(path=tmp_path)
        config.set_basic_auth(to_set, "John", "j0hn123")
        assert config.get_basic_auth(to_set) == ("John", "j0hn123")
        assert config.get_basic_auth(to_get) == ("John", "j0hn123")

    def test_oidc(self, tmp_path, time_machine):
        time_machine.move_to("2020-06-08T11:18:27Z")
        config = AuthConfig(path=tmp_path)
        config.set_oidc_client_config("oeo.test", "default", client_id="client123", client_secret="$6cr67")
        assert config.path.exists()
        assert [p.name for p in tmp_path.iterdir()] == [AuthConfig.DEFAULT_FILENAME]
        with config.path.open("r") as f:
            data = json.load(f)
        assert data["backends"] == {"oeo.test": {"oidc": {"providers": {
            "default": {"date": "2020-06-08T11:18:27Z", "client_id": "client123", "client_secret": "$6cr67"}
        }}}}
        assert config.get_oidc_client_configs("oeo.test", "default") == ("client123", "$6cr67")
        assert config.get_oidc_provider_configs("oeo.test") == {
            "default": {"date": "2020-06-08T11:18:27Z", "client_id": "client123", "client_secret": "$6cr67"}
        }

    @pytest.mark.parametrize(["to_set", "to_get"], [
        ("https://oeo.test", "https://oeo.test/"),
        ("https://oeo.test/", "https://oeo.test"),
    ])
    def test_oidc_backend_normalization(self, tmp_path, to_set, to_get, time_machine):
        time_machine.move_to("2020-06-08T11:18:27Z")
        config = AuthConfig(path=tmp_path)
        config.set_oidc_client_config(to_set, "default", client_id="client123", client_secret="$6cr67")
        for backend in [to_set, to_get]:
            assert config.get_oidc_client_configs(backend, "default") == ("client123", "$6cr67")
            assert config.get_oidc_provider_configs(backend) == {
                "default": {"date": "2020-06-08T11:18:27Z", "client_id": "client123", "client_secret": "$6cr67"}
            }

    def test_tmp_openeo_config_home(self, tmp_openeo_config_home, tmp_path):
        expected_dir = str(tmp_path)
        assert str(AuthConfig.default_path()).startswith(expected_dir)
        assert not AuthConfig.default_path().exists()
        config = AuthConfig()
        assert str(config.path).startswith(expected_dir)
        assert config.load() == {}


class TestRefreshTokenStorage:

    def test_public_file(self, tmp_path, caplog):
        path = tmp_path / "refresh_tokens.json"
        with path.open("w") as f:
            json.dump({}, f)
        r = RefreshTokenStore(path=path)
        if platform.system() != "Windows":
            with pytest.raises(
                PermissionError, match="readable by others.*expected: 600"
            ):
                r.get_refresh_token("foo", "bar")
            with pytest.raises(
                PermissionError, match="readable by others.*expected: 600"
            ):
                r.set_refresh_token("foo", "bar", "imd6$3cr3t")
        else:
            regex = re.compile("readable by others.*expected: 600")
            caplog.set_level(logging.INFO)

            r.get_refresh_token("foo", "bar")
            assert caplog.record_tuples[0][1] == logging.INFO
            message = caplog.record_tuples[0][2]
            assert regex.search(message)

            caplog.clear()
            r.set_refresh_token("foo", "bar", "imd6$3cr3t")
            assert caplog.record_tuples[0][1] == logging.INFO
            message = caplog.record_tuples[0][2]
            assert regex.search(message)

    def test_permissions(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set_refresh_token("foo", "bar", "imd6$3cr3t")
        st_mode = get_file_mode(tmp_path / RefreshTokenStore.DEFAULT_FILENAME)
        assert st_mode & 0o777 == 0o600

    def test_get_set_refresh_token(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set_refresh_token("foo", "bar", "ih6zdaT0k3n")
        assert r.get_refresh_token("foo", "bar") == "ih6zdaT0k3n"
