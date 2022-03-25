import contextlib
import os
import random
import textwrap
from configparser import ConfigParser
from pathlib import Path
from unittest import mock

import pytest

from openeo.config import get_user_config_dir, get_user_data_dir, ClientConfig, get_config, ConfigLoader


def test_get_user_config_dir():
    assert get_user_config_dir() == Path(__file__).parent / "data/user_dirs/config/openeo-python-client"


def test_get_user_data_dir():
    assert get_user_data_dir() == Path(__file__).parent / "data/user_dirs/data/openeo-python-client"


class TestClientConfig:

    def test_default(self):
        config = ClientConfig()
        assert config.dump() == {}
        assert config.get("foo") is None
        assert config.get("foo", default="bar") == "bar"

    def test_load_config_parser(self):
        cp = ConfigParser()
        cp.read_string("""
            [Connection]
            default_backend = openeo.cloud
            [Foo.Bar]
            baz.xev = Yep
        """)
        config = ClientConfig().load_config_parser(cp)
        assert config.dump() == {
            "connection.default_backend": "openeo.cloud",
            "foo.bar.baz.xev": "Yep"
        }
        assert config.get("connection.default_backend") == "openeo.cloud"

    def test_load_ini_file(self, tmp_path):
        path = tmp_path / "openeo.ini"
        path.write_text(textwrap.dedent("""
            [Connection]
            default_backend = openeo.cloud
            [Foo.Bar]
            baz.xev = Yep
        """))
        config = ClientConfig().load_ini_file(path)
        assert config.dump() == {
            "connection.default_backend": "openeo.cloud",
            "foo.bar.baz.xev": "Yep"
        }
        assert config.get("connection.default_backend") == "openeo.cloud"
        assert config.get(("connection", "default_backend")) == "openeo.cloud"
        assert config.get(("Connection", "Default_Backend")) == "openeo.cloud"


@contextlib.contextmanager
def working_dir(path):
    orig = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(orig)


class TestConfigLoader:

    def _create_config(self, path: Path, default_backend="openeo.test"):
        path.parent.mkdir(exist_ok=True)
        path.write_text(textwrap.dedent(f"""
            [Connection]
            default_backend = {default_backend}
        """))

    def test_load_from_OPENEO_CLIENT_CONFIGg(self, tmp_path):
        path = tmp_path / "my-openeo-conf.ini"
        default_backend = f"openeo{random.randint(0, 10000)}.test"
        self._create_config(path, default_backend=default_backend)
        with mock.patch.dict(os.environ, {"OPENEO_CLIENT_CONFIG": str(path)}):
            config = ConfigLoader.load()
        assert config.get("connection.default_backend") == default_backend

    def test_load_from_cwd(self, tmp_path):
        path = tmp_path / "openeo-client-config.ini"
        default_backend = f"openeo{random.randint(0, 10000)}.test"
        self._create_config(path, default_backend=default_backend)
        with working_dir(tmp_path):
            config = ConfigLoader.load()
        assert config.get("connection.default_backend") == default_backend

    def test_load_from_OPENEO_CONFIG_HOME(self, tmp_openeo_config_home):
        path = tmp_openeo_config_home / "openeo-client-config.ini"
        default_backend = f"openeo{random.randint(0, 10000)}.test"
        self._create_config(path, default_backend=default_backend)
        config = ConfigLoader.load()
        assert config.get("connection.default_backend") == default_backend

    def test_load_from_XDG_CONFIG_HOME(self, tmp_path):
        path = tmp_path / "openeo-python-client" / "openeo-client-config.ini"
        default_backend = f"openeo{random.randint(0, 10000)}.test"
        self._create_config(path, default_backend=default_backend)
        with mock.patch.dict(os.environ, {"XDG_CONFIG_HOME": str(tmp_path)}):
            config = ConfigLoader.load()
        assert config.get("connection.default_backend") == default_backend
