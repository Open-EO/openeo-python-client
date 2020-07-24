"""
Functionality to store and retrieve authentication settings (usernames, passwords, client ids, ...)
from local config files.
"""

# TODO: also allow to set client_id, client_secret, refresh_token through env variables?


import json
import logging
import stat
from datetime import datetime
from pathlib import Path
from typing import Union, Tuple, Dict

from openeo import __version__
from openeo.util import get_user_data_dir, rfc3339, get_user_config_dir, deep_get, deep_set

_PRIVATE_PERMS = stat.S_IRUSR | stat.S_IWUSR

log = logging.getLogger(__name__)


def assert_private_file(path: Path):
    """Check that given file is only readable by user."""
    mode = path.stat().st_mode
    if (mode & stat.S_IRWXG) or (mode & stat.S_IRWXO):
        raise PermissionError(
            "File {p} is readable by others: st_mode {a:o} (expected permissions: {e:o}).".format(
                p=path, a=mode, e=_PRIVATE_PERMS)
        )


def utcnow_rfc3339() -> str:
    """Current datetime formatted as RFC-3339 string."""
    return rfc3339.datetime(datetime.utcnow())


def _normalize_url(url: str) -> str:
    """Normalize a url (trim trailing slash), to simplify equality checking."""
    return url.rstrip("/") or "/"


class PrivateJsonFile:
    """
    Base class for private config/data files in JSON format.
    """

    DEFAULT_FILENAME = "private.json"

    def __init__(self, path: Path = None):
        if path is None:
            path = self.default_path()
        if path.is_dir():
            path = path / self.DEFAULT_FILENAME
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    @classmethod
    def default_path(cls) -> Path:
        return get_user_config_dir(auto_create=True) / cls.DEFAULT_FILENAME

    def load(self, empty_on_file_not_found=True) -> dict:
        """Load all data from file"""
        if not self._path.exists():
            if empty_on_file_not_found:
                return {}
            raise FileNotFoundError(self._path)
        assert_private_file(self._path)
        log.debug("Loading private JSON file {p}".format(p=self._path))
        # TODO: add file locking to avoid race conditions?
        with self._path.open("r", encoding="utf8") as f:
            return json.load(f)

    def _write(self, data: dict):
        """Write whole data to file."""
        log.debug("Writing private JSON file {p}".format(p=self._path))
        # TODO: add file locking to avoid race conditions?
        with self._path.open("w", encoding="utf8") as f:
            json.dump(data, f, indent=2)
        self._path.chmod(mode=_PRIVATE_PERMS)
        assert_private_file(self._path)

    def get(self, *keys, default=None) -> Union[dict, str, int]:
        """Load JSON file and do deep get with given keys."""
        result = deep_get(self.load(), *keys, default=default)
        if isinstance(result, Exception) or (isinstance(result, type) and issubclass(result, Exception)):
            # pylint: disable=raising-bad-type
            raise result
        return result

    def set(self, *keys, value):
        data = self.load()
        deep_set(data, *keys, value=value)
        self._write(data)


class AuthConfig(PrivateJsonFile):
    DEFAULT_FILENAME = "auth-config.json"

    @classmethod
    def default_path(cls) -> Path:
        return get_user_config_dir(auto_create=True) / cls.DEFAULT_FILENAME

    def _write(self, data: dict):
        # When starting fresh: add some metadata and defaults
        if "metadata" not in data:
            data["metadata"] = {
                "type": "AuthConfig",
                "created": utcnow_rfc3339(),
                "created_by": "openeo-python-client {v}".format(v=__version__),
                "version": 1,
            }
            data.setdefault("general", {})
            data.setdefault("backends", {})
        return super()._write(data=data)

    def get_basic_auth(self, backend: str) -> Tuple[Union[None, str], Union[None, str]]:
        """Get username/password combo for given backend. Values will be None when no config is available."""
        basic = self.get("backends", _normalize_url(backend), "basic", default={})
        username = basic.get("username")
        password = basic.get("password") if username else None
        return username, password

    def set_basic_auth(self, backend: str, username: str, password: Union[str, None]):
        data = self.load()
        keys = ("backends", _normalize_url(backend), "basic",)
        # TODO: support multiple basic auth credentials? (pick latest by default for example)
        deep_set(data, *keys, "date", value=utcnow_rfc3339())
        deep_set(data, *keys, "username", value=username)
        if password:
            deep_set(data, *keys, "password", value=password)
        self._write(data)

    def get_oidc_provider_configs(self, backend: str) -> Dict[str, dict]:
        """
        Get provider config items for given backend.

        Returns a dict mapping provider_id to dicts with "client_id" and "client_secret" items
        """
        return self.get("backends", _normalize_url(backend), "oidc", "providers", default={})

    def get_oidc_client_configs(self, backend: str, provider_id: str) -> Tuple[str, str]:
        """
        Get client_id and client_secret for given backend+provider_id. Values will be None when no config is available.
        """
        client = self.get("backends", _normalize_url(backend), "oidc", "providers", provider_id, default={})
        client_id = client.get("client_id")
        client_secret = client.get("client_secret") if client_id else None
        return client_id, client_secret

    def set_oidc_client_config(
            self, backend: str, provider_id: str, client_id: str, client_secret: str = None, issuer: str = None
    ):
        data = self.load()
        keys = ("backends", _normalize_url(backend), "oidc", "providers", provider_id)
        # TODO: support multiple clients? (pick latest by default for example)
        deep_set(data, *keys, "date", value=utcnow_rfc3339())
        deep_set(data, *keys, "client_id", value=client_id)
        if client_secret:
            deep_set(data, *keys, "client_secret", value=client_secret)
        if issuer:
            deep_set(data, *keys, "issuer", value=issuer)
        self._write(data)


class RefreshTokenStore(PrivateJsonFile):
    """
    Basic JSON-file based storage of refresh tokens.
    """

    DEFAULT_FILENAME = "refresh-tokens.json"

    @classmethod
    def default_path(cls) -> Path:
        return get_user_data_dir(auto_create=True) / cls.DEFAULT_FILENAME

    def get_refresh_token(self, issuer: str, client_id: str) -> Union[str, None]:
        return self.get(_normalize_url(issuer), client_id, "refresh_token", default=None)

    def set_refresh_token(self, issuer: str, client_id: str, refresh_token: str):
        data = self.load()
        log.info("Storing refresh token for issuer {i!r} (client {c!r})".format(i=issuer, c=client_id))
        deep_set(data, _normalize_url(issuer), client_id, value={
            "date": utcnow_rfc3339(),
            "refresh_token": refresh_token,
        })
        self._write(data)
