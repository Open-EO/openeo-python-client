"""
Functionality to store and retrieve authentication settings (usernames, passwords, client ids, ...)
from local config files.
"""

import json
import logging
import stat
from datetime import datetime
from pathlib import Path
from typing import Union

from openeo.util import get_user_data_dir, rfc3339

log = logging.getLogger(__name__)


class RefreshTokenStore:
    """
    Basic JSON-file based storage of refresh tokens.
    """
    _PERMS = stat.S_IRUSR | stat.S_IWUSR

    DEFAULT_FILENAME = "refresh_tokens.json"

    class NoRefreshToken(Exception):
        pass

    def __init__(self, path: Path = None):
        if path is None:
            path = self.default_path()
        if path.is_dir():
            path = path / self.DEFAULT_FILENAME
        self._path = path

    @classmethod
    def default_path(cls) -> Path:
        return get_user_data_dir(auto_create=True) / cls.DEFAULT_FILENAME

    def _get_all(self, empty_on_file_not_found=True) -> dict:
        if not self._path.exists():
            if empty_on_file_not_found:
                return {}
            raise FileNotFoundError(self._path)
        mode = self._path.stat().st_mode
        if (mode & stat.S_IRWXG) or (mode & stat.S_IRWXO):
            raise PermissionError(
                "Refresh token file {p} is readable by others: st_mode {a:o} (expected permissions: {e:o}).".format(
                    p=self._path, a=mode, e=self._PERMS)
            )
        with self._path.open("r", encoding="utf8") as f:
            log.info("Using refresh tokens from {p}".format(p=self._path))
            return json.load(f)

    def _write_all(self, data: dict):
        with self._path.open("w", encoding="utf8") as f:
            json.dump(data, f, indent=2)
        self._path.chmod(mode=self._PERMS)

    def get(self, issuer: str, client_id: str, allow_miss=True) -> Union[str, None]:
        try:
            data = self._get_all()
            return data[issuer][client_id]["refresh_token"]
        except (FileNotFoundError, KeyError):
            if allow_miss:
                return None
            else:
                raise self.NoRefreshToken()

    def set(self, issuer: str, client_id: str, refresh_token: str):
        data = self._get_all(empty_on_file_not_found=True)
        log.info("Storing refresh token for issuer {i!r} (client {c!r})".format(i=issuer, c=client_id))
        # TODO: should OIDC grant type also be a part of the key?
        #       e.g. to avoid mixing client credential flow tokens with tokens of other (user oriented) flows?
        data.setdefault(issuer, {}).setdefault(client_id, {
            "date": rfc3339.datetime(datetime.utcnow()),
            "refresh_token": refresh_token,
        })
        self._write_all(data)
