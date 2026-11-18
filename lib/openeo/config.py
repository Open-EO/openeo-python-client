"""

openEO client configuration (e.g. through config files)

"""

from __future__ import annotations

import logging
import os
import platform
from configparser import ConfigParser
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterator, List, Optional, Sequence, Union

from openeo.util import in_interactive_mode

_log = logging.getLogger(__name__)

DEFAULT_APP_NAME = "openeo-python-client"


def _get_user_dir(
    app_name=DEFAULT_APP_NAME,
    xdg_env_var="XDG_CONFIG_HOME",
    win_env_var="APPDATA",
    fallback="~/.config",
    win_fallback="~\\AppData\\Roaming",
    macos_fallback="~/Library/Preferences",
    auto_create=True,
) -> Path:
    """
    Get platform specific config/data/cache folder
    """
    # Platform specific root locations (from highest priority to lowest)
    env = os.environ
    if platform.system() == "Windows":
        roots = [env.get(win_env_var), win_fallback, fallback]
    elif platform.system() == "Darwin":
        roots = [env.get(xdg_env_var), macos_fallback, fallback]
    else:
        # Assume unix
        roots = [env.get(xdg_env_var), fallback]

    # Filter out None's, expand user prefix and append app name
    dirs = [Path(r).expanduser() / app_name for r in roots if r]
    # Prepend with OPENEO_CONFIG_HOME if set.
    if env.get("OPENEO_CONFIG_HOME"):
        dirs.insert(0, Path(env.get("OPENEO_CONFIG_HOME")))

    # Use highest prio dir that already exists.
    for p in dirs:
        if p.exists() and p.is_dir():
            return p

    # No existing dir: create highest prio one (if possible)
    if auto_create:
        for p in dirs:
            try:
                p.mkdir(parents=True)
                _log.info("Created user dir for {a!r}: {p}".format(a=app_name, p=p))
                return p
            except OSError:
                pass

    raise Exception("Failed to find user dir for {a!r}. Tried: {p!r}".format(a=app_name, p=dirs))


def get_user_config_dir(app_name=DEFAULT_APP_NAME, auto_create=True) -> Path:
    """
    Get platform specific config folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var="XDG_CONFIG_HOME",
        win_env_var="APPDATA",
        fallback="~/.config",
        win_fallback="~\\AppData\\Roaming",
        macos_fallback="~/Library/Preferences",
        auto_create=auto_create,
    )


def get_user_data_dir(app_name=DEFAULT_APP_NAME, auto_create=True) -> Path:
    """
    Get platform specific data folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var="XDG_DATA_HOME",
        win_env_var="APPDATA",
        fallback="~/.local/share",
        win_fallback="~\\AppData\\Roaming",
        macos_fallback="~/Library",
        auto_create=auto_create,
    )


class ClientConfig:
    """
    openEO client configuration. Essentially a flat mapping of config key-value pairs.
    """

    # TODO: support for loading JSON based config files?

    def __init__(self):
        self._config = {}
        self._sources = []

    @classmethod
    def _key(cls, key: Union[str, Sequence[str]]):
        """Normalize a key: make lower case and flatten sequences"""
        if not isinstance(key, str):
            key = ".".join(str(k) for k in key)
        return key.lower()

    def _set(self, key: Union[str, Sequence[str]], value: Any):
        """Set config value at key"""
        self._config[self._key(key)] = value

    def get(self, key: Union[str, Sequence[str]], default=None) -> Any:
        """Get setting at given key"""
        # TODO: option to cast/convert to certain type?
        return self._config.get(self._key(key), default)

    def load_ini_file(self, path: Union[str, Path]) -> ClientConfig:
        cp = ConfigParser()
        read_ok = cp.read(path)
        self._sources.extend(read_ok)
        return self.load_config_parser(cp)

    def load_config_parser(self, parser: ConfigParser) -> ClientConfig:
        for section in parser.sections():
            for option, value in parser.items(section=section):
                self._set(key=(section, option), value=value)
        return self

    def dump(self) -> dict:
        return deepcopy(self._config)

    @property
    def sources(self) -> List[str]:
        return [str(s) for s in self._sources]

    def __repr__(self):
        return f"<{type(self).__name__} from {self.sources}>"


class ConfigLoader:
    @classmethod
    def config_locations(cls) -> Iterator[Path]:
        """Config location candidates"""
        # From highest to lowest priority
        if "OPENEO_CLIENT_CONFIG" in os.environ:
            yield Path(os.environ["OPENEO_CLIENT_CONFIG"])
        yield Path.cwd() / "openeo-client-config.ini"
        if "OPENEO_CONFIG_HOME" in os.environ:
            yield Path(os.environ["OPENEO_CONFIG_HOME"]) / "openeo-client-config.ini"
        if "XDG_CONFIG_HOME" in os.environ:
            yield Path(os.environ["XDG_CONFIG_HOME"]) / DEFAULT_APP_NAME / "openeo-client-config.ini"
        yield Path.home() / ".openeo-client-config.ini"

    @classmethod
    def load(cls) -> ClientConfig:
        # TODO: (option to) merge layered configs instead of returning on first hit?
        config = ClientConfig()
        for path in cls.config_locations():
            _log.debug(f"Config file candidate: {path}")
            if path.exists():
                if path.suffix.lower() == ".ini":
                    _log.debug(f"Loading config from {path}")
                    try:
                        config.load_ini_file(path)
                        break
                    except Exception:
                        _log.warning(f"Failed to load config from {path}", exc_info=True)
        return config


# Global config (lazily loaded by :py:func:`get_config`)
_global_config = None


def get_config() -> ClientConfig:
    """Get global openEO client config (:py:class:`ClientConfig`) (lazy loaded)."""
    global _global_config
    if _global_config is None:
        _global_config = ConfigLoader.load()
        # Note: explicit `', '.join()` instead of implicit `repr` on full `sources` list
        # as the latter causes ugly escaping of Windows path separator.
        message = f"Loaded openEO client config from sources: [{', '.join(_global_config.sources)}]"
        _log.info(message)
        if _global_config.sources:
            config_log(message)

    return _global_config


def get_config_option(key: Optional[str] = None, default=None) -> str:
    """Get config value for given key from global config (lazy loaded)."""
    return get_config().get(key=key, default=default)


def config_log(message: str):
    """Print a config related message if verbosity is configured for that."""
    verbose = get_config_option("general.verbose", default="auto")
    if verbose == "print" or (verbose == "auto" and in_interactive_mode()):
        print(message)
