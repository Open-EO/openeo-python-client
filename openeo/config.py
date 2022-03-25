"""

openEO client configuration (e.g. through config files)

"""

import logging
import os
import platform
from configparser import ConfigParser
from copy import deepcopy
from pathlib import Path
from typing import Union, Any, Sequence, Iterator, Optional

_log = logging.getLogger(__name__)

DEFAULT_APP_NAME = "openeo-python-client"


def _get_user_dir(
        app_name=DEFAULT_APP_NAME,
        xdg_env_var='XDG_CONFIG_HOME',
        win_env_var='APPDATA',
        fallback='~/.config',
        win_fallback='~\\AppData\\Roaming',
        macos_fallback='~/Library/Preferences',
        auto_create=True,
) -> Path:
    """
    Get platform specific config/data/cache folder
    """
    # Platform specific root locations (from highest priority to lowest)
    env = os.environ
    if platform.system() == 'Windows':
        roots = [env.get(win_env_var), win_fallback, fallback]
    elif platform.system() == 'Darwin':
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
        xdg_env_var='XDG_CONFIG_HOME', win_env_var='APPDATA',
        fallback='~/.config', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library/Preferences',
        auto_create=auto_create
    )


def get_user_data_dir(app_name=DEFAULT_APP_NAME, auto_create=True) -> Path:
    """
    Get platform specific data folder
    """
    return _get_user_dir(
        app_name=app_name,
        xdg_env_var='XDG_DATA_HOME', win_env_var='APPDATA',
        fallback='~/.local/share', win_fallback='~\\AppData\\Roaming', macos_fallback='~/Library',
        auto_create=auto_create
    )


class ClientConfig:
    """
    openEO client configuration. Essentially a flat mapping of config key-value pairs.
    """

    # TODO: support for loading JSON based config files?

    def __init__(self):
        self._config = {}

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
        return self._config.get(self._key(key), default)

    def load_ini_file(self, path: Union[str, Path]) -> "ClientConfig":
        cp = ConfigParser()
        cp.read(path)
        return self.load_config_parser(cp)

    def load_config_parser(self, parser: ConfigParser) -> "ClientConfig":
        for section in parser.sections():
            for option, value in parser.items(section=section):
                self._set(key=(section, option), value=value)
        return self

    def dump(self) -> dict:
        return deepcopy(self._config)


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
        _log.info("Loading global config")
        config = ClientConfig()
        for path in cls.config_locations():
            _log.debug(f"Trying {path}")
            if path.exists():
                if path.suffix.lower() == ".ini":
                    _log.info(f"Loading config from {path}")
                    try:
                        config.load_ini_file(path)
                        break
                    except Exception:
                        _log.warning(f"Failed to load config from {path}", exc_info=True)
        return config


# Global config (lazily loaded by :py:func:`get_config`)
_global_config = None


def get_config(key: Optional[str] = None, default=None) -> Union[ClientConfig, str]:
    """Get a value from (or the whole) global :py:class:`ClientConfig` (lazily loaded)."""
    global _global_config
    if _global_config is None:
        _global_config = ConfigLoader.load()
    if key:
        return _global_config.get(key, default=default)
    else:
        return _global_config
