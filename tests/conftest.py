import os
from pathlib import Path
from unittest import mock

import pytest

from openeo.util import ensure_dir

pytest_plugins = "pytester"

# Make tests more predictable and avoid loading real configs in tests.
os.environ["XDG_CONFIG_HOME"] = str(Path(__file__).parent / "data/user_dirs/config")
os.environ["XDG_DATA_HOME"] = str(Path(__file__).parent / "data/user_dirs/data")
# Windows does not use the *nix environment variables XDG_CONFIG_HOME and XDG_DATA_HOME
# It has only one corresponding folder for both config and data: APPDATA
os.environ["APPDATA"] = str(Path(__file__).parent / "data/user_dirs/AppData/Roaming")


@pytest.fixture
def tmp_openeo_config_home(tmp_path):
    """
    Fixture to set `OPENEO_CONFIG_HOME` env var to temp path,
    which is used as default for get_user_config_dir, get_user_data_dir, AuthConfig, PrivateJsonFile, ...
    """
    path = ensure_dir(Path(str(tmp_path)) / "openeo-conf")
    with mock.patch.dict("os.environ", {"OPENEO_CONFIG_HOME": str(path)}):
        yield path
