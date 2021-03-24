import os
from pathlib import Path
from unittest import mock

import pytest

from openeo.util import ensure_dir

# Make tests more predictable and avoid loading real configs in tests.
os.environ["XDG_CONFIG_HOME"] = str(Path(__file__).parent / "data/user_dirs/config")
os.environ["XDG_DATA_HOME"] = str(Path(__file__).parent / "data/user_dirs/data")


@pytest.fixture
def tmp_openeo_config_home(tmp_path):
    """
    Fixture to set `OPENEO_CONFIG_HOME` env var to temp path,
    which is used as default for get_user_config_dir, get_user_data_dir, AuthConfig, PrivateJsonFile, ...
    """
    path = ensure_dir(Path(str(tmp_path)) / "openeo-conf")
    with mock.patch.dict("os.environ", {"OPENEO_CONFIG_HOME": str(path)}):
        yield path
