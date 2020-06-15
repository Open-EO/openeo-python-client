import os
from pathlib import Path

# Make tests more predictable and avoid loading real configs in tests.
os.environ["XDG_CONFIG_HOME"] = str(Path(__file__).parent / "data/user_dirs/config")
os.environ["XDG_DATA_HOME"] = str(Path(__file__).parent / "data/user_dirs/data")
