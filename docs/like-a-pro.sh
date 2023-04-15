#!/usr/bin/env bash
# Run `sphinx-autobuild` to automatically rebuild the docs
# and reload your browser page as described at
# https://open-eo.github.io/openeo-python-client/development.html#like-a-pro .
# Assumes `sphinx-autobuild` to be installed in your dev environment
# (e.g. `pip install sphinx-autobuild`).

set -euxo pipefail

cd $(dirname $0)/..
pwd

sphinx-autobuild --open-browser --delay 3 docs/ --watch openeo/ docs/_build/html/
