#!/usr/bin/env python
"""
Script to manage/update specification sub-repositories,
in the style of git submodules, but without requiring
them for all development situations
because they are only necessary for a small subset of
maintenance tasks.

This script should be runnable without non-stdlib dependencies.
"""

# TODO: windows support?

import contextlib
import logging
import logging.config
import os
import subprocess
import sys
from pathlib import Path
from typing import List, NamedTuple


_log = logging.getLogger(__name__)
_ROOT_DIR = Path(__file__).parent


class SubRepo(NamedTuple):
    url: str
    rev: str
    path: str


def main():
    logging.basicConfig(
        level=logging.INFO, format=">>> {message}", style="{", stream=sys.stdout
    )
    subrepos = [
        SubRepo(
            url="https://github.com/Open-EO/openeo-processes.git",
            rev="ca9e31094b863233d88459b6cf2a37416bc90d4e",
            path="openeo-processes",
        )
    ]

    for subrepo in subrepos:
        ensure_subrepo(subrepo)

    run_command(["ls", "-al", _ROOT_DIR])


def run_command(cmd: List[str], *, return_stdout: bool = False, **kwargs):
    _log.info(f"Running {cmd} ({kwargs=})")
    if not return_stdout:
        return subprocess.check_call(args=cmd, **kwargs)
    else:
        return subprocess.check_output(args=cmd, **kwargs)


@contextlib.contextmanager
def in_cwd(path: Path):
    orig_cwd = Path.cwd()
    try:
        _log.info(f"Changing working directory to {path}")
        os.chdir(path)
        yield
    finally:
        _log.info(f"Resetting working directory to {path}")
        os.chdir(orig_cwd)


def ensure_subrepo(subrepo: SubRepo):
    path = _ROOT_DIR / subrepo.path
    _log.info(f"Ensuring subrepo {subrepo}")
    if not path.exists():
        run_command(["git", "clone", subrepo.url, str(path)])
        run_command(["git", "config", "advice.detachedHead", "false"], cwd=path)

    if path.is_dir() and (path / ".git").is_dir():
        _log.info(f"{path} already looks like a git repo")
    else:
        raise RuntimeError(f"{path} exists but does not look like a git repo")

    # Checkout to desired revision
    run_command(["git", "checkout", subrepo.rev], cwd=path)
    run_command(["git", "log", "-1"], cwd=path)
    run_command(["git", "submodule", "update", "--init", "--recursive"], cwd=path)

    # Check that repo is not dirty
    dirty = run_command(
        ["git", "status", "--short", "--untracked-files=all", "--ignored"],
        cwd=path,
        return_stdout=True,
    )
    if dirty.strip():
        raise RuntimeError(f"Repo {path} seems dirty: {dirty}")


if __name__ == "__main__":
    main()
