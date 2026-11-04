"""
Helpers to derive a job title or description from the script that creates it.

These helpers are opt-in: nothing in the openEO Python client calls them automatically.
"""

from __future__ import annotations

import inspect
import logging
import subprocess
import textwrap
import tokenize
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Optional, Tuple, Union

logger = logging.getLogger(__name__)

#: Default maximum number of source code characters included by :py:func:`auto_description`.
DEFAULT_SOURCE_MAX_LENGTH = 1000

_GIT_TIMEOUT = 5


@dataclass(frozen=True)
class GitInfo:
    """
    Git information about the repository and source file of a job script.

    :param commit: the full hash of the requested Git revision.
    :param dirty: whether the repository has uncommitted changes (including untracked files).
    :param file_tracked: whether the source file is tracked by Git.
    """

    commit: str
    dirty: bool
    file_tracked: bool


def _caller_frame(*, stacklevel: int = 1) -> Optional[FrameType]:
    """Get a frame from the call stack, where ``stacklevel=1`` is the caller of the calling helper."""
    frame = inspect.currentframe()
    try:
        for _ in range(stacklevel + 1):
            if frame is None:
                return None
            frame = frame.f_back
        return frame
    finally:
        del frame


def _detect_caller() -> Tuple[Optional[Path], Optional[str]]:
    """Detect the source file and function name of the code that calls the public helper."""
    frame = _caller_frame(stacklevel=2)
    try:
        if frame is None:
            return None, None
        filename = frame.f_code.co_filename
        source_path = None
        if not filename.startswith("<") and Path(filename).is_file():
            source_path = Path(filename)
        function = frame.f_code.co_name
        return source_path, None if function == "<module>" else function
    finally:
        del frame


def get_source_path(src: Optional[Union[str, Path]] = None) -> Optional[Path]:
    """
    Get the path of the Python source file that describes a job.

    :param src: explicit path to a Python script (a string or :py:class:`~pathlib.Path`).
        If omitted, the file of the calling code is detected.
    :return: the source path, or ``None`` if no source file can be detected
        (for example in a notebook or interactive session).
    """
    if src is not None:
        return Path(src)
    return _detect_caller()[0]


def get_function_name() -> Optional[str]:
    """
    Get the name of the calling function.

    :return: the function name, or ``None`` if the caller is a module or the frame is unavailable.
    """
    return _detect_caller()[1]


def _validate_git_ref(ref: str) -> None:
    if not ref or ref.startswith("-"):
        raise ValueError(f"Invalid Git reference: {ref!r}")


def _git(*args: str, cwd: Path) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=cwd,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=_GIT_TIMEOUT,
    )


def _get_git_info(source_path: Optional[Path], *, ref: str) -> Optional[GitInfo]:
    _validate_git_ref(ref)
    if source_path is None:
        logger.warning("Could not attach Git information: no source file is available.")
        return None

    try:
        start_dir = source_path.resolve().parent
        repo_root = Path(_git("rev-parse", "--show-toplevel", cwd=start_dir).strip()).resolve()
        commit = _git("rev-parse", "--verify", ref, cwd=repo_root).strip()
        dirty = bool(_git("status", "--porcelain", cwd=repo_root).strip())
        try:
            relative_path = source_path.resolve().relative_to(repo_root)
            _git("ls-files", "--error-unmatch", "--", str(relative_path), cwd=repo_root)
            file_tracked = True
        except (subprocess.CalledProcessError, ValueError):
            file_tracked = False
    except (OSError, subprocess.SubprocessError) as exc:
        # Do not log subprocess output, which could contain sensitive information.
        logger.warning("Could not attach Git information (%s).", type(exc).__name__)
        return None

    return GitInfo(commit=commit, dirty=dirty, file_tracked=file_tracked)


def get_git_info(src: Optional[Union[str, Path]] = None, *, ref: str = "HEAD") -> Optional[GitInfo]:
    """
    Look up Git information about the repository and source file of a job.

    :param src: path to the Python script (a string or :py:class:`~pathlib.Path`).
        If omitted, the file of the calling code is detected.
    :param ref: Git reference to report, e.g. ``"HEAD"`` (default), a branch name or a commit hash.
    :return: the Git information, or ``None`` if it could not be obtained
        (for example because the source file or the ``git`` executable is unavailable).
    :raises ValueError: if the Git reference is invalid.

    The commit is that of the whole repository, so :py:attr:`GitInfo.dirty` and
    :py:attr:`GitInfo.file_tracked` tell whether the source file matches that revision.
    """
    source_path = Path(src) if src is not None else _detect_caller()[0]
    return _get_git_info(source_path, ref=ref)


def _read_source_code(source_path: Optional[Path], *, max_length: Optional[int]) -> str:
    if max_length is not None and max_length < 0:
        raise ValueError("max_length should be non-negative or None.")
    if source_path is None:
        raise ValueError("Cannot include source code: no source file is available.")

    with tokenize.open(source_path) as source_file:
        source = source_file.read()

    total_length = len(source)
    if max_length is not None and total_length > max_length:
        source = source[:max_length] + f"\n... (truncated: {max_length} of {total_length} characters)"
    return source


def get_source_code(
    src: Optional[Union[str, Path]] = None,
    *,
    max_length: Optional[int] = DEFAULT_SOURCE_MAX_LENGTH,
) -> str:
    """
    Read the source code of a job script.

    :param src: path to the Python script (a string or :py:class:`~pathlib.Path`).
        If omitted, the file of the calling code is detected.
    :param max_length: maximum number of characters to return (1000 by default).
        Longer source code is truncated with a marker reporting the original length.
        Pass ``None`` for the full source code.
    :return: the source code, read using the declared Python source encoding.
    :raises ValueError: if no source file can be detected or ``max_length`` is negative.
    :raises OSError: if the source file cannot be read.
    """
    source_path = Path(src) if src is not None else _detect_caller()[0]
    return _read_source_code(source_path, max_length=max_length)


def auto_description(
    src: Optional[Union[str, Path]] = None,
    *,
    include_source: bool = False,
    source_max_length: Optional[int] = DEFAULT_SOURCE_MAX_LENGTH,
    include_git_commit: bool = False,
    git_ref: str = "HEAD",
) -> str:
    """
    Build a job description with information about the script that creates it.

    Pass the returned string explicitly as the ``description`` argument of
    :py:meth:`~openeo.rest.datacube.DataCube.create_job` or
    :py:meth:`~openeo.rest.datacube.DataCube.execute_batch`.

    :param src: path to the Python script (a string or :py:class:`~pathlib.Path`).
        If omitted, the caller's file and function are detected.
        Only the filename, not the full path, is included.
    :param include_source: include the script's source as an indented Markdown code block.
        Disabled by default: it can contain credentials or other sensitive information.
    :param source_max_length: maximum number of source code characters to include (1000 by default).
        Longer source is truncated with a marker reporting the original length.
        Pass ``None`` for the full source code.
    :param include_git_commit: include the Git revision of the repository containing the script.
        Requires the ``git`` executable. Unavailable information is logged and omitted.
    :param git_ref: Git reference to report when ``include_git_commit`` is enabled,
        e.g. ``"HEAD"`` (default), a branch name or a commit hash.
    :return: the description, with blank lines separating metadata and optional source code.
    :raises ValueError: if source code is requested but no source file can be detected,
        or if Git information is requested with an invalid ``git_ref``.
    :raises OSError: if source code is requested but the file cannot be read.

    The reported commit is that of the whole repository, not the last commit touching
    the script, so uncommitted changes and an untracked source file are reported separately.

    .. warning::
        Review the generated description before submitting it, especially when
        including source code. No secrets are automatically redacted.
    """
    if src is None:
        source_path, function = _detect_caller()
    else:
        source_path, function = Path(src), None

    sections = [f"Source: {source_path.name}" if source_path is not None else "Source: unavailable"]
    if function is not None:
        sections.append(f"Function: {function}")

    if include_git_commit:
        git_info = _get_git_info(source_path, ref=git_ref)
        if git_info is not None:
            sections.append(f"Git commit: {git_info.commit}")
            if git_info.dirty:
                sections.append("Git working tree: uncommitted changes")
            if not git_info.file_tracked:
                sections.append("Git source file: untracked")

    if include_source:
        source = _read_source_code(source_path, max_length=source_max_length)
        sections.append("Python source:\n\n" + textwrap.indent(source, "    "))

    return "\n\n".join(sections)


def auto_title(src: Optional[Union[str, Path]] = None) -> str:
    """
    Build a short job title from the script that creates it.

    :param src: path to the Python script (a string or :py:class:`~pathlib.Path`).
        If omitted, the caller's file and, inside a function, function name are detected.
    :return: ``"<filename>: <function>"``, ``"<filename>"`` if the function is unavailable,
        or ``"openEO job"`` if no source file can be detected.
    """
    source_path, function = _detect_caller() if src is None else (Path(src), None)
    if source_path is None:
        return "openEO job"
    return f"{source_path.name}: {function}" if function is not None else source_path.name
