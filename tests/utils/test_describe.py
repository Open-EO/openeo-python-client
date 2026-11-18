import pathlib
import shutil
import subprocess
import tokenize
import unittest.mock as mock
from types import SimpleNamespace

import pytest

from openeo.utils.describe import (
    DEFAULT_SOURCE_MAX_LENGTH,
    GitInfo,
    auto_description,
    auto_title,
    get_git_info,
    get_source_code,
    get_source_path,
)


def _fake_frame(filename: str) -> SimpleNamespace:
    """Fake stack frame with just the code attributes the helpers inspect."""
    return SimpleNamespace(f_code=SimpleNamespace(co_filename=filename, co_name="<module>"))


def _fake_check_output(*, toplevel, commit, status="", tracked=True):
    """Fake `git` subprocess output for the commands used by the Git helpers."""

    def check_output(command, **kwargs):
        if "--show-toplevel" in command:
            return f"{toplevel}\n"
        if "--verify" in command:
            return f"{commit}\n"
        if "status" in command:
            return status
        if "ls-files" in command:
            if not tracked:
                raise subprocess.CalledProcessError(1, command)
            return ""
        raise AssertionError(f"Unexpected Git command: {command}")

    return check_output


requires_git = pytest.mark.skipif(shutil.which("git") is None, reason="git executable is required")


def _run_git(*args, cwd) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


@pytest.fixture
def git_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _run_git("init", cwd=repo)
    _run_git("config", "user.name", "Test", cwd=repo)
    _run_git("config", "user.email", "test@example.com", cwd=repo)
    script = repo / "analysis.py"
    script.write_text("print(42)\n", encoding="utf-8")
    _run_git("add", "analysis.py", cwd=repo)
    _run_git("commit", "-m", "Add analysis script", cwd=repo)
    return repo, script


class TestGetSourcePath:
    @pytest.mark.parametrize("path_factory", [str, pathlib.Path])
    def test_explicit_source(self, tmp_path, path_factory):
        src = tmp_path / "analysis.py"
        assert get_source_path(path_factory(src)) == src

    @pytest.mark.parametrize("filename", ["<stdin>", "<ipython-input-1>", "missing.py"])
    def test_undetectable_source(self, tmp_path, filename):
        if filename == "missing.py":
            filename = str(tmp_path / filename)
        with mock.patch("openeo.utils.describe._caller_frame", return_value=_fake_frame(filename)):
            assert get_source_path() is None


class TestAutoDescription:
    @pytest.mark.parametrize("path_factory", [str, pathlib.Path])
    @pytest.mark.parametrize("include_source", [False, True])
    def test_explicit_source(self, tmp_path, path_factory, include_source):
        src = tmp_path / "analysis.py"
        src.write_text("print(42)\n", encoding="utf-8")
        with mock.patch("openeo.utils.describe.tokenize.open", wraps=tokenize.open) as read_source:
            description = auto_description(path_factory(src), include_source=include_source)
        assert description.startswith("Source: analysis.py")
        assert str(tmp_path) not in description
        assert read_source.called == include_source

    def test_explicit_source_omits_function(self, tmp_path):
        with mock.patch("openeo.utils.describe._detect_caller") as detect_caller:
            assert auto_description(tmp_path / "analysis.py") == "Source: analysis.py"
            assert auto_title(tmp_path / "analysis.py") == "analysis.py"
        detect_caller.assert_not_called()

    def test_caller(self, pytester):
        pytester.makepyfile(myscript="""
            from openeo.utils.describe import auto_description, auto_title

            def run_analysis():
                print(auto_description())
                print("---")
                print(auto_title())

            run_analysis()
            """)
        result = pytester.runpython("myscript.py")
        assert result.ret == 0
        assert result.stdout.lines == [
            "Source: myscript.py",
            "",
            "Function: run_analysis",
            "---",
            "myscript.py: run_analysis",
        ]

    def test_caller_include_source(self, pytester):
        pytester.makepyfile(myscript="""
            from openeo.utils.describe import auto_description

            def run_analysis():
                print(auto_description(include_source=True))

            run_analysis()
            """)
        result = pytester.runpython("myscript.py")
        assert result.ret == 0
        stdout = result.stdout.str()
        assert "Source: myscript.py" in stdout
        assert "Function: run_analysis" in stdout
        assert "Python source:" in stdout
        assert "def run_analysis():" in stdout

    def test_unavailable_source(self):
        with mock.patch("openeo.utils.describe._detect_caller", return_value=(None, None)):
            assert auto_description() == "Source: unavailable"
            assert auto_title() == "openEO job"
            with pytest.raises(ValueError, match="no source file is available"):
                auto_description(include_source=True)

    def test_function_without_source(self):
        with mock.patch("openeo.utils.describe._detect_caller", return_value=(None, "run_analysis")):
            assert auto_description() == "Source: unavailable\n\nFunction: run_analysis"

    def test_module_level_caller(self, pytester):
        pytester.makepyfile(myscript="""
            from openeo.utils.describe import auto_title

            print(auto_title())
            """)
        result = pytester.runpython("myscript.py")
        assert result.ret == 0
        assert result.stdout.lines == ["myscript.py"]

    @pytest.mark.parametrize(
        "source", ["", "print('café')", "# ```python\n# ~~~~\nprint('café')\n", 'message = """\n```\n~~~~\n"""\n']
    )
    def test_include_source(self, tmp_path, source):
        src = tmp_path / "analysis.py"
        src.write_text(source, encoding="utf-8")
        indented = "".join("    " + line for line in source.splitlines(keepends=True))
        assert auto_description(src, include_source=True) == "Source: analysis.py\n\nPython source:\n\n" + indented

    def test_source_encoding(self, tmp_path):
        src = tmp_path / "analysis.py"
        source = "# coding: latin-1\nprint('café')\n"
        src.write_bytes(source.encode("latin-1"))
        assert "    print('café')\n" in auto_description(src, include_source=True)

    def test_missing_source(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            auto_description(tmp_path / "missing.py", include_source=True)

    def test_unreadable_source(self, tmp_path):
        with mock.patch("openeo.utils.describe.tokenize.open", side_effect=PermissionError("cannot read source")):
            with pytest.raises(PermissionError, match="cannot read source"):
                auto_description(tmp_path / "analysis.py", include_source=True)

    @pytest.mark.parametrize("src", [123, [], {}])
    def test_invalid_source(self, src):
        with pytest.raises(TypeError):
            auto_description(src)


class TestSourceTruncation:
    @pytest.mark.parametrize(
        "source, max_length, expected",
        [
            ("print(1)\n", DEFAULT_SOURCE_MAX_LENGTH, "print(1)\n"),
            ("x" * 10, 10, "x" * 10),
            ("x" * 1001, None, "x" * 1001),
            ("x" * 1001, 1000, "x" * 1000 + "\n... (truncated: 1000 of 1001 characters)"),
        ],
    )
    def test_get_source_code(self, tmp_path, source, max_length, expected):
        src = tmp_path / "analysis.py"
        src.write_text(source, encoding="utf-8")
        assert get_source_code(src, max_length=max_length) == expected

    def test_default_limit(self, tmp_path):
        src = tmp_path / "analysis.py"
        src.write_text("# " + "x" * 2000, encoding="utf-8")
        description = auto_description(src, include_source=True)
        assert "truncated: 1000 of 2002 characters" in description

    def test_multibyte_source(self, tmp_path):
        src = tmp_path / "analysis.py"
        src.write_text("é" * 1200, encoding="utf-8")
        description = auto_description(src, include_source=True)
        assert "    " + "é" * 1000 in description
        assert "truncated: 1000 of 1200 characters" in description

    def test_disabled(self, tmp_path):
        src = tmp_path / "analysis.py"
        src.write_text("x" * 1200, encoding="utf-8")
        description = auto_description(src, include_source=True, source_max_length=None)
        assert "truncated" not in description
        assert "    " + "x" * 1200 in description

    @pytest.mark.parametrize("max_length", [-1, -1000])
    def test_negative_limit(self, tmp_path, max_length):
        with pytest.raises(ValueError, match="non-negative"):
            get_source_code(tmp_path / "analysis.py", max_length=max_length)
        with pytest.raises(ValueError, match="non-negative"):
            auto_description(tmp_path / "analysis.py", include_source=True, source_max_length=max_length)

    def test_get_source_code_without_source(self):
        with mock.patch("openeo.utils.describe._caller_frame", return_value=None):
            with pytest.raises(ValueError, match="no source file is available"):
                get_source_code()


class TestGit:
    @pytest.mark.parametrize("include_git_commit", [False, True])
    def test_lookup_only_when_requested(self, tmp_path, include_git_commit):
        src = tmp_path / "analysis.py"
        fake_git = _fake_check_output(toplevel=tmp_path, commit="a" * 40)
        with mock.patch("openeo.utils.describe.subprocess.check_output", side_effect=fake_git) as check_output:
            description = auto_description(src, include_git_commit=include_git_commit)
        assert check_output.called == include_git_commit
        assert description.startswith("Source: analysis.py")
        if include_git_commit:
            assert f"Git commit: {'a' * 40}" in description

    def test_uncommitted_changes_and_untracked_source(self, tmp_path):
        src = tmp_path / "analysis.py"
        fake_git = _fake_check_output(toplevel=tmp_path, commit="a" * 40, status=" M analysis.py\n", tracked=False)
        with mock.patch("openeo.utils.describe.subprocess.check_output", side_effect=fake_git):
            info = get_git_info(src)
            description = auto_description(src, include_git_commit=True)
        assert info == GitInfo(commit="a" * 40, dirty=True, file_tracked=False)
        assert "Git working tree: uncommitted changes" in description
        assert "Git source file: untracked" in description

    @pytest.mark.parametrize("ref", ["", "-x", "--help"])
    def test_invalid_git_ref(self, tmp_path, ref):
        with pytest.raises(ValueError, match="Invalid Git reference"):
            get_git_info(tmp_path / "analysis.py", ref=ref)

    @pytest.mark.parametrize(
        "error",
        [
            FileNotFoundError("git is not installed"),
            PermissionError("git is not executable"),
            subprocess.CalledProcessError(128, ["git"], stderr="sensitive subprocess output"),
            subprocess.TimeoutExpired(["git"], timeout=5, output="sensitive subprocess output"),
        ],
    )
    def test_git_failure(self, tmp_path, caplog, error):
        with mock.patch("openeo.utils.describe.subprocess.check_output", side_effect=error):
            assert auto_description(tmp_path / "analysis.py", include_git_commit=True) == "Source: analysis.py"
        assert "Could not attach Git information" in caplog.text
        assert type(error).__name__ in caplog.text
        assert "sensitive subprocess output" not in caplog.text

    def test_git_without_source(self, caplog):
        with mock.patch("openeo.utils.describe._caller_frame", return_value=None):
            with mock.patch("openeo.utils.describe.subprocess.check_output") as check_output:
                assert auto_description(include_git_commit=True) == "Source: unavailable"
        check_output.assert_not_called()
        assert "Could not attach Git information: no source file is available." in caplog.text

    @requires_git
    def test_clean_repository(self, git_repo):
        repo, script = git_repo
        commit = _run_git("rev-parse", "HEAD", cwd=repo).stdout.strip()
        assert get_git_info(script) == GitInfo(commit=commit, dirty=False, file_tracked=True)
        assert auto_description(script, include_git_commit=True) == f"Source: analysis.py\n\nGit commit: {commit}"

    @requires_git
    def test_dirty_repository(self, git_repo):
        _, script = git_repo
        script.write_text("print(43)\n", encoding="utf-8")
        info = get_git_info(script)
        assert info is not None
        assert info.dirty is True
        assert info.file_tracked is True
        description = auto_description(script, include_git_commit=True)
        assert "Git working tree: uncommitted changes" in description
        assert "Git source file: untracked" not in description

    @requires_git
    def test_untracked_source(self, git_repo):
        repo, _ = git_repo
        untracked = repo / "new_script.py"
        untracked.write_text("print(1)\n", encoding="utf-8")
        info = get_git_info(untracked)
        assert info is not None
        assert info.dirty is True
        assert info.file_tracked is False
        assert "Git source file: untracked" in auto_description(untracked, include_git_commit=True)

    @requires_git
    def test_explicit_ref(self, git_repo):
        repo, script = git_repo
        first_commit = _run_git("rev-parse", "HEAD", cwd=repo).stdout.strip()
        script.write_text("print(43)\n", encoding="utf-8")
        _run_git("add", "analysis.py", cwd=repo)
        _run_git("commit", "-m", "Update analysis script", cwd=repo)
        second_commit = _run_git("rev-parse", "HEAD", cwd=repo).stdout.strip()

        assert get_git_info(script, ref=first_commit).commit == first_commit
        assert get_git_info(script, ref=second_commit).commit == second_commit
        assert get_git_info(script).commit == second_commit

    @requires_git
    def test_not_a_git_repository(self, tmp_path, caplog):
        script = tmp_path / "analysis.py"
        script.write_text("print(1)\n", encoding="utf-8")
        assert get_git_info(script) is None
        assert "Could not attach Git information" in caplog.text
