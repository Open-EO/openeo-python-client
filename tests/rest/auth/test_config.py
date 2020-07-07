import json

import pytest

from openeo.rest.auth.config import RefreshTokenStore


class TestRefreshTokenStorage:

    def test_start_empty(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        assert r.get("foo", "bar") is None

    def test_pass_dir(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set("foo", "bar", "imd6$3cr3t")
        assert (tmp_path / RefreshTokenStore.DEFAULT_FILENAME).exists()
        assert [p.name for p in tmp_path.iterdir()] == [RefreshTokenStore.DEFAULT_FILENAME]

    def test_pass_file(self, tmp_path):
        path = tmp_path / "my_tokens.secret"
        r = RefreshTokenStore(path=path)
        r.set("foo", "bar", "imd6$3cr3t")
        assert path.exists()
        assert [p.name for p in tmp_path.iterdir()] == ["my_tokens.secret"]

    def test_public_file(self, tmp_path):
        path = tmp_path / "refresh_tokens.json"
        with path.open("w") as f:
            json.dump({}, f)
        r = RefreshTokenStore(path=path)
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            r.get("foo", "bar")
        with pytest.raises(PermissionError, match="readable by others.*expected permissions: 600"):
            r.set("foo", "bar", "imd6$3cr3t")

    def test_permissions(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set("foo", "bar", "imd6$3cr3t")
        st_mode = (tmp_path / RefreshTokenStore.DEFAULT_FILENAME).stat().st_mode
        assert st_mode & 0o777 == 0o600

    def test_start_empty_exception_on_miss(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        with pytest.raises(RefreshTokenStore.NoRefreshToken):
            r.get("foo", "bar", allow_miss=False)

    def test_get_set(self, tmp_path):
        r = RefreshTokenStore(path=tmp_path)
        r.set("foo", "bar", "ih6zdaT0k3n")
        assert r.get("foo", "bar") == "ih6zdaT0k3n"
