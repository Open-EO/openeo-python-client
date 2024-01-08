import re
from pathlib import PurePosixPath

import pytest

import openeo
from openeo.rest import OpenEoApiError, OpenEoApiPlainError
from openeo.rest.userfile import UserFile

API_URL = "https://oeo.test"


@pytest.fixture
def con100(requests_mock) -> openeo.Connection:
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    con = openeo.connect(API_URL)
    return con


class TestUserFile:
    def test_simple(self, con100):
        f = UserFile("foo.txt", connection=con100)
        assert f.path == PurePosixPath("foo.txt")
        assert f.metadata == {"path": "foo.txt"}

    def test_from_metadata(self, con100):
        f = UserFile.from_metadata(metadata={"path": "foo.txt"}, connection=con100)
        assert f.path == PurePosixPath("foo.txt")
        assert f.metadata == {"path": "foo.txt"}

    def test_connection_get_file(self, con100):
        f = con100.get_file("foo.txt")
        assert f.path == PurePosixPath("foo.txt")
        assert f.metadata == {"path": "foo.txt"}

    def test_connection_list_files(self, con100, requests_mock):
        requests_mock.get(
            API_URL + "/files",
            json={
                "files": [
                    {
                        "path": "foo.txt",
                        "size": 182,
                        "modified": "2015-10-20T17:22:10Z",
                    },
                    {
                        "path": "data/foo.tiff",
                        "size": 183142,
                        "modified": "2017-01-01T09:36:18Z",
                    },
                ],
            },
        )
        files = con100.list_files()
        assert len(files)
        assert files[0].path == PurePosixPath("foo.txt")
        assert files[0].metadata == {
            "path": "foo.txt",
            "size": 182,
            "modified": "2015-10-20T17:22:10Z",
        }
        assert files[1].path == PurePosixPath("data/foo.tiff")
        assert files[1].metadata == {
            "path": "data/foo.tiff",
            "size": 183142,
            "modified": "2017-01-01T09:36:18Z",
        }

    def test_upload(self, con100, tmp_path, requests_mock):
        source = tmp_path / "to-upload.txt"
        source.write_bytes(b"hello world\n")
        f = UserFile("foo.txt", connection=con100)

        def put_files(request, context):
            import io

            if isinstance(request.text, io.BufferedReader):
                body = request.text.read()
            else:
                body = request.text
            assert body == b"hello world\n"
            context.status_code = 200
            return {
                "path": "foo.txt",
                "size": len(body),
                "modified": "2018-01-03T10:55:29Z",
            }

        upload_mock = requests_mock.put(API_URL + "/files/foo.txt", json=put_files)

        f2 = f.upload(source)
        assert f2.path == PurePosixPath("foo.txt")
        assert f2.metadata == {
            "path": "foo.txt",
            "size": 12,
            "modified": "2018-01-03T10:55:29Z",
        }

        assert upload_mock.call_count == 1

    @pytest.mark.parametrize("status_code", [404, 500])
    def test_upload_fail(self, tmp_path, con100, requests_mock, status_code):
        source = tmp_path / "to-upload.txt"
        source.write_text("hello world\n")
        f = UserFile("foo.txt", connection=con100)

        upload_mock = requests_mock.put(API_URL + "/files/foo.txt", status_code=status_code)

        with pytest.raises(OpenEoApiPlainError, match=rf"\[{status_code}\]"):
            f.upload(source)

        assert upload_mock.call_count == 1

    @pytest.mark.parametrize(
        ["target", "expected_target"],
        [
            (None, "to-upload.txt"),
            ("foo.txt", "foo.txt"),
            ("data/foo.txt", "data/foo.txt"),
        ],
    )
    def test_connection_upload(self, con100, tmp_path, requests_mock, target, expected_target):
        source = tmp_path / "to-upload.txt"
        source.write_bytes(b"hello world\n")

        def put_files(request, context):
            import io

            if isinstance(request.text, io.BufferedReader):
                body = request.text.read()
            else:
                body = request.text
            assert body == b"hello world\n"
            context.status_code = 200
            return {
                "path": re.match("/files/(.*)", request.path).group(1),
                "size": len(body),
                "modified": "2018-01-03T10:55:29Z",
            }

        upload_mock = requests_mock.put(API_URL + f"/files/{expected_target}", json=put_files)

        f = con100.upload_file(source, target=target)
        assert upload_mock.call_count == 1

        assert f.path == PurePosixPath(expected_target)
        assert f.metadata == {
            "path": expected_target,
            "size": 12,
            "modified": "2018-01-03T10:55:29Z",
        }

    @pytest.mark.parametrize("status_code", [404, 500])
    def test_connection_upload_fail(self, con100, tmp_path, requests_mock, status_code):
        source = tmp_path / "foo.txt"
        source.write_text("hello world\n")

        upload_mock = requests_mock.put(API_URL + "/files/foo.txt", status_code=status_code)

        with pytest.raises(OpenEoApiPlainError, match=rf"\[{status_code}\]"):
            con100.upload_file(source=source)

        assert upload_mock.call_count == 1

    @pytest.mark.parametrize(
        ["target", "expected"],
        [
            (None, "foo.txt"),
            ("data", "data/foo.txt"),
            ("data/bar.txt", "data/bar.txt"),
        ],
    )
    def test_download(self, con100, tmp_path, requests_mock, target, expected, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)

        download_mock = requests_mock.get(API_URL + "/files/foo.txt", content=b"hello world\n")

        if target is not None:
            target = tmp_path / target

        f = UserFile("foo.txt", connection=con100)
        f.download(target)

        expected = tmp_path / expected
        assert expected.exists()
        assert expected.read_bytes() == b"hello world\n"
        assert download_mock.call_count == 1
