import re

import openeo
from openeo.rest import JobFailedException, OpenEoClientException
import pytest

from openeo.rest.job import RESTJob
from .. import as_path

API_URL = "https://oeo.net"

TIFF_CONTENT = b'T1f7D6t6l0l' * 1000


@pytest.fixture
def session040(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "0.4.0"})
    session = openeo.connect(API_URL)
    return session


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    con = openeo.connect(API_URL)
    return con


def test_execute_batch(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results")
    requests_mock.get(API_URL + "/jobs/f00ba5", [
        {'json': {"status": "submitted"}},
        {'json': {"status": "queued"}},
        {'json': {"status": "running", "progress": 15}},
        {'json': {"status": "running", "progress": 80}},
        {'json': {"status": "finished", "progress": 100}},
    ])
    requests_mock.get(API_URL + "/jobs/f00ba5/results", json={
        "links": [{"href": API_URL + "/jobs/f00ba5/files/output.tiff"}]
    })
    requests_mock.get(API_URL + "/jobs/f00ba5/files/output.tiff", text="tiffdata")
    requests_mock.get(API_URL + "/jobs/f00ba5/logs", json={'logs': []})

    path = tmpdir.join("tmp.tiff")
    log = []

    def print(msg):
        log.append(msg)

    job = session040.load_collection("SENTINEL2").execute_batch(
        outputfile=as_path(path), out_format="GTIFF",
        max_poll_interval=.1, print=print
    )

    for log_entry in job.logs():
        print(log_entry.message)

    assert re.match(r"0:00:00(.0\d*)? Job 'f00ba5': submitted \(progress N/A\)", log[0])
    assert re.match(r"0:00:00.1\d* Job 'f00ba5': queued \(progress N/A\)", log[1])
    assert re.match(r"0:00:00.2\d* Job 'f00ba5': running \(progress 15%\)", log[2])
    assert re.match(r"0:00:00.3\d* Job 'f00ba5': running \(progress 80%\)", log[3])
    assert re.match(r"0:00:00.4\d* Job 'f00ba5': finished \(progress 100%\)", log[4])

    assert path.read() == "tiffdata"


def test_execute_batch_with_error(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results")
    requests_mock.get(API_URL + "/jobs/f00ba5", [
        {'json': {"status": "submitted"}},
        {'json': {"status": "queued"}},
        {'json': {"status": "running", "progress": 15}},
        {'json': {"status": "running", "progress": 80}},
        {'json': {"status": "error", "progress": 100}},
    ])
    requests_mock.get(API_URL + "/jobs/f00ba5/logs", json={
        'logs': [{
            'id': "123abc",
            'level': 'error',
            'message': "error processing batch job"
        }]
    })

    path = tmpdir.join("tmp.tiff")
    log = []

    def print(msg):
        log.append(msg)

    try:
        session040.load_collection("SENTINEL2").execute_batch(
            outputfile=as_path(path), out_format="GTIFF",
            max_poll_interval=.1, print=print
        )

        assert False
    except JobFailedException as e:
        log_entries = e.job.logs()

        assert log_entries[0].message == "error processing batch job"

    assert re.match(r"0:00:00(.0\d*)? Job 'f00ba5': submitted \(progress N/A\)", log[0])
    assert re.match(r"0:00:00.1\d* Job 'f00ba5': queued \(progress N/A\)", log[1])
    assert re.match(r"0:00:00.2\d* Job 'f00ba5': running \(progress 15%\)", log[2])
    assert re.match(r"0:00:00.3\d* Job 'f00ba5': running \(progress 80%\)", log[3])
    assert re.match(r"0:00:00.4\d* Job 'f00ba5': error \(progress 100%\)", log[4])


def test_get_job_logs(session040, requests_mock):
    requests_mock.get(API_URL + "/jobs/f00ba5/logs", json={
        'logs': [{
            'id': "123abc",
            'level': 'error',
            'message': "error processing batch job"
        }]
    })

    log_entries = session040.job('f00ba5').logs(offset="123abc")

    assert log_entries[0].message == "error processing batch job"


def test_create_job_100(con100, requests_mock):
    def check_request(request):
        assert request.json() == {
            "process": {"process_graph": {"foo1": {"process_id": "foo"}}},
            "title": "Foo", 'description': None,
            'budget': None, 'plan': None,
        }
        return True

    requests_mock.post(API_URL + "/jobs", headers={"OpenEO-Identifier": "f00ba5"}, additional_matcher=check_request)
    con100.create_job({"foo1": {"process_id": "foo"}}, title="Foo")


def test_download_result_040(session040, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"links": [
        {"href": API_URL + "/dl/jjr1.tiff"},
    ]})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=session040)
    target = as_path(tmp_path / "result.tiff")
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result(con100, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=con100)
    target = as_path(tmp_path / "result.tiff")
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_folder(con100, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=con100)
    target = as_path(tmp_path / "folder")
    target.mkdir()
    res = job.download_result(target)
    assert res == target / "1.tiff"
    assert list(p.name for p in target.iterdir()) == ["1.tiff"]
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_multiple(con100, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff"},
        "2.tiff": {"href": API_URL + "/dl/jjr2.tiff"},
    }})
    job = RESTJob("jj", connection=con100)
    with pytest.raises(OpenEoClientException, match="Expected one result file to download, but got 2"):
        job.download_result(tmp_path / "res.tiff")


def test_download_results_040(session040, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"links": [
        {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff"},
        {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff"},
    ]})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    requests_mock.get(API_URL + "/dl/jjr2.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=session040)
    target = as_path(tmp_path / "folder")
    target.mkdir()
    downloads = job.download_results(target)
    assert downloads == {
        target / "jjr1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff"},
        target / "jjr2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff"},
    }
    assert set(p.name for p in target.iterdir()) == {"jjr1.tiff", "jjr2.tiff"}
    with (target / "jjr1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT
    with (target / "jjr2.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_results(con100, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        "2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    requests_mock.get(API_URL + "/dl/jjr2.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=con100)
    target = as_path(tmp_path / "folder")
    target.mkdir()
    downloads = job.download_results(target)
    assert downloads == {
        target / "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        target / "2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff"}
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT
