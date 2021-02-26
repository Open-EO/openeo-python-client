import re
from pathlib import Path

import pytest

import openeo
from openeo.rest import JobFailedException, OpenEoClientException
from openeo.rest.job import RESTJob, ResultAsset
from .. import as_path

API_URL = "https://oeo.test"

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


@pytest.mark.slow
def test_execute_batch(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
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
    assert job.status() == "finished"

    assert re.match(r"0:00:00(.0\d*)? Job 'f00ba5': submitted \(progress N/A\)", log[0])
    assert re.match(r"0:00:00.1\d* Job 'f00ba5': queued \(progress N/A\)", log[1])
    assert re.match(r"0:00:00.2\d* Job 'f00ba5': running \(progress 15%\)", log[2])
    assert re.match(r"0:00:00.3\d* Job 'f00ba5': running \(progress 80%\)", log[3])
    assert re.match(r"0:00:00.4\d* Job 'f00ba5': finished \(progress 100%\)", log[4])

    assert path.read() == "tiffdata"
    assert job.logs() == []


@pytest.mark.slow
def test_execute_batch_with_error(session040, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
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
        pytest.fail("execute_batch should fail")
    except JobFailedException as e:
        assert e.job.status() == "error"
        assert [(l.level, l.message) for l in e.job.logs()] == [
            ("error", "error processing batch job"),
        ]

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
            "title": "Foo", "description": "just testing"
        }
        return True

    requests_mock.post(
        API_URL + "/jobs",
        status_code=201, headers={"OpenEO-Identifier": "f00ba5"}, additional_matcher=check_request
    )
    con100.create_job({"foo1": {"process_id": "foo"}}, title="Foo", description="just testing")


def test_download_result_040(session040, requests_mock, tmp_path):
    requests_mock.get(API_URL + "/jobs/jj/results", json={"links": [
        {"href": API_URL + "/dl/jjr1.tiff"},
    ]})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj", connection=session040)
    assert job.list_results() == {'links': [{'href': 'https://oeo.test/dl/jjr1.tiff'}]}
    target = as_path(tmp_path / "result.tiff")
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


@pytest.fixture
def job_with_1_asset(con100, requests_mock, tmp_path) -> RESTJob:
    requests_mock.get(API_URL + "/jobs/jj1/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj1", connection=con100)
    return job


@pytest.fixture
def job_with_2_assets(con100, requests_mock, tmp_path) -> RESTJob:
    requests_mock.get(API_URL + "/jobs/jj2/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        "2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    requests_mock.get(API_URL + "/dl/jjr2.tiff", content=TIFF_CONTENT)
    job = RESTJob("jj2", connection=con100)
    return job


def test_download_result(job_with_1_asset: RESTJob, tmp_path):
    job = job_with_1_asset
    target = as_path(tmp_path / "result.tiff")
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_download_file(job_with_1_asset: RESTJob, tmp_path):
    job = job_with_1_asset
    target = as_path(tmp_path / "result.tiff")
    res = job.get_results().download_file(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_folder(job_with_1_asset: RESTJob, tmp_path):
    job = job_with_1_asset
    target = as_path(tmp_path / "folder")
    target.mkdir()
    res = job.download_result(target)
    assert res == target / "1.tiff"
    assert list(p.name for p in target.iterdir()) == ["1.tiff"]
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_download_file_to_folder(job_with_1_asset: RESTJob, tmp_path):
    job = job_with_1_asset
    target = as_path(tmp_path / "folder")
    target.mkdir()
    res = job.get_results().download_file(target)
    assert res == target / "1.tiff"
    assert list(p.name for p in target.iterdir()) == ["1.tiff"]
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_multiple(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
    expected = re.escape("Can not use `download_file` with multiple assets. Use `download_files` instead")
    with pytest.raises(OpenEoClientException, match=expected):
        job.download_result(tmp_path / "res.tiff")


def test_get_results_multiple_download_single(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
    expected = re.escape("Can not use `download_file` with multiple assets. Use `download_files` instead")
    with pytest.raises(OpenEoClientException, match=expected):
        job.get_results().download_file(tmp_path / "res.tiff")


def test_get_results_multiple_download_single_by_name(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
    target = as_path(tmp_path / "res.tiff")
    path = job.get_results().download_file(target, name="1.tiff")
    assert path == target
    assert list(p.name for p in tmp_path.iterdir()) == ["res.tiff"]
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_multiple_download_single_by_wrong_name(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
    target = as_path(tmp_path / "res.tiff")
    expected = r"No asset 'foobar\.tiff' in: \['.\.tiff', '.\.tiff'\]"
    with pytest.raises(OpenEoClientException, match=expected):
        job.get_results().download_file(target, name="foobar.tiff")


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


def test_download_results(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
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


def test_list_results(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets
    assert job.list_results() == {'assets': {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'}
    }}


def test_get_results_download_files(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets

    target = as_path(tmp_path / "folder")
    target.mkdir()
    results = job.get_results()

    assets = job.get_results().get_assets()
    assert {a.name: a.metadata for a in assets} == {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'}
    }

    downloads = results.download_files(target)
    assert set(downloads) == {target / "1.tiff", target / "2.tiff"}
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff"}
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_download_files_new_folder(job_with_2_assets: RESTJob, tmp_path):
    job = job_with_2_assets

    target = as_path(tmp_path / "folder")
    results = job.get_results()
    assert not target.exists()
    downloads = results.download_files(target)
    assert target.exists()
    assert target.is_dir()
    assert set(downloads) == {target / "1.tiff", target / "2.tiff"}
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff"}
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_download_file(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = RESTJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={'type': 'image/tiff; application=geotiff'})
    target = as_path(tmp_path / "res.tiff")
    path = asset.download(target)

    assert isinstance(path, Path)
    assert path.name == "res.tiff"
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_download_folder(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = RESTJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
    target = as_path(tmp_path / "folder")
    target.mkdir()
    path = asset.download(target)

    assert isinstance(path, Path)
    assert path.name == "1.tiff"
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_load_json(con100, requests_mock):
    href = API_URL + "/dl/jjr1.json"
    requests_mock.get(href, json={"bands": [1, 2, 3]})

    job = RESTJob("jj", connection=con100)
    asset = ResultAsset(job, name="out.json", href=href, metadata={"type": "application/json"})
    res = asset.load_json()

    assert res == {"bands": [1, 2, 3]}


def test_result_asset_load_bytes(con100, requests_mock):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = RESTJob("jj", connection=con100)
    asset = ResultAsset(job, name="out.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
    res = asset.load_bytes()

    assert res == TIFF_CONTENT
