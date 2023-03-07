import json
import logging
import re
from pathlib import Path
from unittest import mock

import pytest
import requests

import openeo
import openeo.rest.job
from openeo.rest import JobFailedException, OpenEoClientException
from openeo.rest.job import BatchJob, ResultAsset

API_URL = "https://oeo.test"

TIFF_CONTENT = b'T1f7D6t6l0l' * 1000


@pytest.fixture
def session040(requests_mock):
    # TODO #134 eliminate 0.4.0 support
    requests_mock.get(API_URL + "/", json={
        "api_version": "0.4.0",
        "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}]
    })
    session = openeo.connect(API_URL)
    return session


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(API_URL + "/", json={
        "api_version": "1.0.0",
        "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}]
    })
    con = openeo.connect(API_URL)
    return con


def fake_time(times=[1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610]):
    """Set up mock to fake time, so that successive `time.time()` calls return from given list"""
    # TODO: unify this in central time mocking utility?
    time_mock = mock.Mock()
    time_mock.time.side_effect = iter(times)
    return mock.patch.object(openeo.rest.job, "time", time_mock)


def test_fake_time():
    with fake_time():
        assert openeo.rest.job.time.time() == 1
        assert openeo.rest.job.time.time() == 2
        assert openeo.rest.job.time.time() == 3
        assert openeo.rest.job.time.time() == 5


def test_execute_batch(con100, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
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

    with fake_time():
        job = con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF",
            max_poll_interval=.1, print=log.append
        )
    assert job.status() == "finished"

    assert re.match(r"0:00:01 Job 'f00ba5': send 'start'", log[0])
    assert re.match(r"0:00:02 Job 'f00ba5': submitted \(progress N/A\)", log[1])
    assert re.match(r"0:00:04 Job 'f00ba5': queued \(progress N/A\)", log[2])
    assert re.match(r"0:00:07 Job 'f00ba5': running \(progress 15%\)", log[3])
    assert re.match(r"0:00:12 Job 'f00ba5': running \(progress 80%\)", log[4])
    assert re.match(r"0:00:20 Job 'f00ba5': finished \(progress 100%\)", log[5])

    assert path.read() == "tiffdata"
    assert job.logs() == []


def test_execute_batch_with_error(con100, requests_mock, tmpdir):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    requests_mock.get(
        API_URL + "/jobs/f00ba5",
        [
            {"json": {"status": "submitted"}},
            {"json": {"status": "queued"}},
            {"json": {"status": "running", "progress": 15}},
            {"json": {"status": "running", "progress": 80}},
            {"json": {"status": "error", "progress": 100}},
        ],
    )
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                {"id": "12", "level": "info", "message": "starting"},
                {"id": "34", "level": "error", "message": "nope"},
            ]
        },
    )

    path = tmpdir.join("tmp.tiff")
    log = []

    try:
        with fake_time():
            con100.load_collection("SENTINEL2").execute_batch(
                outputfile=path, out_format="GTIFF",
                max_poll_interval=.1, print=log.append
            )
        pytest.fail("execute_batch should fail")
    except JobFailedException as e:
        assert e.job.status() == "error"
        assert [(l.level, l.message) for l in e.job.logs()] == [
            ("info", "starting"),
            ("error", "nope"),
        ]

    assert log == [
        "0:00:01 Job 'f00ba5': send 'start'",
        "0:00:02 Job 'f00ba5': submitted (progress N/A)",
        "0:00:04 Job 'f00ba5': queued (progress N/A)",
        "0:00:07 Job 'f00ba5': running (progress 15%)",
        "0:00:12 Job 'f00ba5': running (progress 80%)",
        "0:00:20 Job 'f00ba5': error (progress 100%)",
        "Your batch job 'f00ba5' failed. Error logs:",
        [{"id": "34", "level": "error", "message": "nope"}],
        "Full logs can be inspected in an openEO (web) editor or with `connection.job('f00ba5').logs()`.",
    ]


@pytest.mark.parametrize(["error_response", "expected"], [
    (
            {"exc": requests.ConnectionError("time out")},
            "Connection error while polling job status: time out",
    ),
    (
            {"status_code": 503, "text": "service unavailable"},
            "Service availability error while polling job status: [503] unknown: service unavailable",
    ),
    (
            {
                "status_code": 503,
                "json": {"code": "OidcProviderUnavailable", "message": "OIDC Provider is unavailable"}
            },
            "Service availability error while polling job status: [503] OidcProviderUnavailable: OIDC Provider is unavailable",
    ),
    (
            {"status_code": 502, "text": "Bad Gateway"},
            "Service availability error while polling job status: [502] unknown: Bad Gateway",
    ),
])
def test_execute_batch_with_soft_errors(con100, requests_mock, tmpdir, error_response, expected):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    requests_mock.get(API_URL + "/jobs/f00ba5", [
        {'json': {"status": "queued"}},
        {'json': {"status": "running", "progress": 15}},
        error_response,
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

    with fake_time():
        job = con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF",
            max_poll_interval=.1, print=log.append
        )
    assert job.status() == "finished"

    assert log == [
        "0:00:01 Job 'f00ba5': send 'start'",
        "0:00:02 Job 'f00ba5': queued (progress N/A)",
        "0:00:04 Job 'f00ba5': running (progress 15%)",
        "0:00:07 Job 'f00ba5': " + expected,
        "0:00:12 Job 'f00ba5': running (progress 80%)",
        "0:00:20 Job 'f00ba5': finished (progress 100%)",
    ]

    assert path.read() == "tiffdata"
    assert job.logs() == []


@pytest.mark.parametrize(["error_response", "expected"], [
    (
            {"exc": requests.ConnectionError("time out")},
            "Connection error while polling job status: time out",
    ),
    (
            {"status_code": 503, "text": "service unavailable"},
            "Service availability error while polling job status: [503] unknown: service unavailable",
    ),
    (
            {
                "status_code": 503,
                "json": {"code": "OidcProviderUnavailable", "message": "OIDC Provider is unavailable"}
            },
            "Service availability error while polling job status: [503] OidcProviderUnavailable: OIDC Provider is unavailable",
    ),
    (
            {"status_code": 502, "text": "Bad Gateway"},
            "Service availability error while polling job status: [502] unknown: Bad Gateway",
    ),
])
def test_execute_batch_with_excessive_soft_errors(con100, requests_mock, tmpdir, error_response, expected):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    responses = [
        {'json': {"status": "queued"}},
        {'json': {"status": "running", "progress": 15}},
    ]
    responses.extend([error_response] * 20)
    requests_mock.get(API_URL + "/jobs/f00ba5", responses)

    path = tmpdir.join("tmp.tiff")
    log = []

    with fake_time(), pytest.raises(OpenEoClientException, match="Excessive soft errors"):
        con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF",
            max_poll_interval=.1, print=log.append
        )

    assert log[:7] == [
        "0:00:01 Job 'f00ba5': send 'start'",
        "0:00:02 Job 'f00ba5': queued (progress N/A)",
        "0:00:04 Job 'f00ba5': running (progress 15%)",
        "0:00:07 Job 'f00ba5': " + expected,
        "0:00:12 Job 'f00ba5': " + expected,
        "0:00:20 Job 'f00ba5': " + expected,
        "0:00:33 Job 'f00ba5': " + expected,
    ]


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


# TODO: do we keep testing on sesssion040, or should we switch to con100?
def test_get_job_logs_returns_debug_loglevel_by_default(session040, requests_mock):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                {
                    "id": "123abc",
                    "level": "error",
                    "message": "error processing batch job",
                },
                {
                    "id": "234abc",
                    "level": "debug",
                    "message": "Some debug info we want to filter out",
                },
                {
                    "id": "345abc",
                    "level": "info",
                    "message": "Some general info we want to filter out",
                },
                {
                    "id": "345abc",
                    "level": "warning",
                    "message": "Some warning we want to filter out",
                },
            ]
        },
    )

    log_entries = session040.job("f00ba5").logs()

    assert len(log_entries) == 4
    assert log_entries[0].level == "error"
    assert log_entries[1].level == "debug"
    assert log_entries[2].level == "info"
    assert log_entries[3].level == "warning"


@pytest.mark.parametrize(
    ["log_level", "exp_num_messages"],
    [
        (None, 4),  # Default is DEBUG / show all log levels.
        (logging.ERROR, 1),
        ("error", 1),
        ("ERROR", 1),
        (logging.WARNING, 2),
        ("warning", 2),
        ("WARNING", 2),
        (logging.INFO, 3),
        ("INFO", 3),
        ("info", 3),
        (logging.DEBUG, 4),
        ("DEBUG", 4),
        ("debug", 4),
    ],
)
def test_get_job_logs_keeps_loglevel_that_is_higher_or_equal(
    session040, requests_mock, log_level, exp_num_messages
):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                {
                    "id": "123abc",
                    "level": "error",
                    "message": "error processing batch job",
                },
                {
                    "id": "234abc",
                    "level": "debug",
                    "message": "Some debug info we want to filter out",
                },
                {
                    "id": "345abc",
                    "level": "info",
                    "message": "Some general info we want to filter out",
                },
                {
                    "id": "345abc",
                    "level": "warning",
                    "message": "Some warning we want to filter out",
                },
            ]
        },
    )

    log_entries = session040.job("f00ba5").logs(log_level=log_level)
    assert len(log_entries) == exp_num_messages


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
    job = BatchJob("jj", connection=session040)
    assert job.list_results() == {'links': [{'href': 'https://oeo.test/dl/jjr1.tiff'}]}
    target = tmp_path / "result.tiff"
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


@pytest.fixture
def job_with_1_asset(con100, requests_mock, tmp_path) -> BatchJob:
    requests_mock.get(API_URL + "/jobs/jj1/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    job = BatchJob("jj1", connection=con100)
    return job


@pytest.fixture
def job_with_2_assets(con100, requests_mock, tmp_path) -> BatchJob:
    requests_mock.get(API_URL + "/jobs/jj2/results", json={
        # This is a STAC Item
        "type": "Feature",
        "assets": {
            "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
            "2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
        }
    })
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    requests_mock.get(API_URL + "/dl/jjr2.tiff", content=TIFF_CONTENT)
    job = BatchJob("jj2", connection=con100)
    return job


def test_download_result(job_with_1_asset: BatchJob, tmp_path):
    job = job_with_1_asset
    target = tmp_path / "result.tiff"
    res = job.download_result(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_download_file(job_with_1_asset: BatchJob, tmp_path):
    job = job_with_1_asset
    target = tmp_path / "result.tiff"
    res = job.get_results().download_file(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_folder(job_with_1_asset: BatchJob, tmp_path):
    job = job_with_1_asset
    target = tmp_path / "folder"
    target.mkdir()
    res = job.download_result(target)
    assert res == target / "1.tiff"
    assert list(p.name for p in target.iterdir()) == ["1.tiff"]
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_download_file_to_folder(job_with_1_asset: BatchJob, tmp_path):
    job = job_with_1_asset
    target = tmp_path / "folder"
    target.mkdir()
    res = job.get_results().download_file(target)
    assert res == target / "1.tiff"
    assert list(p.name for p in target.iterdir()) == ["1.tiff"]
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_download_result_multiple(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    expected = re.escape("Can not use `download_file` with multiple assets. Use `download_files` instead")
    with pytest.raises(OpenEoClientException, match=expected):
        job.download_result(tmp_path / "res.tiff")


def test_get_results_multiple_download_single(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    expected = re.escape("Can not use `download_file` with multiple assets. Use `download_files` instead")
    with pytest.raises(OpenEoClientException, match=expected):
        job.get_results().download_file(tmp_path / "res.tiff")


def test_get_results_multiple_download_single_by_name(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    target = tmp_path / "res.tiff"
    path = job.get_results().download_file(target, name="1.tiff")
    assert path == target
    assert list(p.name for p in tmp_path.iterdir()) == ["res.tiff"]
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_multiple_download_single_by_wrong_name(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    target = tmp_path / "res.tiff"
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
    job = BatchJob("jj", connection=session040)
    target = tmp_path / "folder"
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


def test_download_results(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    target = tmp_path / "folder"
    target.mkdir()
    downloads = job.download_results(target)
    assert downloads == {
        target / "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        target / "2.tiff": {"href": API_URL + "/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff"}
    with (target / "1.tiff").open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_list_results(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    assert job.list_results() == {"type": "Feature", 'assets': {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'}
    }}


def test_get_results_download_files(job_with_2_assets: BatchJob, tmp_path):
    target = tmp_path / "folder"
    target.mkdir()

    job = job_with_2_assets
    results = job.get_results()

    assets = job.get_results().get_assets()
    assert {a.name: a.metadata for a in assets} == {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'},
    }

    downloads = results.download_files(target)
    assert set(downloads) == {target / "1.tiff", target / "2.tiff", target / "job-results.json"}
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff", "job-results.json"}
    assert (target / "1.tiff").read_bytes() == TIFF_CONTENT
    job_results_metadata = json.loads((target / "job-results.json").read_text())
    assert job_results_metadata["type"] == "Feature"
    assert job_results_metadata["assets"] == {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'},
    }


def test_get_results_download_files_new_folder(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    results = job.get_results()
    target = tmp_path / "folder"
    assert not target.exists()
    downloads = results.download_files(target)
    assert target.exists()
    assert target.is_dir()
    assert set(downloads) == {target / "1.tiff", target / "2.tiff", target / "job-results.json"}
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff", "job-results.json"}
    assert (target / "1.tiff").read_bytes() == TIFF_CONTENT
    job_results_metadata = json.loads((target / "job-results.json").read_text())
    assert job_results_metadata["type"] == "Feature"
    assert job_results_metadata["assets"] == {
        '1.tiff': {'href': 'https://oeo.test/dl/jjr1.tiff', 'type': 'image/tiff; application=geotiff'},
        '2.tiff': {'href': 'https://oeo.test/dl/jjr2.tiff', 'type': 'image/tiff; application=geotiff'},
    }


@pytest.mark.parametrize(["include_stac_metadata", "expected"], [
    (True, {"1.tiff", "2.tiff", "job-results.json"}),
    (False, {"1.tiff", "2.tiff"})
])
def test_get_results_download_files_include_stac_metadata(
        job_with_2_assets: BatchJob, tmp_path, include_stac_metadata, expected
):
    results = job_with_2_assets.get_results()
    target = tmp_path / "folder"
    downloads = results.download_files(target, include_stac_metadata=include_stac_metadata)
    assert target.is_dir()
    assert set(downloads) == set(target / p for p in expected)
    assert set(p.name for p in target.iterdir()) == expected


def test_result_asset_download_file(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={'type': 'image/tiff; application=geotiff'})
    target = tmp_path / "res.tiff"
    path = asset.download(target)

    assert isinstance(path, Path)
    assert path.name == "res.tiff"
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_download_folder(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
    target = tmp_path / "folder"
    target.mkdir()
    path = asset.download(target)

    assert isinstance(path, Path)
    assert path.name == "1.tiff"
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_load_json(con100, requests_mock):
    href = API_URL + "/dl/jjr1.json"
    requests_mock.get(href, json={"bands": [1, 2, 3]})

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="out.json", href=href, metadata={"type": "application/json"})
    res = asset.load_json()

    assert res == {"bands": [1, 2, 3]}


def test_result_asset_load_bytes(con100, requests_mock):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="out.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
    res = asset.load_bytes()

    assert res == TIFF_CONTENT


def test_get_results_download_file_other_domain(con100, requests_mock, tmp_path):
    """https://github.com/Open-EO/openeo-python-client/issues/201"""
    secret = "!secret token!"
    requests_mock.get(API_URL + '/credentials/basic', json={"access_token": secret})

    def get_results(request, context):
        assert "auth" in repr(request.headers).lower()
        assert secret in repr(request.headers)
        return {"assets": {
            "1.tiff": {"href": "https://evilcorp.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        }}

    def download_tiff(request, context):
        assert "auth" not in repr(request.headers).lower()
        assert secret not in repr(request.headers)
        return TIFF_CONTENT

    requests_mock.get(API_URL + "/jobs/jj1/results", json=get_results)
    requests_mock.get("https://evilcorp.test/dl/jjr1.tiff", content=download_tiff)

    con100.authenticate_basic("john", "j0hn")
    job = BatchJob("jj1", connection=con100)
    target = tmp_path / "result.tiff"
    res = job.get_results().download_file(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT
