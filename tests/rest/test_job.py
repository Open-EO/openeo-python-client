import contextlib
import itertools
import json
import logging
import re
from pathlib import Path
from typing import Optional
from unittest import mock

import httpretty
import pytest
import requests

import openeo
import openeo.rest.job
from openeo.rest import JobFailedException, OpenEoApiPlainError, OpenEoClientException
from openeo.rest.job import BatchJob, ResultAsset
from openeo.rest.models.general import Link
from openeo.rest.models.logs import LogEntry
from openeo.utils.http import (
    HTTP_402_PAYMENT_REQUIRED,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
)

API_URL = "https://oeo.test"

TIFF_CONTENT = b'T1f7D6t6l0l' * 10000

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
    requests_mock.get(
        API_URL + "/jobs/f00ba5",
        [
            {"json": {"status": "submitted"}},
            {"json": {"status": "queued"}},
            {"json": {"status": "running", "progress": 15.51}},
            {"json": {"status": "running", "progress": 80}},
            {"json": {"status": "finished", "progress": 100}},
        ],
    )
    requests_mock.get(
        API_URL + "/jobs/f00ba5/results",
        json={
            "assets": {
                "output.tiff": {
                    "href": API_URL + "/jobs/f00ba5/files/output.tiff",
                    "type": "image/tiff; application=geotiff",
                },
            }
        },
    )
    requests_mock.head(API_URL + "/jobs/f00ba5/files/output.tiff", headers={"Content-Length": str(len("tiffdata"))})
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
    assert re.match(r"0:00:07 Job 'f00ba5': running \(progress 15.5%\)", log[3])
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


@pytest.mark.parametrize("show_error_logs", [True, False])
def test_execute_batch_show_error_logs(con100, requests_mock, show_error_logs):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    requests_mock.get(API_URL + "/jobs/f00ba5", json={"status": "error", "progress": 100})
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={"logs": [{"id": "34", "level": "error", "message": "nope"}]},
    )

    stdout = []
    with fake_time(), pytest.raises(JobFailedException):
        con100.load_collection("SENTINEL2").execute_batch(
            max_poll_interval=0.1, print=stdout.append, show_error_logs=show_error_logs
        )

    expected = [
        "0:00:01 Job 'f00ba5': send 'start'",
        "0:00:02 Job 'f00ba5': error (progress 100%)",
    ]
    if show_error_logs:
        expected += [
            "Your batch job 'f00ba5' failed. Error logs:",
            [{"id": "34", "level": "error", "message": "nope"}],
            "Full logs can be inspected in an openEO (web) editor or with `connection.job('f00ba5').logs()`.",
        ]
    assert stdout == expected


@pytest.mark.parametrize(["error_response", "expected"], [
    (
            {"exc": requests.ConnectionError("time out")},
            "Connection error while polling job status: time out",
    ),
    (
            {"status_code": 503, "text": "service unavailable"},
            "Service availability error while polling job status: [503] service unavailable",
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
            "Service availability error while polling job status: [502] Bad Gateway",
    ),
])
def test_execute_batch_with_soft_errors(con100, requests_mock, tmpdir, error_response, expected):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    requests_mock.get(
        API_URL + "/jobs/f00ba5",
        [
            {"json": {"status": "queued"}},
            {"json": {"status": "running", "progress": 15}},
            error_response,
            {"json": {"status": "running", "progress": 80}},
            {"json": {"status": "finished", "progress": 100}},
        ],
    )
    requests_mock.get(
        API_URL + "/jobs/f00ba5/results",
        json={
            "assets": {
                "output.tiff": {
                    "href": API_URL + "/jobs/f00ba5/files/output.tiff",
                    "type": "image/tiff; application=geotiff",
                },
            }
        },
    )
    requests_mock.head(API_URL + "/jobs/f00ba5/files/output.tiff", headers={"Content-Length": str(len("tiffdata"))})
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
            "Service availability error while polling job status: [503] service unavailable",
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
            "Service availability error while polling job status: [502] Bad Gateway",
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


@pytest.mark.parametrize(
    ["require_success", "expectation"],
    [
        (True, pytest.raises(JobFailedException, match="'job-000' didn't finish successfully")),
        (False, contextlib.nullcontext()),
    ],
)
def test_start_and_wait_with_error_require_success(dummy_backend, require_success, expectation):
    dummy_backend.setup_simple_job_status_flow(queued=0, running=1, final="error")
    cube = dummy_backend.connection.load_collection("S2").save_result(format="GTiff")
    job = cube.create_job()
    assert job.status() == "created"
    with expectation, fake_time():
        job.start_and_wait(require_success=require_success)
    assert job.status() == "error"


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize(
    ["retry_config", "extra_responses", "expectation_context", "expected_sleeps"],
    [
        (  # Default retry settings
            None,
            [],
            contextlib.nullcontext(),
            [23, 34],
        ),
        (  # Default config with a generic 500 error
            None,
            [httpretty.Response(status=HTTP_500_INTERNAL_SERVER_ERROR, body="Internal Server Error")],
            pytest.raises(OpenEoApiPlainError, match=re.escape("[500] Internal Server Error")),
            [23],
        ),
        (  # Only retry by default on 429, but still handle a 503 error with soft error skipping feature of execute_batch poll loop
            {"status_forcelist": [HTTP_429_TOO_MANY_REQUESTS]},
            [httpretty.Response(status=HTTP_503_SERVICE_UNAVAILABLE, body="Service Unavailable")],
            contextlib.nullcontext(),
            [23, 12.34, 34],
        ),
        (
            # Explicit status_forcelist with custom status code to retry
            {"status_forcelist": [HTTP_429_TOO_MANY_REQUESTS, HTTP_402_PAYMENT_REQUIRED]},
            [httpretty.Response(status=HTTP_402_PAYMENT_REQUIRED, body="Payment Required")],
            contextlib.nullcontext(),
            [23, 34],
        ),
        (
            # No retry setup: also fail on 429
            False,
            [],
            pytest.raises(OpenEoApiPlainError, match=re.escape("[429] Too Many Requests")),
            [],
        ),
    ],
)
def test_execute_batch_retry_after_429_too_many_requests(
    tmpdir, retry_config, extra_responses, expectation_context, expected_sleeps
):
    httpretty.register_uri(
        httpretty.GET,
        uri=API_URL + "/",
        body=json.dumps({"api_version": "1.0.0", "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}]}),
    )
    httpretty.register_uri(
        httpretty.GET,
        uri=API_URL + "/file_formats",
        body=json.dumps({"output": {"GTiff": {"gis_data_types": ["raster"]}}}),
    )
    httpretty.register_uri(
        httpretty.GET,
        uri=API_URL + "/collections/SENTINEL2",
        body=json.dumps({"foo": "bar"}),
    )
    httpretty.register_uri(
        httpretty.POST, uri=API_URL + "/jobs", status=201, adding_headers={"OpenEO-Identifier": "f00ba5"}, body=""
    )
    httpretty.register_uri(httpretty.POST, uri=API_URL + "/jobs/f00ba5/results", status=202)
    httpretty.register_uri(
        httpretty.GET,
        uri=API_URL + "/jobs/f00ba5",
        responses=[
            httpretty.Response(body=json.dumps({"status": "queued"})),
            httpretty.Response(status=429, body="Too Many Requests", adding_headers={"Retry-After": "23"}),
            httpretty.Response(body=json.dumps({"status": "running", "progress": 80})),
        ]
        + extra_responses
        + [
            httpretty.Response(body=json.dumps({"status": "running", "progress": 80})),
            httpretty.Response(status=429, body="Too Many Requests", adding_headers={"Retry-After": "34"}),
            httpretty.Response(body=json.dumps({"status": "finished", "progress": 100})),
        ],
    )
    httpretty.register_uri(
        httpretty.GET,
        uri=API_URL + "/jobs/f00ba5/results",
        body=json.dumps(
            {
                "assets": {
                    "output.tiff": {
                        "href": API_URL + "/jobs/f00ba5/files/output.tiff",
                        "type": "image/tiff; application=geotiff",
                    },
                }
            }
        ),
    )
    httpretty.register_uri(httpretty.GET, uri=API_URL + "/jobs/f00ba5/files/output.tiff", body="tiffdata")
    httpretty.register_uri(httpretty.GET, uri=API_URL + "/jobs/f00ba5/logs", body=json.dumps({"logs": []}))

    con = openeo.connect(API_URL, retry=retry_config)

    max_poll_interval = 0.1
    connection_retry_interval = 12.34
    with mock.patch("time.sleep") as sleep_mock:
        job = con.load_collection("SENTINEL2").create_job()
        with expectation_context:
            job.start_and_wait(max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval)

    # Check retry related sleeps
    actual_sleeps = [args[0] for args, kwargs in sleep_mock.call_args_list]
    actual_sleeps = [s for s in actual_sleeps if s != max_poll_interval]
    assert actual_sleeps == expected_sleeps


class LogGenerator:
    """Helper to generate log entry (dicts) with auto-generated ids, messages, etc."""

    def __init__(self):
        self._auto_id = itertools.count().__next__

    def _auto_message(self, id: str, level: str) -> str:
        greeting = {"debug": "Yo", "info": "Hello", "warning": "Beware", "error": "Halt!"}.get(level, "Greetings")
        return f"{greeting} {id}"

    def log(self, message: Optional[str] = None, *, id: Optional[str] = None, level: str = "info") -> dict:
        id = id or f"abc{self._auto_id():03d}"
        message = message or self._auto_message(id=id, level=level)
        return {"id": id, "level": level, "message": message}

    def debug(self, **kwargs) -> dict:
        return self.log(level="debug", **kwargs)

    def info(self, **kwargs) -> dict:
        return self.log(level="info", **kwargs)

    def warning(self, **kwargs) -> dict:
        return self.log(level="warning", **kwargs)

    def error(self, **kwargs) -> dict:
        return self.log(level="error", **kwargs)

    def __call__(self, **kwargs):
        return self.log(**kwargs)


@pytest.fixture
def log_generator() -> LogGenerator:
    return LogGenerator()


def test_get_job_logs_basic(con100, requests_mock, log_generator):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                log_generator.info(message="Starting"),
                log_generator.error(message="Nope!"),
            ]
        },
    )

    logs = con100.job("f00ba5").logs(offset="TODO")
    # Original interface
    assert logs == [
        {"id": "abc000", "level": "info", "message": "Starting"},
        {"id": "abc001", "level": "error", "message": "Nope!"},
    ]
    assert logs == [
        LogEntry(id="abc000", level="info", message="Starting"),
        LogEntry(id="abc001", level="error", message="Nope!"),
    ]
    # Explicit property to get log entry listing
    assert logs.logs == [
        LogEntry(id="abc000", level="info", message="Starting"),
        LogEntry(id="abc001", level="error", message="Nope!"),
    ]


def test_get_job_logs_extra_metadata(con100, requests_mock, log_generator):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [log_generator.info(message="Hello world")],
            "links": [
                {"rel": "next", "href": "https://oeo.test/jobs/f00ba5/logs?offset=123abc"},
            ],
            "federation:missing": ["eoeb"],
        },
    )

    logs = con100.job("f00ba5").logs()
    assert logs.logs == [
        LogEntry(id="abc000", level="info", message="Hello world"),
    ]
    assert logs.links == [
        Link(rel="next", href="https://oeo.test/jobs/f00ba5/logs?offset=123abc"),
    ]
    assert logs.ext_federation_missing() == ["eoeb"]


def test_get_job_logs_level_handling_default(con100, requests_mock, log_generator):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                log_generator.error(),
                log_generator.debug(),
                log_generator.info(),
                log_generator.warning(),
            ]
        },
    )
    logs = con100.job("f00ba5").logs()
    assert logs == [
        {"id": "abc000", "level": "error", "message": "Halt! abc000"},
        {"id": "abc001", "level": "debug", "message": "Yo abc001"},
        {"id": "abc002", "level": "info", "message": "Hello abc002"},
        {"id": "abc003", "level": "warning", "message": "Beware abc003"},
    ]


@pytest.mark.parametrize(
    ["levels", "expected"],
    [
        ([logging.ERROR, "error", "ERROR"], [{"id": "abc005", "level": "error", "message": "Halt! abc005"}]),
        (
            [logging.WARNING, "warning", "WARNING"],
            [
                {"id": "abc004", "level": "warning", "message": "Beware abc004"},
                {"id": "abc005", "level": "error", "message": "Halt! abc005"},
            ],
        ),
        (
            [logging.INFO, "INFO", "info"],
            [
                {"id": "abc000", "level": "info", "message": "Hello abc000"},
                {"id": "abc003", "level": "info", "message": "Hello abc003"},
                {"id": "abc004", "level": "warning", "message": "Beware abc004"},
                {"id": "abc005", "level": "error", "message": "Halt! abc005"},
            ],
        ),
        (
            [logging.DEBUG, "DEBUG", "debug", None, 0],
            [
                {"id": "abc000", "level": "info", "message": "Hello abc000"},
                {"id": "abc001", "level": "debug", "message": "Yo abc001"},
                {"id": "abc002", "level": "weird", "message": "Greetings abc002"},
                {"id": "abc003", "level": "info", "message": "Hello abc003"},
                {"id": "abc004", "level": "warning", "message": "Beware abc004"},
                {"id": "abc005", "level": "error", "message": "Halt! abc005"},
            ],
        ),
    ],
)
def test_get_job_logs_level_handling_custom(con100, requests_mock, log_generator, levels, expected):
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                log_generator.info(),
                log_generator.debug(),
                log_generator.log(level="weird"),
                log_generator.info(),
                log_generator.warning(),
                log_generator.error(),
            ]
        },
    )

    for level in levels:
        logs = con100.job("f00ba5").logs(level=level)
        assert logs == expected


@pytest.mark.parametrize(
    ["response_extra", "expected"],
    [
        (
            {},
            [{"id": "abc001", "level": "error", "message": "Halt! abc001"}],
        ),
        (
            {"level": "warning"},
            [
                {"id": "abc000", "level": "info", "message": "Not a warning"},
                {"id": "abc001", "level": "error", "message": "Halt! abc001"},
            ],
        ),
    ],
)
def test_get_job_logs_level_handling_custom_with_backend_level(
    con100, requests_mock, log_generator, response_extra, expected
):
    """If backend response includes a "level": trust it (no client-side filtering)."""
    requests_mock.get(
        API_URL + "/jobs/f00ba5/logs",
        json={
            "logs": [
                log_generator.info(message="Not a warning"),
                log_generator.error(),
            ],
            **response_extra,
        },
    )
    assert con100.job("f00ba5").logs(level="warning") == expected


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


def test_get_results_metadata_url(con100):
    job = con100.job("job-456")
    assert job.get_results_metadata_url() == "/jobs/job-456/results"


def test_get_results_metadata_url_full(con100):
    job = con100.job("job-456")
    assert (
        job.get_results_metadata_url(full=True)
        == "https://oeo.test/jobs/job-456/results"
    )


@pytest.fixture
def job_with_1_asset(con100, requests_mock, tmp_path) -> BatchJob:
    requests_mock.get(API_URL + "/jobs/jj1/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.head(API_URL + "/dl/jjr1.tiff", headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)

    job = BatchJob("jj1", connection=con100)
    return job

@pytest.fixture
def job_with_chunked_asset_using_head(con100, requests_mock, tmp_path) -> BatchJob:
    def handle_content(request, context):
        range = request.headers.get("Range")
        assert range
        search = re.search(r"bytes=(\d+)-(\d+)", range)
        assert search
        from_bytes = int(search.group(1))
        to_bytes = int(search.group(2))
        assert from_bytes < to_bytes
        return TIFF_CONTENT[from_bytes : to_bytes + 1]

    requests_mock.get(
        API_URL + "/jobs/jj1/results",
        json={
            "assets": {
                "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
            }
        },
    )
    requests_mock.head(
        API_URL + "/dl/jjr1.tiff", headers={"Content-Length": f"{len(TIFF_CONTENT)}", "Accept-Ranges": "bytes"}
    )
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=handle_content)
    job = BatchJob("jj1", connection=con100)
    return job


@pytest.fixture
def job_with_chunked_asset_using_head_old(con100, requests_mock, tmp_path) -> BatchJob:
    requests_mock.get(API_URL + "/jobs/jj1/results", json={"assets": {
        "1.tiff": {"href": API_URL + "/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
    }})
    requests_mock.head(API_URL + "/dl/jjr1.tiff", headers={"Content-Length": f"{len(TIFF_CONTENT)}", "Accept-Ranges": "bytes"})

    chunk_size = 1000
    for r in range(0, len(TIFF_CONTENT), chunk_size):
        from_bytes = r
        to_bytes = min(r + chunk_size, len(TIFF_CONTENT)) - 1
        requests_mock.get(API_URL + "/dl/jjr1.tiff", request_headers={"Range": f"bytes={from_bytes}-{to_bytes}"},
                     response_list = [{"status_code": 500, "text": "Server error"},
                                      {"status_code": 206, "content": TIFF_CONTENT[from_bytes:to_bytes+1]}])
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
    requests_mock.head(API_URL + "/dl/jjr1.tiff", headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
    requests_mock.get(API_URL + "/dl/jjr1.tiff", content=TIFF_CONTENT)
    requests_mock.head(API_URL + "/dl/jjr2.tiff", headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
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

def test_get_results_download_file_ranged(job_with_chunked_asset_using_head: BatchJob, tmp_path):
    job = job_with_chunked_asset_using_head
    target = tmp_path / "result.tiff"
    res = job.get_results().download_file(target, range_size=1000)
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
    requests_mock.head(href, headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={'type': 'image/tiff; application=geotiff'})
    target = tmp_path / "res.tiff"
    path = asset.download(target)

    assert isinstance(path, Path)
    assert path.name == "res.tiff"
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_result_asset_download_file_error(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.head(href, status_code=500, text="Nope!")
    requests_mock.get(href, status_code=500, text="Nope!")

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, name="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})

    with pytest.raises(OpenEoApiPlainError, match="Nope!"):
        _ = asset.download(tmp_path / "res.tiff")

    # Nothing should be downloaded
    assert list(tmp_path.iterdir()) == []


def test_result_asset_download_folder(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.head(href, headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
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
    requests_mock.head(href, headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
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
    requests_mock.head("https://evilcorp.test/dl/jjr1.tiff", headers={"Content-Length": "666"})
    requests_mock.get("https://evilcorp.test/dl/jjr1.tiff", content=download_tiff)

    con100.authenticate_basic("john", "j0hn")
    job = BatchJob("jj1", connection=con100)
    target = tmp_path / "result.tiff"
    res = job.get_results().download_file(target)
    assert res == target
    with target.open("rb") as f:
        assert f.read() == TIFF_CONTENT



@pytest.mark.parametrize(
    ["list_jobs_kwargs", "expected_qs"],
    [
        ({}, {"limit": ["100"]}),
        ({"limit": None}, {}),
        ({"limit": 123}, {"limit": ["123"]}),
    ],
)
def test_list_jobs(con100, requests_mock, list_jobs_kwargs, expected_qs, basic_auth):

    def get_jobs(request, context):
        assert request.headers["Authorization"] == f"Bearer basic//{basic_auth.access_token}"
        assert request.qs == expected_qs
        return {
            "jobs": [
                {
                    "id": "job123",
                    "status": "running",
                    "created": "2021-02-22T09:00:00Z",
                },
                {
                    "id": "job456",
                    "status": "created",
                    "created": "2021-03-22T10:00:00Z",
                },
            ]
        }

    requests_mock.get(API_URL + "/jobs", json=get_jobs)

    con100.authenticate_basic(basic_auth.username, basic_auth.password)
    jobs = con100.list_jobs(**list_jobs_kwargs)
    assert jobs == [
        {"id": "job123", "status": "running", "created": "2021-02-22T09:00:00Z"},
        {"id": "job456", "status": "created", "created": "2021-03-22T10:00:00Z"},
    ]


def test_list_jobs_extra_metadata(con100, requests_mock, caplog, basic_auth):

    def get_jobs(request, context):
        assert request.headers["Authorization"] == f"Bearer basic//{basic_auth.access_token}"
        return {
            "jobs": [
                {
                    "id": "job123",
                    "status": "running",
                    "created": "2021-02-22T09:00:00Z",
                },
                {
                    "id": "job456",
                    "status": "created",
                    "created": "2021-03-22T10:00:00Z",
                },
            ],
            "links": [
                {"rel": "next", "href": API_URL + "/jobs?limit=2&offset=2"},
            ],
            "federation:missing": ["oeob"],
        }

    requests_mock.get(API_URL + "/jobs", json=get_jobs)

    con100.authenticate_basic(basic_auth.username, basic_auth.password)
    jobs = con100.list_jobs()
    assert jobs == [
        {"id": "job123", "status": "running", "created": "2021-02-22T09:00:00Z"},
        {"id": "job456", "status": "created", "created": "2021-03-22T10:00:00Z"},
    ]
    assert jobs.links == [Link(rel="next", href="https://oeo.test/jobs?limit=2&offset=2")]
    assert jobs.ext_federation_missing() == ["oeob"]
    assert "Partial job listing: missing federation components: ['oeob']." in caplog.text
