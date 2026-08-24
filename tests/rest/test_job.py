import contextlib
import itertools
import json
import logging
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from unittest import mock

import dirty_equals
import httpretty
import pytest
import requests

import openeo
import openeo.rest.job
from openeo.rest import JobFailedException, OpenEoApiPlainError, OpenEoClientException
from openeo.rest._testing import JobResultCollectionMocker
from openeo.rest.job import (
    BatchJob,
    JobResultDownloadException,
    ResultAsset,
    _filename_extension_from_url,
    _filename_from_url,
    _JobResultDownloader,
    _sanitize_filename,
)
from openeo.rest.models.general import Link
from openeo.rest.models.logs import LogEntry
from openeo.testing.stac import StacDummyBuilder
from openeo.util import dict_no_none, load_json
from openeo.utils.events import EVENTS
from openeo.utils.http import (
    HTTP_402_PAYMENT_REQUIRED,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
)

_log = logging.getLogger(__name__)

API_URL = "https://oeo.test"

TIFF_CONTENT = b"T1f7D6t6l0l" * 10000


@pytest.fixture
def con100(requests_mock):
    requests_mock.get(
        API_URL + "/", json={"api_version": "1.0.0", "endpoints": [{"path": "/credentials/basic", "methods": ["GET"]}]}
    )
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

    requests_mock.get(API_URL + "/jobs/f00ba5/logs", json={"logs": []})

    path = tmpdir.join("tmp.tiff")
    log = []

    with fake_time():
        job = con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF", max_poll_interval=0.1, print=log.append
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
                outputfile=path, out_format="GTIFF", max_poll_interval=0.1, print=log.append
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


@pytest.mark.parametrize(
    ["error_response", "expected"],
    [
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
                "json": {"code": "OidcProviderUnavailable", "message": "OIDC Provider is unavailable"},
            },
            "Service availability error while polling job status: [503] OidcProviderUnavailable: OIDC Provider is unavailable",
        ),
        (
            {"status_code": 502, "text": "Bad Gateway"},
            "Service availability error while polling job status: [502] Bad Gateway",
        ),
    ],
)
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
    requests_mock.get(API_URL + "/jobs/f00ba5/logs", json={"logs": []})

    path = tmpdir.join("tmp.tiff")
    log = []

    with fake_time():
        job = con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF", max_poll_interval=0.1, print=log.append
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


@pytest.mark.parametrize(
    ["error_response", "expected"],
    [
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
                "json": {"code": "OidcProviderUnavailable", "message": "OIDC Provider is unavailable"},
            },
            "Service availability error while polling job status: [503] OidcProviderUnavailable: OIDC Provider is unavailable",
        ),
        (
            {"status_code": 502, "text": "Bad Gateway"},
            "Service availability error while polling job status: [502] Bad Gateway",
        ),
    ],
)
def test_execute_batch_with_excessive_soft_errors(con100, requests_mock, tmpdir, error_response, expected):
    requests_mock.get(API_URL + "/file_formats", json={"output": {"GTiff": {"gis_data_types": ["raster"]}}})
    requests_mock.get(API_URL + "/collections/SENTINEL2", json={"foo": "bar"})
    requests_mock.post(API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"})
    requests_mock.post(API_URL + "/jobs/f00ba5/results", status_code=202)
    responses = [
        {"json": {"status": "queued"}},
        {"json": {"status": "running", "progress": 15}},
    ]
    responses.extend([error_response] * 20)
    requests_mock.get(API_URL + "/jobs/f00ba5", responses)

    path = tmpdir.join("tmp.tiff")
    log = []

    with fake_time(), pytest.raises(OpenEoClientException, match="Excessive soft errors"):
        con100.load_collection("SENTINEL2").execute_batch(
            outputfile=path, out_format="GTIFF", max_poll_interval=0.1, print=log.append
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


def test_start_job_event(dummy_backend):
    history = []
    dummy_backend.connection.events.on(EVENTS.JOB_STARTED, lambda **kwargs: history.append(kwargs))
    cube = dummy_backend.connection.load_collection("S2").save_result(format="GTiff")
    job = cube.create_job()
    assert history == []
    job.start()
    assert history == [{"event": "job.started", "job_id": "job-000"}]


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
            "title": "Foo",
            "description": "just testing",
        }
        return True

    requests_mock.post(
        API_URL + "/jobs", status_code=201, headers={"OpenEO-Identifier": "f00ba5"}, additional_matcher=check_request
    )
    con100.create_job({"foo1": {"process_id": "foo"}}, title="Foo", description="just testing")


def test_get_results_metadata_url(con100):
    job = con100.job("job-456")
    assert job.get_results_metadata_url() == "/jobs/job-456/results"


def test_get_results_metadata_url_full(con100):
    job = con100.job("job-456")
    assert job.get_results_metadata_url(full=True) == "https://oeo.test/jobs/job-456/results"


@pytest.fixture
def job_with_results_mocker(con100, requests_mock) -> Callable:
    """
    Helper to set up a job with downloadable assets
    (STAC Item style)
    """

    def setup(*, job_id="jj1", assets: dict, media_type: str = "image/tiff; application=geotiff"):
        results_doc = {
            "type": "Feature",
            "assets": {
                asset_key: {"href": f"{API_URL}/{asset_path.lstrip('/')}", "type": media_type}
                for asset_key, asset_path in assets.items()
            },
        }
        _log.info(f"{results_doc=}")
        requests_mock.get(f"{API_URL}/jobs/{job_id}/results", json=results_doc)

        for asset_key, asset_path in assets.items():
            requests_mock.head(
                f"{API_URL}/{asset_path.lstrip('/')}", headers={"Content-Length": f"{len(TIFF_CONTENT)}"}
            )
            requests_mock.get(f"{API_URL}/{asset_path.lstrip('/')}", content=TIFF_CONTENT)

        job = BatchJob(job_id, connection=con100)
        return job

    return setup



@pytest.fixture
def job_with_1_asset(job_with_results_mocker) -> BatchJob:
    return job_with_results_mocker(job_id="jj1", assets={"1.tiff": "/dl/jjr1.tiff"})


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

    chunk_size = 1000
    for r in range(0, len(TIFF_CONTENT), chunk_size):
        from_bytes = r
        to_bytes = min(r + chunk_size, len(TIFF_CONTENT)) - 1
        requests_mock.get(
            API_URL + "/dl/jjr1.tiff",
            request_headers={"Range": f"bytes={from_bytes}-{to_bytes}"},
            response_list=[
                {"status_code": 500, "text": "Server error"},
                {"status_code": 206, "content": TIFF_CONTENT[from_bytes : to_bytes + 1]},
            ],
        )
    job = BatchJob("jj1", connection=con100)
    return job


@pytest.fixture
def job_with_2_assets(job_with_results_mocker) -> BatchJob:
    return job_with_results_mocker(
        job_id="jj2",
        assets={
            "1.tiff": "/dl/jjr1.tiff",
            "2.tiff": "/dl/jjr2.tiff",
        },
    )


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


@pytest.mark.parametrize(
    ["asset_key", "href", "expected"],
    [
        ("1.tiff", "/dl/1.tiff", "1.tiff"),
        ("1.tiff", "/dl-1", "1.tiff"),
        ("res1", "/dl/1.tiff", "res1-1.tiff"),
        ("res1", "/dl-1", "res1-dl-1.tiff"),
        ("res/1", "/dl/1.tiff", "res1-1.tiff"),
        ("res/1", "/dl-1", "res1-dl-1.tiff"),
        ("res/1.tiff", "/dl/1.tiff", "res1.tiff-1.tiff"),
        ("res/1.tiff", "/dl-1", "res1.tiff-dl-1.tiff"),
        ("re$:1t#f", "/dl/1.tiff", "re1tf-1.tiff"),
        ("re$:1t#f", "/dl-1", "re1tf-dl-1.tiff"),
        ("26/06/19 1:2:3#45z", "/dl/45z", "26061912345z-45z.tiff"),
    ],
)
def test_get_results_download_file_asset_keys(job_with_results_mocker, tmp_path, asset_key, href, expected):
    job = job_with_results_mocker(job_id="job-123", assets={asset_key: href})
    target = tmp_path / "folder"
    target.mkdir()

    res = job.get_results().download_file(target)

    assert res == target / expected
    assert list(p.name for p in target.iterdir()) == [expected]
    assert res.read_bytes() == TIFF_CONTENT


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


def test_get_results_multiple_download_single_by_key(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    target = tmp_path / "res.tiff"
    path = job.get_results().download_file(target, key="1.tiff")
    assert path == target
    assert list(p.name for p in tmp_path.iterdir()) == ["res.tiff"]
    with path.open("rb") as f:
        assert f.read() == TIFF_CONTENT


def test_get_results_multiple_download_single_by_wrong_key(job_with_2_assets: BatchJob, tmp_path):
    job = job_with_2_assets
    target = tmp_path / "res.tiff"
    expected = r"No asset 'foobar\.tiff' in: \['.\.tiff', '.\.tiff'\]"
    with pytest.raises(OpenEoClientException, match=expected):
        job.get_results().download_file(target, key="foobar.tiff")


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
    assert job.list_results() == {
        "type": "Feature",
        "assets": {
            "1.tiff": {"href": "https://oeo.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
            "2.tiff": {"href": "https://oeo.test/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
        },
    }


def test_get_results_download_files(job_with_2_assets: BatchJob, tmp_path):
    target = tmp_path / "folder"
    target.mkdir()

    job = job_with_2_assets
    results = job.get_results()

    assets = job.get_results().get_assets()
    assert {a.key: a.metadata for a in assets} == {
        "1.tiff": {"href": "https://oeo.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        "2.tiff": {"href": "https://oeo.test/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }

    downloads = results.download_files(target)
    assert set(downloads) == {target / "1.tiff", target / "2.tiff", target / "job-results.json"}
    assert set(p.name for p in target.iterdir()) == {"1.tiff", "2.tiff", "job-results.json"}
    assert (target / "1.tiff").read_bytes() == TIFF_CONTENT
    job_results_metadata = json.loads((target / "job-results.json").read_text())
    assert job_results_metadata["type"] == "Feature"
    assert job_results_metadata["assets"] == {
        "1.tiff": {"href": "https://oeo.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        "2.tiff": {"href": "https://oeo.test/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
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
        "1.tiff": {"href": "https://oeo.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
        "2.tiff": {"href": "https://oeo.test/dl/jjr2.tiff", "type": "image/tiff; application=geotiff"},
    }


@pytest.mark.parametrize(
    ["include_stac_metadata", "expected"],
    [(True, {"1.tiff", "2.tiff", "job-results.json"}), (False, {"1.tiff", "2.tiff"})],
)
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
    asset = ResultAsset(job, key="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
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
    asset = ResultAsset(job, key="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})

    with pytest.raises(OpenEoApiPlainError, match="Nope!"):
        _ = asset.download(tmp_path / "res.tiff")

    # Nothing should be downloaded
    assert list(tmp_path.iterdir()) == []


def test_result_asset_download_folder(con100, requests_mock, tmp_path):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.head(href, headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, key="1.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
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
    asset = ResultAsset(job, key="out.json", href=href, metadata={"type": "application/json"})
    res = asset.load_json()

    assert res == {"bands": [1, 2, 3]}


def test_result_asset_load_bytes(con100, requests_mock):
    href = API_URL + "/dl/jjr1.tiff"
    requests_mock.head(href, headers={"Content-Length": f"{len(TIFF_CONTENT)}"})
    requests_mock.get(href, content=TIFF_CONTENT)

    job = BatchJob("jj", connection=con100)
    asset = ResultAsset(job, key="out.tiff", href=href, metadata={"type": "image/tiff; application=geotiff"})
    res = asset.load_bytes()

    assert res == TIFF_CONTENT


def test_get_results_download_file_other_domain(con100, requests_mock, tmp_path):
    """https://github.com/Open-EO/openeo-python-client/issues/201"""
    secret = "!secret token!"
    requests_mock.get(API_URL + "/credentials/basic", json={"access_token": secret})

    def get_results(request, context):
        assert "auth" in repr(request.headers).lower()
        assert secret in repr(request.headers)
        return {
            "assets": {
                "1.tiff": {"href": "https://evilcorp.test/dl/jjr1.tiff", "type": "image/tiff; application=geotiff"},
            }
        }

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


class TestJobResults:
    # TODO: move all "job.get_results()" based tests
    #       inside this class for cleaner test structure

    @pytest.fixture
    def result_collection_mocker(self, con100, requests_mock) -> JobResultCollectionMocker:
        """helper to mock collection-style job results"""
        return JobResultCollectionMocker(requests_mock=requests_mock, connection=con100)

    def test_download_as_collection_basic(self, result_collection_mocker, tmp_path):
        job = result_collection_mocker.setup_job_results(
            items={"item1": {"assets": {"asset1": {"path": "asset1.tiff"}}}}
        )
        downloaded = job.get_results().download_as_collection(target=tmp_path)

        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "type": "Collection",
                    "links": [{"rel": "item", "href": "item1/item1.json"}],
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict(
                {
                    "id": "item1",
                    "type": "Feature",
                    "assets": {
                        "asset1": dirty_equals.IsPartialDict(href="asset1.tiff"),
                    },
                }
            ),
            "item1/asset1.tiff": b"TIFF-DUMMY-DATA",
        }

        TestJobResultDownloader.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    @pytest.mark.parametrize(
        ["redact_url_logging"],
        [
            (True,),
            (False,),
        ],
    )
    def test_download_as_collection_redact_url_logging(
        self, result_collection_mocker, tmp_path, redact_url_logging, caplog
    ):
        caplog.set_level(logging.DEBUG, logger="openeo.rest.job")

        job = result_collection_mocker.setup_job_results(
            items={
                "item1": {
                    "full_path": "items/item1.json?token=secret123",
                    "assets": {"asset1": {"full_path": "assets/asset1.tiff?token=secret456"}},
                }
            }
        )
        downloaded = job.get_results().download_as_collection(target=tmp_path, redact_url_logging=redact_url_logging)

        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "type": "Collection",
                    "links": [{"rel": "item", "href": "item1/item1.json"}],
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict(
                {
                    "id": "item1",
                    "type": "Feature",
                    "assets": {"asset1": dirty_equals.IsPartialDict(href="asset1.tiff")},
                }
            ),
            "item1/asset1.tiff": b"TIFF-DUMMY-DATA",
        }
        TestJobResultDownloader.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

        assert ("secret123" in caplog.text) == (not redact_url_logging)
        assert ("secret456" in caplog.text) == (not redact_url_logging)


class TestResultAsset:
    @pytest.fixture
    def job(self, con100):
        return BatchJob("jj", connection=con100)

    @pytest.mark.parametrize(
        ["key", "href", "media_type", "expected"],
        [
            ("cube.tiff", "http://data.test/dl/cube.tiff", None, "cube.tiff"),
            ("cube.tiff", "http://data.test/dl/output.tiff", None, "cube.tiff"),
            ("cube123.tiff", "http://data.test/dl/output.tiff", None, "cube123.tiff"),
            ("cube-123.tiff", "http://data.test/dl/output.tiff", None, "cube-123.tiff"),
            ("cube.v123.tiff", "http://data.test/dl/output.tiff", None, "cube.v123.tiff"),
            ("cube_123.tiff", "http://data.test/dl/output.tiff", None, "cube_123.tiff"),
            ("cube.t1ff", "http://data.test/dl/output.tiff", None, "cube.t1ff"),
            ("output", "http://data.test/dl/cube.tiff", None, "output-cube.tiff"),
            ("output/1", "http://data.test/dl/cube.tiff", None, "output1-cube.tiff"),
            ("output#1", "http://data.test/dl/cube.tiff", None, "output1-cube.tiff"),
            ("output(1)", "http://data.test/dl/cube.tiff", None, "output1-cube.tiff"),
            ("output[1]", "http://data.test/dl/cube.tiff", None, "output1-cube.tiff"),
            ("output:1", "http://data.test/dl/cube.tiff", None, "output1-cube.tiff"),
            ("output:1", "http://data.test/dl/cube", None, "output1-cube"),
            ("output:1", "http://data.test/dl/cube", "image/tiff", "output1-cube.tiff"),
            ("output:1", "http://data.test/dl/cube", "image/tiff; application=geotiff", "output1-cube.tiff"),
            ("output:1", "http://data.test/dl/cube", "application/x-netcdf", "output1-cube.nc"),
            ("ΩuτΠuτ.tiff", "http://data.test/dl/cube.tiff", "image/tiff", "ΩuτΠuτ.tiff"),
            ("donnée-discrètes.tiff", "http://data.test/dl/cube.tiff", "image/tiff", "donnée-discrètes.tiff"),
            ("band-weiß.tiff", "http://data.test/dl/cube.tiff", "image/tiff", "band-weiß.tiff"),
            ("bands/weiß", "http://data.test/dl/cube.tiff", "image/tiff", "bandsweiß-cube.tiff"),
            ("bands:weiß", "http://data.test/dl/cube.tiff", "image/tiff", "bandsweiß-cube.tiff"),
            ("立方体.tiff", "http://data.test/dl/cube.tiff", "image/tiff", "立方体.tiff"),
            ("立方体;123", "http://data.test/dl/cube.tiff", "image/tiff", "立方体123-cube.tiff"),
            # Percent-encoded unicode in href URL (e.g. from signed S3 URLs)
            ("output", "http://data.test/dl/file_%CF%83.nc", "application/x-netcdf", "output-file_σ.nc"),
            (
                "7ff3410f",
                "https://s3.example.com/jobs/test-dotError_proba-cube_year2024_32%CF%83LB03.nc?X-Amz-Signature=abc",
                "application/x-netcdf",
                "7ff3410f-test-dotError_proba-cube_year2024_32σLB03.nc",
            ),
        ],
    )
    def test_download_make_filename(self, tmp_path, job, key, href, media_type, expected, requests_mock):
        requests_mock.head(href, headers={"Content-Length": "data"})
        requests_mock.get(href, text="data")

        asset = ResultAsset(job=job, key=key, href=href, metadata=dict_no_none(type=media_type))
        res = asset.download(tmp_path)
        assert res == tmp_path / expected
        assert res.read_bytes() == b"data"


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


def test_sanitize_filename():
    assert _sanitize_filename("foo/bar.txt") == "foobar.txt"
    assert _sanitize_filename("foo/bar.txt", replacement="_") == "foo_bar.txt"
    assert _sanitize_filename(r"foo\bar.txt", replacement="_") == "foo_bar.txt"
    assert _sanitize_filename("foo\nbar.txt", replacement="_") == "foo_bar.txt"
    assert _sanitize_filename("foo$bar.txt", replacement="_") == "foo_bar.txt"
    assert _sanitize_filename("foo%bar.txt", replacement="_") == "foo_bar.txt"


def test_sanitize_filename_invalid():
    for filename in ["", " ", ".", " . ", "..", " .. "]:
        with pytest.raises(ValueError):
            _sanitize_filename(filename)

    # This is still fine however
    assert _sanitize_filename(".config") == ".config"
    # Weird, but you do you
    assert _sanitize_filename("..config") == "..config"


def test_filename_from_url():
    assert _filename_from_url("https://example.com/foo/bar.txt") == "bar.txt"
    assert _filename_from_url("https://example.com/foo/bar.txt?q=1&r=2#frag") == "bar.txt"
    assert _filename_from_url("https://example.com/foo/ba%CF%83.txt") == "baσ.txt"
    assert _filename_from_url("https://example.com/foo/bar") == "bar"
    assert _filename_from_url("https://example.com/foo/bar/") == "bar"
    assert _filename_from_url("https://example.com/") == ""

    # Full mode
    assert _filename_from_url("https://example.com/foo/bar.txt", full=True) == "foo/bar.txt"
    assert _filename_from_url("https://example.com/foo/bar.txt?q=1&r=2#frag", full=True) == "foo/bar.txt"
    assert _filename_from_url("https://example.com/fo%CF%83/ba%CF%83", full=True) == "foσ/baσ"
    assert _filename_from_url("https://example.com/foo/bar", full=True) == "foo/bar"
    assert _filename_from_url("https://example.com/foo/bar/", full=True) == "foo/bar"
    assert _filename_from_url("https://example.com/", full=True) == ""

    # Relative href
    assert _filename_from_url("foo/bar.txt") == "bar.txt"
    assert _filename_from_url("/foo/bar.txt") == "bar.txt"
    assert _filename_from_url("foo/bar.txt", full=True) == "foo/bar.txt"
    assert _filename_from_url("/foo/bar.txt", full=True) == "foo/bar.txt"


@pytest.mark.parametrize(
    ["url", "full", "expected"],
    [
        # Base cases
        ("https://example.com/foo/bar.txt", False, "bar.txt"),
        ("https://example.com/foo/bar.txt", True, "foo/bar.txt"),
        # Period usage
        ("https://example.com/foo/./bar.txt", False, "bar.txt"),
        ("https://example.com/foo/./bar.txt", True, ValueError(r"Invalid file/folder name '\.'")),
        ("https://example.com/foo/%2E/bar.txt", True, ValueError(r"Invalid file/folder name '\.'")),
        ("https://example.com/foo/.", False, ValueError(r"Invalid file/folder name '\.'")),
        # Double period usage
        ("https://example.com/foo/../bar.txt", False, "bar.txt"),
        ("https://example.com/foo/../bar.txt", True, ValueError(r"Invalid file/folder name '\.\.'")),
        ("https://example.com/foo/%2E%2E/bar.txt", True, ValueError(r"Invalid file/folder name '\.\.'")),
        ("https://example.com/foo/..", False, ValueError(r"Invalid file/folder name '\.\.'")),
        # Empty path parts
        ("https://example.com/foo/bar/", False, "bar"),
        ("https://example.com/foo/bar/", True, "foo/bar"),
        ("https://example.com/foo//bar.txt", False, "bar.txt"),
        ("https://example.com/foo//bar.txt", True, "foo/bar.txt"),
    ],
)
def test_filename_from_url_invalid_parts(url, full, expected):
    if isinstance(expected, Exception):
        with pytest.raises(type(expected), match=str(expected)):
            _filename_from_url(url, full=full)
    else:
        assert _filename_from_url(url, full=full) == expected


def test_filename_extension_from_url():
    assert _filename_extension_from_url("https://example.com/foo/bar.txt") == ".txt"
    assert _filename_extension_from_url("foo/bar.txt") == ".txt"
    assert _filename_extension_from_url("/foo/bar.txt") == ".txt"
    assert _filename_extension_from_url("https://example.com/foo/bar.tiff") == ".tiff"
    assert _filename_extension_from_url("https://example.com/foo/bar.tar.gz") == ".tar.gz"
    assert _filename_extension_from_url("https://example.com/foo/bar.txt?q=1&r=2#frag") == ".txt"
    assert _filename_extension_from_url("https://example.com/foo/ba%CF%83.%CF%84x%CF%84") == ".τxτ"
    assert _filename_extension_from_url("https://example.com/foo/bar") == ""
    assert _filename_extension_from_url("https://example.com/foo/bar/") == ""
    assert _filename_extension_from_url("https://example.com/") == ""

    assert _filename_extension_from_url("https://example.com/foo/bar", fallback=".data") == ".data"
    assert _filename_extension_from_url("https://example.com/foo/bar.txt", fallback=".data") == ".txt"


class TestJobResultDownloader:

    @pytest.fixture
    def result_mocker(self, con100, requests_mock) -> JobResultCollectionMocker:
        return JobResultCollectionMocker(requests_mock=requests_mock, connection=con100)

    @staticmethod
    def check_expected_downloads(downloaded: List[Path], expected: Dict[str, Any], tmp_path: Path):
        expected_paths = set(tmp_path / k for k in expected.keys())
        assert set(downloaded) == expected_paths
        assert set(p for p in tmp_path.glob("**/*") if p.is_file()) == expected_paths

        for path, expected_value in expected.items():
            actual = tmp_path / path
            if actual.suffix == ".json":
                assert load_json(actual) == expected_value
            elif actual.suffix in {".tif", ".tiff"}:
                assert actual.read_bytes() == expected_value
            else:
                raise ValueError(f"Unsupported {path=} {expected_value=}")

    def test_basic(self, result_mocker, tmp_path):
        job = result_mocker.setup_job_results(
            items={
                "item1": {"assets": {"asset1": {"path": "asset1.tiff"}}},
            }
        )
        downloader = _JobResultDownloader(job=job, target=tmp_path)
        downloaded = downloader.download_collection()
        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "id": "job-123-results",
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "links": [{"rel": "item", "href": "item1/item1.json"}],
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict(
                {
                    "id": "item1",
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "assets": {
                        "asset1": dirty_equals.IsPartialDict(href="asset1.tiff"),
                    },
                }
            ),
            "item1/asset1.tiff": b"TIFF-DUMMY-DATA",
        }
        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    def test_one_item_multiple_assets(self, result_mocker, tmp_path):
        job = result_mocker.setup_job_results(
            items={
                "item1": {
                    "assets": {
                        "asset1": {"path": "asset1.tiff"},
                        "asset2": {"path": "asset2.tiff"},
                        "asset3": {"path": "asset3.tiff"},
                    }
                }
            }
        )
        downloader = _JobResultDownloader(job=job, target=tmp_path)
        downloaded = downloader.download_collection()
        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "id": "job-123-results",
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "links": [{"rel": "item", "href": "item1/item1.json"}],
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict(
                {
                    "id": "item1",
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "assets": {
                        "asset1": dirty_equals.IsPartialDict(href="asset1.tiff"),
                        "asset2": dirty_equals.IsPartialDict(href="asset2.tiff"),
                        "asset3": dirty_equals.IsPartialDict(href="asset3.tiff"),
                    },
                }
            ),
            "item1/asset1.tiff": b"TIFF-DUMMY-DATA",
            "item1/asset2.tiff": b"TIFF-DUMMY-DATA",
            "item1/asset3.tiff": b"TIFF-DUMMY-DATA",
        }
        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    def test_multiple_items(self, result_mocker, tmp_path):
        job = result_mocker.setup_job_results(
            items={
                "item1": {
                    "assets": {
                        "asset1": {"path": "asset1.tiff"},
                    },
                },
                "item2": {
                    "assets": {
                        "asset2": {"path": "asset2.tiff"},
                        "asset3": {"path": "asset3.tiff"},
                    }
                },
            }
        )
        downloader = _JobResultDownloader(job=job, target=tmp_path)
        downloaded = downloader.download_collection()
        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "id": "job-123-results",
                    "type": "Collection",
                    "links": [
                        {"rel": "item", "href": "item1/item1.json"},
                        {"rel": "item", "href": "item2/item2.json"},
                    ],
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict(
                {
                    "id": "item1",
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "assets": {"asset1": dirty_equals.IsPartialDict(href="asset1.tiff")},
                }
            ),
            "item2/item2.json": dirty_equals.IsPartialDict(
                {
                    "id": "item2",
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "assets": {
                        "asset2": dirty_equals.IsPartialDict(href="asset2.tiff"),
                        "asset3": dirty_equals.IsPartialDict(href="asset3.tiff"),
                    },
                }
            ),
            "item1/asset1.tiff": b"TIFF-DUMMY-DATA",
            "item2/asset2.tiff": b"TIFF-DUMMY-DATA",
            "item2/asset3.tiff": b"TIFF-DUMMY-DATA",
        }
        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    @pytest.mark.parametrize(
        ["on_download_failure", "items_setup", "expected"],
        [
            (
                "warn",
                {
                    "item1": {
                        "assets": {"asset1": {"path": "asset1.tiff", "error": {"message": "Nope no asset1 for you"}}},
                    }
                },
                "Failed to download item asset..*Nope no asset1 for you",
            ),
            (
                "error",
                {
                    "item1": {
                        "assets": {"asset1": {"path": "asset1.tiff", "error": {"message": "Nope no asset1 for you"}}},
                    }
                },
                "Failed to download item asset.*Nope no asset1 for you",
            ),
            (
                "warn",
                {"item1": {"error": {"message": "Nope no item1 for you"}}},
                "Failed to download item.*Nope no item1 for you",
            ),
            (
                "error",
                {"item1": {"error": {"message": "Nope no item1 for you"}}},
                "Failed to download item.*Nope no item1 for you",
            ),
        ],
    )
    def test_warn_or_error_on_download_fail(
        self, result_mocker, tmp_path, caplog, on_download_failure, items_setup, expected
    ):
        job = result_mocker.setup_job_results(items=items_setup)

        expected = re.compile(expected)
        if on_download_failure == "error":
            context = pytest.raises(JobResultDownloadException, match=expected)
        else:
            context = contextlib.nullcontext()

        downloader = _JobResultDownloader(job=job, target=tmp_path, on_download_failure=on_download_failure)
        with context:
            downloader.download_collection()

        if on_download_failure == "warn":
            assert expected.search(caplog.text)

    @pytest.mark.parametrize(
        [
            "download_derived_from",
            "expected_links",
            "expected_downloads_extra",
        ],
        [
            (
                False,
                [
                    {"rel": "item", "href": "item1/item1.json"},
                    {"rel": "derived_from", "href": "https://oeo.test/j/job-123/r/d/derived_from.json"},
                ],
                {},
            ),
            (
                True,
                [
                    {"rel": "item", "href": "item1/item1.json"},
                    {"rel": "derived_from", "href": "derived_from.json"},
                ],
                {
                    "derived_from.json": {"hello": "world"},
                },
            ),
        ],
    )
    def test_download_derived_from_link(
        self, result_mocker, tmp_path, download_derived_from, expected_links, expected_downloads_extra
    ):
        job = result_mocker.setup_job_results(
            items={"item1": {}},
            linked_docs=[
                {"rel": "derived_from", "path": "derived_from.json", "json": {"hello": "world"}},
            ],
        )
        downloader = _JobResultDownloader(job=job, target=tmp_path)
        downloaded = downloader.download_collection(download_derived_from=download_derived_from)
        expected = {
            "job-results.json": dirty_equals.IsPartialDict(
                {
                    "type": "Collection",
                    "links": expected_links,
                }
            ),
            "item1/item1.json": dirty_equals.IsPartialDict({"id": "item1", "type": "Feature"}),
            **expected_downloads_extra,
        }
        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    @pytest.mark.parametrize(
        ["path_templates", "expected"],
        [
            (
                # Flat structure
                {
                    "collection": "CO_{job_id}.json",
                    "item": "IT_{item_id}.json",
                    "asset": "AS_{item_id}-{asset_key}-{asset_filename}",
                    "collection-asset": "CA_{asset_key}",
                    "generic-link": "GL_{filename}",
                },
                {
                    "CO_job-123.json": dirty_equals.IsPartialDict(
                        {
                            "links": [
                                {"rel": "item", "href": "IT_item1.json"},
                                {"rel": "derived_from", "href": "GL_derived_from.json"},
                            ]
                        }
                    ),
                    "IT_item1.json": dirty_equals.IsPartialDict(
                        {"assets": {"a1": dirty_equals.IsPartialDict(href="AS_item1-a1-asset1.tiff")}}
                    ),
                    "AS_item1-a1-asset1.tiff": b"TIFF-DUMMY-DATA",
                    "GL_derived_from.json": {"hello": "world"},
                },
            ),
            (
                # folder organisation per type
                {
                    "collection": "collections/{job_id}.json",
                    "item": "items/{job_id}-{item_id}/item.json",
                    "asset": "assets/{job_id}-{item_id}-{asset_key}/{asset_filename}",
                    "collection-asset": "assets/{job_id}-{asset_key}/{asset_filename}",
                    "generic-link": "docs/{job_id}/{filename}",
                },
                {
                    "collections/job-123.json": dirty_equals.IsPartialDict(
                        {
                            "links": [
                                {"rel": "item", "href": "../items/job-123-item1/item.json"},
                                {"rel": "derived_from", "href": "../docs/job-123/derived_from.json"},
                            ]
                        }
                    ),
                    "items/job-123-item1/item.json": dirty_equals.IsPartialDict(
                        {"assets": {"a1": dirty_equals.IsPartialDict(href="../../assets/job-123-item1-a1/asset1.tiff")}}
                    ),
                    "assets/job-123-item1-a1/asset1.tiff": b"TIFF-DUMMY-DATA",
                    "docs/job-123/derived_from.json": {"hello": "world"},
                },
            ),
        ],
    )
    def test_custom_file_tree_structure(self, result_mocker, tmp_path, path_templates, expected):
        job = result_mocker.setup_job_results(
            items={
                "item1": {"assets": {"a1": {"path": "asset1.tiff"}}},
            },
            linked_docs=[
                {"rel": "derived_from", "path": "derived_from.json", "json": {"hello": "world"}},
            ],
        )
        downloader = _JobResultDownloader(
            job=job,
            target=tmp_path,
            path_templates=path_templates,
        )
        downloaded = downloader.download_collection(download_derived_from=True)

        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    def test_custom_file_tree_structure_auto_increment(self, result_mocker, tmp_path):
        job = result_mocker.setup_job_results(
            items={
                "item11": {
                    "assets": {
                        "a11-1": {"path": "asset11-1.tiff", "content": b"DATA:11-1"},
                        "a11-2": {"path": "asset11-2.tiff", "content": b"DATA:11-2"},
                    }
                },
                "item22": {
                    "assets": {
                        "a22-1": {"path": "asset22-1.tiff", "content": b"DATA:22-1"},
                    }
                },
            },
            linked_docs=[
                {"rel": "derived_from", "path": "derived_from8.json", "json": {"hello": "eight"}},
                {"rel": "derived_from", "path": "derived_from9.json", "json": {"hello": "nine"}},
            ],
        )
        path_templates = {
            "collection": "collection-{auto_increment}{extension}",
            "item": "item-{auto_increment:02d}{extension}",
            "asset": "asset-{auto_increment:03d}{extension}",
            "collection-asset": "collection-asset-{auto_increment}{extension}",
            "generic-link": "generic-{auto_increment}{extension}",
        }

        downloader = _JobResultDownloader(
            job=job,
            target=tmp_path,
            path_templates=path_templates,
        )
        downloaded = downloader.download_collection(download_derived_from=True)

        expected = {
            "collection-1.json": dirty_equals.IsPartialDict(
                {
                    "links": [
                        {"rel": "item", "href": "item-01.json"},
                        {"rel": "item", "href": "item-02.json"},
                        {"rel": "derived_from", "href": "generic-1.json"},
                        {"rel": "derived_from", "href": "generic-2.json"},
                    ]
                }
            ),
            "item-01.json": dirty_equals.IsPartialDict(
                {
                    "assets": {
                        "a11-1": dirty_equals.IsPartialDict(href="asset-001.tiff"),
                        "a11-2": dirty_equals.IsPartialDict(href="asset-002.tiff"),
                    },
                }
            ),
            "item-02.json": dirty_equals.IsPartialDict(
                {
                    "assets": {
                        "a22-1": dirty_equals.IsPartialDict(href="asset-003.tiff"),
                    }
                }
            ),
            "asset-001.tiff": b"DATA:11-1",
            "asset-002.tiff": b"DATA:11-2",
            "asset-003.tiff": b"DATA:22-1",
            "generic-1.json": {"hello": "eight"},
            "generic-2.json": {"hello": "nine"},
        }

        self.check_expected_downloads(downloaded=downloaded, expected=expected, tmp_path=tmp_path)

    def test_download_collision_default(self, result_mocker, tmp_path, caplog):
        """Download collisions are logged as warning by default (on_download_failure="warn")"""
        job = result_mocker.setup_job_results(
            items={
                "item1": {"assets": {"a": {"full_path": "data/item1/asset.tiff"}}},
                "item2": {"assets": {"a": {"full_path": "data/item2/asset.tiff"}}},
            },
        )
        downloader = _JobResultDownloader(job=job, target=tmp_path, path_templates={"asset": "assets/{asset_filename}"})
        downloader.download_collection()
        assert caplog.text == dirty_equals.IsStr(
            regex=r".*Download collision, already downloaded.*asset\.tiff.*", regex_flags=re.DOTALL
        )

    def test_download_collision_with_raise(self, result_mocker, tmp_path):
        job = result_mocker.setup_job_results(
            items={
                "item1": {"assets": {"a": {"full_path": "data/item1/asset.tiff"}}},
                "item2": {"assets": {"a": {"full_path": "data/item2/asset.tiff"}}},
            },
        )
        downloader = _JobResultDownloader(
            job=job, target=tmp_path, path_templates={"asset": "assets/{asset_filename}"}, on_download_failure="raise"
        )
        with pytest.raises(JobResultDownloadException, match=r"Download collision, already downloaded.*asset\.tiff"):
            downloader.download_collection()
