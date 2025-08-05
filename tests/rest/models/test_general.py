import re

import dirty_equals
import pytest

from openeo.rest.models.general import (
    CollectionListingResponse,
    JobListingResponse,
    Link,
    LogsResponse,
    ProcessListingResponse,
)
from openeo.rest.models.logs import LogEntry


class TestLink:
    def test_basic(self):
        link = Link(rel="about", href="https://example.com/about")
        assert link.rel == "about"
        assert link.href == "https://example.com/about"
        assert link.title is None
        assert link.type is None

    def test_full(self):
        link = Link(rel="about", href="https://example.com/about", type="text/html", title="About example")
        assert link.rel == "about"
        assert link.href == "https://example.com/about"
        assert link.title == "About example"
        assert link.type == "text/html"

    def test_repr(self):
        link = Link(rel="about", href="https://example.com/about")
        assert repr(link) == "Link(rel='about', href='https://example.com/about', type=None, title=None)"


class TestCollectionListingResponse:
    def test_basic(self):
        data = {"collections": [{"id": "S2"}, {"id": "S3"}]}
        collections = CollectionListingResponse(data)
        assert collections == [{"id": "S2"}, {"id": "S3"}]
        assert repr(collections) == "[{'id': 'S2'}, {'id': 'S3'}]"

    def test_links(self):
        data = {
            "collections": [{"id": "S2"}, {"id": "S3"}],
            "links": [
                {"rel": "self", "href": "https://openeo.test/collections"},
                {"rel": "next", "href": "https://openeo.test/collections?page=2"},
            ],
        }
        collections = CollectionListingResponse(data)
        assert collections.links == [
            Link(rel="self", href="https://openeo.test/collections"),
            Link(rel="next", href="https://openeo.test/collections?page=2"),
        ]

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {"collections": [{"id": "S2"}], "federation:missing": ["wwu"]},
                ["wwu"],
            ),
            (
                {"collections": [{"id": "S2"}]},
                None,
            ),
        ],
    )
    def test_federation_missing(self, data, expected):
        collections = CollectionListingResponse(data)
        assert collections.ext_federation_missing() == expected

    def test_repr_html_basic(self):
        data = {"collections": [{"id": "S2"}]}
        collections = CollectionListingResponse(data)
        assert collections._repr_html_() == dirty_equals.IsStr(
            regex=r'.*<openeo-collections>.*"collections":\s*\[{"id":\s*"S2".*', regex_flags=re.DOTALL
        )


class TestProcessListingResponse:
    def test_basic(self):
        data = {"processes": [{"id": "ndvi"}, {"id": "s2mask"}]}
        processes = ProcessListingResponse(data)
        assert processes == [{"id": "ndvi"}, {"id": "s2mask"}]
        assert repr(processes) == "[{'id': 'ndvi'}, {'id': 's2mask'}]"

    def test_links(self):
        data = {
            "processes": [{"id": "ndvi"}, {"id": "s2mask"}],
            "links": [
                {"rel": "self", "href": "https://openeo.test/processes"},
                {"rel": "next", "href": "https://openeo.test/processes?page=2"},
            ],
        }
        processes = ProcessListingResponse(data)
        assert processes.links == [
            Link(rel="self", href="https://openeo.test/processes"),
            Link(rel="next", href="https://openeo.test/processes?page=2"),
        ]

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {"processes": [{"id": "ndvi"}], "federation:missing": ["wow"]},
                ["wow"],
            ),
            (
                {"processes": [{"id": "ndvi"}]},
                None,
            ),
        ],
    )
    def test_federation_missing(self, data, expected):
        processes = ProcessListingResponse(data)
        assert processes.ext_federation_missing() == expected


    def test_repr_html_basic(self):
        data = {"processes": [{"id": "ndvi"}, {"id": "s2mask"}]}
        processes = ProcessListingResponse(data)
        assert processes._repr_html_() == dirty_equals.IsStr(
            regex=r'.*<openeo-processes>.*"processes":\s*\[{"id":\s*"ndvi".*', regex_flags=re.DOTALL
        )


class TestJobListingResponse:
    def test_basic(self):
        data = {"jobs": [{"id": "job-01"}, {"id": "job-02"}]}
        jobs = JobListingResponse(data)
        assert jobs == [{"id": "job-01"}, {"id": "job-02"}]
        assert repr(jobs) == "[{'id': 'job-01'}, {'id': 'job-02'}]"

    def test_links(self):
        data = {
            "jobs": [{"id": "job-01"}, {"id": "job-02"}],
            "links": [
                {"rel": "self", "href": "https://openeo.test/jobs"},
                {"rel": "next", "href": "https://openeo.test/jobs?page=2"},
            ],
        }
        jobs = JobListingResponse(data)
        assert jobs.links == [
            Link(rel="self", href="https://openeo.test/jobs"),
            Link(rel="next", href="https://openeo.test/jobs?page=2"),
        ]

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {"jobs": [{"id": "job-01"}], "federation:missing": ["wow"]},
                ["wow"],
            ),
            (
                {"jobs": [{"id": "job-01"}]},
                None,
            ),
        ],
    )
    def test_federation_missing(self, data, expected):
        jobs = JobListingResponse(data)
        assert jobs.ext_federation_missing() == expected

    def test_repr_html_basic(self):
        data = {"jobs": [{"id": "job-01"}, {"id": "job-02"}]}
        jobs = JobListingResponse(data)
        assert jobs._repr_html_() == dirty_equals.IsStr(
            regex=r'.*<openeo-data-table>.*"data":\s*\[{"id":\s*"job-01".*', regex_flags=re.DOTALL
        )


class TestLogsResponse:
    def test_basic(self):
        data = {"logs": [{"id": "log-01", "level": "info", "message": "hello"}]}
        logs = LogsResponse(data)
        assert logs == [{"id": "log-01", "level": "info", "message": "hello"}]
        assert logs == [LogEntry(id="log-01", level="info", message="hello")]
        assert logs.logs == [{"id": "log-01", "level": "info", "message": "hello"}]
        assert logs.logs == [LogEntry(id="log-01", level="info", message="hello")]
        assert repr(logs) == "[{'id': 'log-01', 'level': 'info', 'message': 'hello'}]"

    def test_links(self):
        data = {
            "logs": [{"id": "log-01", "level": "info", "message": "hello"}],
            "links": [
                {"rel": "self", "href": "https://openeo.test/logs"},
                {"rel": "next", "href": "https://openeo.test/logs?page=2"},
            ],
        }
        logs = LogsResponse(data)
        assert logs.links == [
            Link(rel="self", href="https://openeo.test/logs"),
            Link(rel="next", href="https://openeo.test/logs?page=2"),
        ]

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {"logs": [{"id": "log-01", "level": "info", "message": "hello"}], "federation:missing": ["wow"]},
                ["wow"],
            ),
            (
                {"logs": [{"id": "log-01", "level": "info", "message": "hello"}]},
                None,
            ),
        ],
    )
    def test_federation_missing(self, data, expected):
        logs = LogsResponse(data)
        assert logs.ext_federation_missing() == expected

    def test_repr_html_basic(self):
        data = {"logs": [{"id": "log-01", "level": "info", "message": "hello"}]}
        logs = LogsResponse(data)
        assert logs._repr_html_() == dirty_equals.IsStr(
            regex=r'.*<openeo-logs>.*"logs":\s*\[{"id":\s*"log-01".*', regex_flags=re.DOTALL
        )
