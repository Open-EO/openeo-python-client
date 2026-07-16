"""
Pytest plugin to automatically list
job ids and synchronous processing request ids
in the test reports.
"""

import logging
from typing import List

import pytest

from openeo import Connection
from openeo.utils.events import EVENTS

_log = logging.getLogger(__name__)

# Simple annotation alias for now
History = List[str]

_AUTO_LIST_JOB_IDS_KEY = pytest.StashKey[History]()


def _instrument_connection(connection: Connection, *, request: pytest.FixtureRequest):
    """
    Inject instrumentation into the given connection to
    collect job ids and synchronous processing request ids
    """
    # TODO: current implementation assumes the connection is created in a function scope,
    #       it does not properly support workflows where a connection object is shared across tests:
    #       there is no unregistration of the handler or guarding against double registration

    def _register(id: str):
        history = request.node.stash.setdefault(_AUTO_LIST_JOB_IDS_KEY, [])
        history.append(id)

    @connection.events.on(EVENTS.JOB_CREATED)
    def on_job_created(job_id: str, **kwargs):
        _register(job_id)

    @connection.events.on(EVENTS.SYNC_RESULT)
    def on_sync_result(sync_id: str, **kwargs):
        _register(sync_id)


@pytest.fixture(scope="function")
def auto_list_job_ids(request):
    """
    Fixture to be called on the openeo connection object
    when created in test function or dedicated fixture.
    """

    def _instrument(connection: Connection):
        _instrument_connection(connection=connection, request=request)

    return _instrument


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield

    if call.when == "call":
        report = outcome.get_result()

        if history := item.stash.get(_AUTO_LIST_JOB_IDS_KEY, []):
            title = "Jobs created and sync processing during this test"
            # TODO: represent as Markdown list
            # TODO: allow callback on the job_ids (e.g. to wrap them in link to some debug utility?)
            listing = "\n".join(history)
            report.sections.append((title, listing))

            # TODO: the following is a poorly-documented JUnitXML/Jenkins-specific hack
            #       to customize the error message.
            #       Unclear if there is a cleaner way to do this.
            if report.failed and hasattr(report.longrepr, "reprcrash"):
                report.longrepr.reprcrash.message = f"{report.longrepr.reprcrash.message}\n\n{title}:\n{listing}"
