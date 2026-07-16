import logging

from openeo.extra.job_management._thread_worker import _JobStartTask, _TaskResult
from openeo.rest._testing import DummyBackend


def test_concurrent_job_limit_is_retryable(requests_mock, caplog):
    caplog.set_level(logging.WARNING)
    backend = DummyBackend.at_url("https://foo.test", requests_mock=requests_mock)
    job = backend.connection.create_job(process_graph={})
    backend.setup_job_start_failure(
        status_code=400,
        response_body={"code": "ConcurrentJobLimit", "message": "Concurrent job limit reached."},
    )

    result = _JobStartTask(
        job_id=job.job_id,
        df_idx=0,
        root_url=backend.connection.root_url,
    ).execute()

    assert result == _TaskResult(
        job_id="job-000",
        df_idx=0,
        db_update={"status": "created"},
        stats_update={"job start retry": 1},
    )
    assert caplog.messages == [
        "Backend concurrent job limit reached while starting job 'job-000'; "
        "scheduling another attempt in a later job manager cycle."
    ]
