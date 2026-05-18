from __future__ import annotations

import datetime
import json
import logging
import time
import typing
from pathlib import Path
from typing import Dict, List, Optional, Union

import requests

from openeo.internal.documentation import openeo_endpoint
from openeo.internal.jupyter import VisualDict, render_component, render_error
from openeo.internal.warnings import deprecated, legacy_alias
from openeo.rest import (
    DEFAULT_DOWNLOAD_CHUNK_SIZE,
    DEFAULT_DOWNLOAD_RANGE_SIZE,
    DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
    DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
    DEFAULT_JOB_STATUS_POLL_SOFT_ERROR_MAX,
    JobFailedException,
    OpenEoApiError,
    OpenEoApiPlainError,
    OpenEoClientException,
)
from openeo.rest.models.general import LogsResponse
from openeo.rest.models.logs import log_level_name
from openeo.util import ensure_dir
from openeo.utils.http import (
    HTTP_408_REQUEST_TIMEOUT,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_501_NOT_IMPLEMENTED,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
)

if typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime).
    from openeo.rest.connection import Connection

logger = logging.getLogger(__name__)


DEFAULT_JOB_RESULTS_FILENAME = "job-results.json"
MAX_RETRIES_PER_RANGE = 3
RETRIABLE_STATUSCODES = [
    HTTP_408_REQUEST_TIMEOUT,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_501_NOT_IMPLEMENTED,
    HTTP_502_BAD_GATEWAY,
    HTTP_503_SERVICE_UNAVAILABLE,
    HTTP_504_GATEWAY_TIMEOUT,
]


class BatchJob:
    """
    Handle for an openEO batch job, allowing it to describe, start, cancel, inspect results, etc.

    .. versionadded:: 0.11.0
        This class originally had the more cryptic name :py:class:`RESTJob`,
        which is still available as legacy alias,
        but :py:class:`BatchJob` is recommended since version 0.11.0.

    """

    # TODO #425 method to bootstrap `load_stac` directly from a BatchJob object

    def __init__(self, job_id: str, connection: Connection):
        self.job_id = job_id
        """Unique identifier of the batch job (string)."""

        self.connection = connection

    def __repr__(self):
        return '<{c} job_id={i!r}>'.format(c=self.__class__.__name__, i=self.job_id)

    def _repr_html_(self):
        data = self.describe()
        capabilities = self.connection.capabilities()
        return render_component(
            "job",
            data=data,
            parameters={
                "currency": capabilities.currency(),
                "federation": capabilities.ext_federation_backend_details(),
            },
        )

    @openeo_endpoint("GET /jobs/{job_id}")
    def describe(self) -> dict:
        """
        Get detailed metadata about a submitted batch job
        (title, process graph, status, progress, ...).

        .. versionadded:: 0.20.0
            This method was previously called :py:meth:`describe_job`.
        """
        return self.connection.get(f"/jobs/{self.job_id}", expected_status=200).json()

    describe_job = legacy_alias(describe, name="describe_job", since="0.20.0", mode="soft")

    def status(self) -> str:
        """
        Get the status of the batch job

        :return: batch job status, one of "created", "queued", "running", "canceled", "finished" or "error".
        """
        return self.describe().get("status", "N/A")

    @openeo_endpoint("DELETE /jobs/{job_id}")
    def delete(self):
        """
        Delete this batch job.

        .. versionadded:: 0.20.0
            This method was previously called :py:meth:`delete_job`.
        """
        self.connection.delete(f"/jobs/{self.job_id}", expected_status=204)

    delete_job = legacy_alias(delete, name="delete_job", since="0.20.0", mode="soft")

    @openeo_endpoint("GET /jobs/{job_id}/estimate")
    def estimate(self):
        """Calculate time/cost estimate for a job."""
        data = self.connection.get(
            f"/jobs/{self.job_id}/estimate", expected_status=200
        ).json()
        currency = self.connection.capabilities().currency()
        return VisualDict('job-estimate', data=data, parameters={'currency': currency})

    estimate_job = legacy_alias(estimate, name="estimate_job", since="0.20.0", mode="soft")

    @openeo_endpoint("POST /jobs/{job_id}/results")
    def start(self) -> BatchJob:
        """
        Start this batch job.

        :return: Started batch job

        .. versionadded:: 0.20.0
            This method was previously called :py:meth:`start_job`.
        """
        self.connection.post(f"/jobs/{self.job_id}/results", expected_status=202)
        return self

    start_job = legacy_alias(start, name="start_job", since="0.20.0", mode="soft")

    @openeo_endpoint("DELETE /jobs/{job_id}/results")
    def stop(self):
        """
        Stop this batch job.

        .. versionadded:: 0.20.0
            This method was previously called :py:meth:`stop_job`.
        """
        self.connection.delete(f"/jobs/{self.job_id}/results", expected_status=204)

    stop_job = legacy_alias(stop, name="stop_job", since="0.20.0", mode="soft")

    def get_results_metadata_url(self, *, full: bool = False) -> str:
        """Get results metadata URL"""
        url = f"/jobs/{self.job_id}/results"
        if full:
            url = self.connection.build_url(url)
        return url

    @deprecated("Use :py:meth:`~BatchJob.get_results` instead.", version="0.4.10")
    def list_results(self) -> dict:
        """Get batch job results metadata."""
        return self.get_results().get_metadata()

    def download_result(self, target: Union[str, Path] = None) -> Path:
        """
        Download single job result to the target file path or into folder (current working dir by default).

        Fails if there are multiple result files.

        :param target: String or path where the file should be downloaded to.
        """
        return self.get_results().download_file(target=target)

    @deprecated(
        "Instead use :py:meth:`BatchJob.get_results` and the more flexible download functionality of :py:class:`JobResults`",
        version="0.4.10")
    def download_results(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        """
        Download all job result files into given folder (current working dir by default).

        The names of the files are taken directly from the backend.

        :param target: String/path, folder where to put the result files.
        :return: file_list: Dict containing the downloaded file path as value and asset metadata
        """
        return self.get_result().download_files(target)

    @deprecated("Use :py:meth:`BatchJob.get_results` instead.", version="0.4.10")
    def get_result(self):
        return _Result(self)

    def get_results(self) -> JobResults:
        """
        Get handle to batch job results for result metadata inspection or downloading resulting assets.

        .. versionadded:: 0.4.10
        """
        return JobResults(job=self)

    def logs(self, offset: Optional[str] = None, level: Union[str, int, None] = None) -> LogsResponse:
        """Retrieve job logs.

        :param offset: The last identifier (property ``id`` of a LogEntry) the client has received.

            If provided, the back-ends only sends the entries that occurred after the specified identifier.
            If not provided or empty, start with the first entry.

            Defaults to None.

        :param level: Minimum log level to retrieve.

            You can use either constants from Python's standard module ``logging``
            or their names (case-insensitive).

            For example:
                ``logging.INFO``, ``"info"`` or ``"INFO"`` can all be used to show the messages
                for level ``logging.INFO`` and above, i.e. also ``logging.WARNING`` and
                ``logging.ERROR`` will be included.

            Default is to show all log levels, in other words ``logging.DEBUG``.
            This is also the result when you explicitly pass log_level=None or log_level="".

        :return: A list containing the log entries for the batch job.
        """
        url = f"/jobs/{self.job_id}/logs"
        params = {}
        if offset is not None:
            params["offset"] = offset
        if level is not None:
            params["level"] = log_level_name(level)
        response_data = self.connection.get(url, params=params, expected_status=200).json()
        return LogsResponse(response_data=response_data, log_level=level, connection=self.connection)

    @deprecated("Use start_and_wait instead", version="0.39.0")
    def run_synchronous(
        self,
        outputfile: Union[str, Path, None] = None,
        print=print,
        max_poll_interval: float = DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
        connection_retry_interval: float = DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
        show_error_logs: bool = True,
    ) -> BatchJob:
        """
        Start the job, wait for it to finish and download result

        :param outputfile: (optional) output path to download to.
        :param print: print/logging function to show progress/status
        :param max_poll_interval: maximum number of seconds to sleep between job status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :param show_error_logs: whether to automatically print error logs when the batch job failed.

        :return: Handle to the job created at the backend.

        .. versionchanged:: 0.37.0
            Added argument ``show_error_logs``.
        """
        self.start_and_wait(
            print=print,
            max_poll_interval=max_poll_interval,
            connection_retry_interval=connection_retry_interval,
            show_error_logs=show_error_logs,
        )
        # TODO #135 support multi file result sets too?
        if outputfile is not None:
            self.download_result(outputfile)
        return self

    def start_and_wait(
        self,
        *,
        print=print,
        max_poll_interval: float = DEFAULT_JOB_STATUS_POLL_INTERVAL_MAX,
        connection_retry_interval: float = DEFAULT_JOB_STATUS_POLL_CONNECTION_RETRY_INTERVAL,
        soft_error_max: int = DEFAULT_JOB_STATUS_POLL_SOFT_ERROR_MAX,
        show_error_logs: bool = True,
        require_success: bool = True,
    ) -> BatchJob:
        """
        Start the batch job, poll its status and wait till it finishes (or fails)

        :param print: print/logging function to show progress/status
        :param max_poll_interval: maximum number of seconds to sleep between job status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :param soft_error_max: maximum number of soft errors (e.g. temporary connection glitches) to allow
        :param show_error_logs: whether to automatically print error logs when the batch job failed.
        :param require_success: whether to raise an exception if the job did not finish successfully.

        :return: Handle to the job created at the backend.

        .. versionchanged:: 0.37.0
            Added argument ``show_error_logs``.

        .. versionchanged:: 0.42.0
            All arguments must be specified as keyword arguments,
            to eliminate the risk of positional mix-ups between heterogeneous arguments and flags.

        .. versionchanged:: 0.42.0
            Added argument ``require_success``.
        """
        # TODO rename `connection_retry_interval` to something more generic?
        start_time = time.time()

        def elapsed() -> str:
            return str(datetime.timedelta(seconds=time.time() - start_time)).rsplit(".")[0]

        def print_status(msg: str):
            print("{t} Job {i!r}: {m}".format(t=elapsed(), i=self.job_id, m=msg))

        # TODO: make `max_poll_interval`, `connection_retry_interval` class constants or instance properties?
        print_status("send 'start'")
        self.start()

        # TODO: also add  `wait` method so you can track a job that already has started explicitly
        #   or just rename this method to `wait` and automatically do start if not started yet?

        # Start with fast polling.
        poll_interval = min(5, max_poll_interval)
        status = None
        _soft_error_count = 0

        def soft_error(message: str):
            """Non breaking error (unless we had too much of them)"""
            nonlocal _soft_error_count
            _soft_error_count += 1
            if _soft_error_count > soft_error_max:
                raise OpenEoClientException("Excessive soft errors")
            print_status(message)
            time.sleep(connection_retry_interval)

        while True:
            # TODO: also allow a hard time limit on this infinite poll loop?
            try:
                job_info = self.describe()
            except requests.ConnectionError as e:
                soft_error("Connection error while polling job status: {e}".format(e=e))
                continue
            except OpenEoApiPlainError as e:
                if e.http_status_code in [HTTP_502_BAD_GATEWAY, HTTP_503_SERVICE_UNAVAILABLE]:
                    soft_error("Service availability error while polling job status: {e}".format(e=e))
                    continue
                else:
                    raise

            status = job_info.get("status", "N/A")

            progress = job_info.get("progress")
            if isinstance(progress, int):
                progress = f"{progress:d}%"
            elif isinstance(progress, float):
                progress = f"{progress:.1f}%"
            else:
                progress = "N/A"
            print_status(f"{status} (progress {progress})")
            if status not in ('submitted', 'created', 'queued', 'running'):
                break

            # Sleep for next poll (and adaptively make polling less frequent)
            time.sleep(poll_interval)
            poll_interval = min(1.25 * poll_interval, max_poll_interval)

        if require_success and status != "finished":
            # TODO: render logs jupyter-aware in a notebook context?
            if show_error_logs:
                print(f"Your batch job {self.job_id!r} failed. Error logs:")
                print(self.logs(level=logging.ERROR))
                print(
                    f"Full logs can be inspected in an openEO (web) editor or with `connection.job({self.job_id!r}).logs()`."
                )
            raise JobFailedException(
                f"Batch job {self.job_id!r} didn't finish successfully. Status: {status} (after {elapsed()}).",
                job=self,
            )

        return self


@deprecated(reason="Use :py:class:`BatchJob` instead", version="0.11.0")
class RESTJob(BatchJob):
    """
    Legacy alias for :py:class:`BatchJob`.
    """


class ResultAsset:
    """
    Result asset of a batch job (e.g. a GeoTIFF or JSON file)

    .. versionadded:: 0.4.10
    """

    def __init__(self, job: BatchJob, name: str, href: str, metadata: dict):
        self.job = job

        self.name = name
        """Asset name as advertised by the backend."""

        self.href = href
        """Download URL of the asset."""

        self.metadata = metadata
        """Asset metadata provided by the backend, possibly containing keys "type" (for media type), "roles", "title", "description"."""

    def __repr__(self):
        return "<ResultAsset {n!r} (type {t}) at {h!r}>".format(
            n=self.name, t=self.metadata.get("type", "unknown"), h=self.href
        )

    def download(
        self, target: Optional[Union[Path, str]] = None, *, chunk_size: int = DEFAULT_DOWNLOAD_CHUNK_SIZE, range_size: int=DEFAULT_DOWNLOAD_RANGE_SIZE
    ) -> Path:
        """
        Download asset to given location

        :param target: download target path. Can be an existing folder
            (in which case the filename advertised by backend will be used)
            or full file name. By default, the working directory will be used.
        :param chunk_size: chunk size for streaming response.
        """
        target = Path(target or Path.cwd())
        if target.is_dir():
            target = target / self.name
        ensure_dir(target.parent)
        logger.info("Downloading Job result asset {n!r} from {h!s} to {t!s}".format(n=self.name, h=self.href, t=target))
        self._download_to_file(url=self.href, target=target, chunk_size=chunk_size, range_size=range_size)
        return target

    def _get_response(self, stream=True) -> requests.Response:
        return self.job.connection.get(self.href, stream=stream)

    def load_json(self) -> dict:
        """Load asset in memory and parse as JSON."""
        if not (self.name.lower().endswith(".json") or self.metadata.get("type") == "application/json"):
            logger.warning("Asset might not be JSON")
        return self._get_response().json()

    def load_bytes(self) -> bytes:
        """Load asset in memory as raw bytes."""
        return self._get_response().content

    # TODO: more `load` methods e.g.: load GTiff asset directly as numpy array


    def _download_to_file(self, url: str, target: Path, *, chunk_size: int=DEFAULT_DOWNLOAD_CHUNK_SIZE, range_size: int=DEFAULT_DOWNLOAD_RANGE_SIZE):
        head = self.job.connection.head(url, stream=True)
        if head.ok and head.headers.get("Accept-Ranges") == "bytes" and 'Content-Length' in head.headers:
            file_size = int(head.headers['Content-Length'])
            self._download_ranged(url=url, target=target, file_size=file_size, chunk_size=chunk_size, range_size=range_size)
        else:
            self._download_all_at_once(url=url, target=target, chunk_size=chunk_size)


    def _download_ranged(self, url: str, target: Path, file_size: int, *, chunk_size: int=DEFAULT_DOWNLOAD_CHUNK_SIZE, range_size: int=DEFAULT_DOWNLOAD_RANGE_SIZE):
        with target.open('wb') as f:
            for from_byte_index in range(0, file_size, range_size):
                to_byte_index = min(from_byte_index + range_size - 1, file_size - 1)
                tries_left = MAX_RETRIES_PER_RANGE
                while tries_left > 0:
                    try:
                        range_headers = {"Range": f"bytes={from_byte_index}-{to_byte_index}"}
                        with self.job.connection.get(path=url, headers=range_headers, stream=True) as r:
                            r.raise_for_status()
                            for block in r.iter_content(chunk_size=chunk_size):
                                f.write(block)
                        break
                    except OpenEoApiPlainError as error:
                        tries_left -= 1
                        if tries_left > 0 and error.http_status_code in RETRIABLE_STATUSCODES:
                            logger.warning(f"Failed to retrieve chunk {from_byte_index}-{to_byte_index} from {url} (status {error.http_status_code}) - retrying")
                            continue
                        else:
                            raise error


    def _download_all_at_once(self, url: str, target: Path, *, chunk_size: int=DEFAULT_DOWNLOAD_CHUNK_SIZE):
        with self.job.connection.get(path=url, stream=True) as r:
            r.raise_for_status()
            with target.open("wb") as f:
                for block in r.iter_content(chunk_size=chunk_size):
                    f.write(block)


class MultipleAssetException(OpenEoClientException):
    pass


class JobResults:
    """
    Results of a batch job: listing of one or more output files (assets)
    and some metadata.

    .. versionadded:: 0.4.10
    """

    def __init__(self, job: BatchJob):
        self._job = job
        self._results = None

    def __repr__(self):
        return "<JobResults for job {j!r}>".format(j=self._job.job_id)

    def get_job_id(self) -> str:
        return self._job.job_id

    def _repr_html_(self):
        try:
            response = self.get_metadata()
            return render_component("batch-job-result", data = response)
        except OpenEoApiError as error:
            return render_error(error)

    def get_metadata(self, force=False) -> dict:
        """Get batch job results metadata (parsed JSON)"""
        if self._results is None or force:
            self._results = self._job.connection.get(
                self._job.get_results_metadata_url(), expected_status=200
            ).json()
        return self._results

    # TODO: provide methods for `stac_version`, `id`, `geometry`, `properties`, `links`, ...?

    def get_assets(self) -> List[ResultAsset]:
        """
        Get all assets from the job results.
        """
        # TODO: add arguments to filter on metadata, e.g. to only get assets of type "image/tiff"
        metadata = self.get_metadata()
        # API 1.0 style: dictionary mapping filenames to metadata dict (with at least a "href" field)
        assets = metadata.get("assets", {})
        if not assets:
            logger.warning("No assets found in job result metadata.")
        return [
            ResultAsset(job=self._job, name=name, href=asset["href"], metadata=asset)
            for name, asset in assets.items()
        ]

    def get_asset(self, name: str = None) -> ResultAsset:
        """
        Get single asset by name or without name if there is only one.
        """
        # TODO: also support getting a single asset by type or role?
        assets = self.get_assets()
        if len(assets) == 0:
            raise OpenEoClientException("No assets in result.")
        if name is None:
            if len(assets) == 1:
                return assets[0]
            else:
                raise MultipleAssetException("Multiple result assets for job {j}: {a}".format(
                    j=self._job.job_id, a=[a.name for a in assets]
                ))
        else:
            try:
                return next(a for a in assets if a.name == name)
            except StopIteration:
                raise OpenEoClientException(
                    "No asset {n!r} in: {a}".format(n=name, a=[a.name for a in assets])
                )

    def download_file(self, target: Union[Path, str] = None, name: str = None, *, chunk_size=DEFAULT_DOWNLOAD_CHUNK_SIZE, range_size: int=DEFAULT_DOWNLOAD_RANGE_SIZE) -> Path:
        """
        Download single asset. Can be used when there is only one asset in the
        :py:class:`JobResults`, or when the desired asset name is given explicitly.

        :param target: path to download to. Can be an existing directory
            (in which case the filename advertised by backend will be used)
            or full file name. By default, the working directory will be used.
        :param name: asset name to download (not required when there is only one asset)
        :return: path of downloaded asset
        """
        try:
            return self.get_asset(name=name).download(target=target, chunk_size=chunk_size, range_size=range_size)
        except MultipleAssetException:
            raise OpenEoClientException(
                "Can not use `download_file` with multiple assets. Use `download_files` instead.")

    def download_files(self, target: Union[Path, str] = None, include_stac_metadata: bool = True, chunk_size=DEFAULT_DOWNLOAD_CHUNK_SIZE) -> List[Path]:
        """
        Download all assets to given folder.

        :param target: path to folder to download to (must be a folder if it already exists)
        :param include_stac_metadata: whether to download the job result metadata as a STAC (JSON) file.
        :return: list of paths to the downloaded assets.
        """
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException(f"Target argument {target} exists but isn't a folder.")
        ensure_dir(target)

        downloaded = [a.download(target, chunk_size=chunk_size) for a in self.get_assets()]

        if include_stac_metadata:
            # TODO #184: convention for metadata file name?
            metadata_file = target / DEFAULT_JOB_RESULTS_FILENAME
            # TODO #184: rewrite references to locally downloaded assets?
            metadata_file.write_text(json.dumps(self.get_metadata()))
            downloaded.append(metadata_file)

        return downloaded


@deprecated(reason="Use :py:class:`JobResults` instead", version="0.4.10")
class _Result:
    """
    Wrapper around `JobResults` to adapt old deprecated "Result" API.

    .. deprecated:: 0.4.10
    """

    # TODO: deprecated: remove this

    def __init__(self, job):
        self.results = JobResults(job=job)

    def download_file(self, target: Union[str, Path] = None) -> Path:
        return self.results.download_file(target=target)

    def download_files(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException(f"Target argument {target} exists but isn't a folder.")
        return {a.download(target): a.metadata for a in self.results.get_assets()}

    def load_json(self) -> dict:
        return self.results.get_asset().load_json()

    def load_bytes(self) -> bytes:
        return self.results.get_asset().load_bytes()
