import datetime
import logging
import time
import typing
from pathlib import Path
from typing import List, Union, Dict, Optional

from deprecated.sphinx import deprecated
from requests import ConnectionError, Response

from openeo.api.logs import LogEntry
from openeo.internal.jupyter import render_component, render_error, VisualDict, VisualList
from openeo.rest import OpenEoClientException, JobFailedException, OpenEoApiError
from openeo.util import ensure_dir

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Imports for type checking only (circular import issue at runtime). `hasattr` is Python 3.5 workaround #210
    from openeo.rest.connection import Connection

logger = logging.getLogger(__name__)


class RESTJob:
    """
    Handle for an openEO batch job, allowing it to describe, start, cancel, inspect results, etc.
    """
    # TODO: rename this to BatchJob?

    def __init__(self, job_id: str, connection: 'Connection'):
        self.job_id = job_id
        """Unique identifier of the batch job (string)."""

        self.connection = connection

    def __repr__(self):
        return '<{c} job_id={i!r}>'.format(c=self.__class__.__name__, i=self.job_id)

    def _repr_html_(self):
        data = self.describe_job()
        currency = self.connection.capabilities().currency()
        return render_component('job', data=data, parameters={'currency': currency})

    def describe_job(self) -> dict:
        """ Get all job information."""
        # GET /jobs/{job_id}
        return self.connection.get("/jobs/{}".format(self.job_id), expected_status=200).json()

    def update_job(self, process_graph=None, output_format=None,
                   output_parameters=None, title=None, description=None,
                   plan=None, budget=None, additional=None):
        """ Update a job."""
        # PATCH /jobs/{job_id}
        raise NotImplementedError

    def delete_job(self):
        """ Delete a job."""
        # DELETE /jobs/{job_id}
        self.connection.delete("/jobs/{}".format(self.job_id), expected_status=204)

    def estimate_job(self):
        """ Calculate an time/cost estimate for a job."""
        # GET /jobs/{job_id}/estimate
        data = self.connection.get("/jobs/{}/estimate".format(self.job_id), expected_status=200).json()
        currency = self.connection.capabilities().currency()
        return VisualDict('job-estimate', data=data, parameters={'currency': currency})

    def start_job(self):
        """ Start / queue a job for processing."""
        # POST /jobs/{job_id}/results
        self.connection.post("/jobs/{}/results".format(self.job_id), expected_status=202)

    def stop_job(self):
        """ Stop / cancel job processing."""
        # DELETE /jobs/{job_id}/results
        self.connection.delete("/jobs/{}/results".format(self.job_id), expected_status=204)

    @deprecated("Use :py:meth:`~RESTJOB.get_results` instead.", version="0.4.10")
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
        "Instead use :py:meth:`RESTJob.get_results` and the more flexible download functionality of :py:class:`JobResults`",
        version="0.4.10")
    def download_results(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        """
        Download all job result files into given folder (current working dir by default).

        The names of the files are taken directly from the backend.

        :param target: String/path, folder where to put the result files.
        :return: file_list: Dict containing the downloaded file path as value and asset metadata
        """
        return self.get_result().download_files(target)

    @deprecated("Use :py:meth:`RESTJob.get_results` instead.", version="0.4.10")
    def get_result(self):
        return _Result(self)

    def get_results(self) -> "JobResults":
        """
        Get handle to batch job results for result metadata inspection or downloading resulting assets.

        .. versionadded:: 0.4.10
        """
        return JobResults(self)

    def status(self):
        """ Returns the status of the job."""
        return self.describe_job().get("status", "N/A")

    def logs(self, offset=None) -> List[LogEntry]:
        """ Retrieve job logs."""
        url = "/jobs/{}/logs".format(self.job_id)
        logs = self.connection.get(url, params={'offset': offset}, expected_status=200).json()["logs"]
        entries = [LogEntry(log) for log in logs]
        return VisualList('logs', data=entries)

    def run_synchronous(self, outputfile: Union[str, Path, None] = None,
                        print=print, max_poll_interval=60, connection_retry_interval=30) -> 'RESTJob':
        """Start the job, wait for it to finish and download result"""
        self.start_and_wait(
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )
        # TODO #135 support multi file result sets too?
        if outputfile is not None:
            self.download_result(outputfile)
        return self

    def start_and_wait(self, print=print, max_poll_interval: int = 60, connection_retry_interval: int = 30) -> "RESTJob":
        """
        Start the batch job, poll its status and wait till it finishes (or fails)

        :param print: print/logging function to show progress/status
        :param max_poll_interval: maximum number of seconds to sleep between status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :return:
        """
        start_time = time.time()

        def elapsed() -> str:
            return str(datetime.timedelta(seconds=time.time() - start_time)).rsplit(".")[0]

        def print_status(msg: str):
            print("{t} Job {i!r}: {m}".format(t=elapsed(), i=self.job_id, m=msg))

        # TODO: make `max_poll_interval`, `connection_retry_interval` class constants or instance properties?
        print_status("send 'start'")
        self.start_job()

        # TODO: also add  `wait` method so you can track a job that already has started explicitly
        #   or just rename this method to `wait` and automatically do start if not started yet?

        # Start with fast polling.
        poll_interval = min(5, max_poll_interval)
        status = None
        while True:
            # TODO: also allow a hard time limit on this infinite poll loop?
            try:
                job_info = self.describe_job()
            except ConnectionError as e:
                print_status("Connection error while querying status: {e}".format(e=e))
                time.sleep(connection_retry_interval)
                continue

            status = job_info.get("status", "N/A")
            progress = '{p}%'.format(p=job_info["progress"]) if "progress" in job_info else "N/A"
            print_status("{s} (progress {p})".format(s=status, p=progress))
            if status not in ('submitted', 'created', 'queued', 'running'):
                break

            # Sleep for next poll (and adaptively make polling less frequent)
            time.sleep(poll_interval)
            poll_interval = min(1.25 * poll_interval, max_poll_interval)

        if status != "finished":
            raise JobFailedException("Batch job {i} didn't finish properly. Status: {s} (after {t}).".format(
                i=self.job_id, s=status, t=elapsed()
            ), job=self)

        return self


class ResultAsset:
    """
    Result asset of a batch job (e.g. a GeoTIFF or JSON file)

    .. versionadded:: 0.4.10
    """

    def __init__(self, job: RESTJob, name: str, href: str, metadata: dict):
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

    def download(self, target: Optional[Union[Path, str]] = None, chunk_size=None) -> Path:
        """
        Download asset to given location

        :param target: download target path. Can be an existing folder
            (in which case the filename advertised by backend will be used)
            or full file name. By default, the working directory will be used.
        """
        target = Path(target or Path.cwd())
        if target.is_dir():
            target = target / self.name
        ensure_dir(target.parent)
        logger.info("Downloading Job result asset {n!r} from {h!s} to {t!s}".format(n=self.name, h=self.href, t=target))
        with target.open("wb") as f:
            response = self._get_response(stream=True)
            for block in response.iter_content(chunk_size=chunk_size):
                f.write(block)
        return target

    def _get_response(self, stream=True) -> Response:
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


class MultipleAssetException(OpenEoClientException):
    pass


class JobResults:
    """
    Results of a batch job: listing of one or more output files (assets)
    and some metadata.

    .. versionadded:: 0.4.10
    """

    def __init__(self, job: RESTJob):
        self._job = job
        self._results_url = "/jobs/{j}/results".format(j=self._job.job_id)
        self._results = None

    def __repr__(self):
        return "<JobResults for job {j!r}>".format(j=self._job.job_id)

    def _repr_html_(self):
        try:
            response = self.get_metadata()
            return render_component("batch-job-result", data = response)
        except OpenEoApiError as error:
            return render_error(error)

    def get_metadata(self, force=False) -> dict:
        """Get batch job results metadata (parsed JSON)"""
        if self._results is None or force:
            self._results = self._job.connection.get(self._results_url, expected_status=200).json()
        return self._results

    # TODO: provide methods for `stac_version`, `id`, `geometry`, `properties`, `links`, ...?

    def get_assets(self) -> List[ResultAsset]:
        """
        Get all assets from the job results.
        """
        # TODO: add arguments to filter on metadata, e.g. to only get assets of type "image/tiff"
        metadata = self.get_metadata()
        if "assets" in metadata:
            # API 1.0 style: dictionary mapping filenames to metadata dict (with at least a "href" field)
            assets = metadata["assets"]
        else:
            # Best effort translation of on old style to "assets" style (#134)
            assets = {a["href"].split("/")[-1]: a for a in metadata["links"]}
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

    def download_file(self, target: Union[Path, str] = None, name: str = None) -> Path:
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
            return self.get_asset(name=name).download(target=target)
        except MultipleAssetException:
            raise OpenEoClientException(
                "Can not use `download_file` with multiple assets. Use `download_files` instead.")

    def download_files(self, target: Union[Path, str] = None) -> List[Path]:
        """
        Download all assets to given folder.

        :param target: path to folder to download to (must be a folder if it already exists)
        :return: list of paths to the downloaded assets.
        """
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))
        ensure_dir(target)
        return [a.download(target) for a in self.get_assets()]


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
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))
        return {a.download(target): a.metadata for a in self.results.get_assets()}

    def load_json(self) -> dict:
        return self.results.get_asset().load_json()

    def load_bytes(self) -> bytes:
        return self.results.get_asset().load_bytes()
