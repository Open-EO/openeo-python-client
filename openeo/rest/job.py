import datetime
import logging
import time
import typing
from pathlib import Path
from typing import List, Union, Dict, Optional

from deprecated import deprecated
from requests import ConnectionError, Response

from openeo.job import Job, JobLogEntry
from openeo.rest import OpenEoClientException, JobFailedException
from openeo.util import ensure_dir

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection

logger = logging.getLogger(__name__)


class RESTJob(Job):

    def __init__(self, job_id: str, connection: 'Connection'):
        super().__init__(job_id)
        self.connection = connection

    def describe_job(self):
        """ Get all job information."""
        # GET /jobs/{job_id}
        return self.connection.get("/jobs/{}".format(self.job_id)).json()

    def update_job(self, process_graph=None, output_format=None,
                   output_parameters=None, title=None, description=None,
                   plan=None, budget=None, additional=None):
        """ Update a job."""
        # PATCH /jobs/{job_id}
        raise NotImplementedError

    def delete_job(self):
        """ Delete a job."""
        # DELETE /jobs/{job_id}
        request = self.connection.delete("/jobs/{}".format(self.job_id))
        assert request.status_code == 204

    def estimate_job(self):
        """ Calculate an time/cost estimate for a job."""
        # GET /jobs/{job_id}/estimate
        return self.connection.get("/jobs/{}/estimate".format(self.job_id)).json()

    def start_job(self):
        """ Start / queue a job for processing."""
        # POST /jobs/{job_id}/results
        url = "/jobs/{}/results".format(self.job_id)
        request = self.connection.post(url)
        if request.status_code != 202:
            logger.warning("{u} returned with status code {s} instead of 202".format(u=url, s=request.status_code))

    def stop_job(self):
        """ Stop / cancel job processing."""
        # DELETE /jobs/{job_id}/results
        request = self.connection.delete("/jobs/{}/results".format(self.job_id))

        return request.status_code

    def list_results(self) -> dict:
        """ Get document with download links."""
        return self.connection.get("/jobs/{}/results".format(self.job_id), expected_status=200).json()

    def download_result(self, target: Union[str, Path] = None) -> Path:
        """
        Download single job result to the target file path or into folder (current working dir by default).
        
        Fails if there are multiple result files.

        :param target: String or path where the file should be downloaded to.
        """
        return self.get_results().download_file(target=target)

    @deprecated("Use `get_results().download_files()` instead")
    def download_results(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        """
        Download all job result files into given folder (current working dir by default).

        The names of the files are taken directly from the backend.

        :param target: String/path, folder where to put the result files.
        :return: file_list: Dict containing the downloaded file path as value and asset metadata
        """
        return self.get_result().download_files(target)

    @deprecated("Use `get_results()` instead.")
    def get_result(self):
        return _Result(self)

    def get_results(self) -> "JobResults":
        return JobResults(self)

    @deprecated
    def download(self, outputfile: str, outputformat=None):
        """ Download the result as a raster."""
        try:
            return self.connection.download_job(self.job_id, outputfile, outputformat)
        except ConnectionAbortedError as e:
            return print(str(e))

    def status(self):
        """ Returns the status of the job."""
        return self.describe_job().get("status", "N/A")

    @deprecated
    def queue(self):
        """ Queues the job. """
        return self.connection.queue_job(self.job_id)

    def logs(self, offset=None) -> List[JobLogEntry]:
        """ Retrieve job logs."""
        return [JobLogEntry(log_entry['id'], log_entry['level'], log_entry['message'])
                for log_entry in self.connection.job_logs(self.job_id, offset)['logs']]

    def run_synchronous(self, outputfile: Union[str, Path],
                        print=print, max_poll_interval=60, connection_retry_interval=30) -> 'RESTJob':
        """Start the job, wait for it to finish and download result"""
        self.start_and_wait(
            print=print, max_poll_interval=max_poll_interval, connection_retry_interval=connection_retry_interval
        )
        # TODO #135 support multi file result sets too?
        if outputfile is not None:
            self.download_result(outputfile)
        return self

    def start_and_wait(self, print=print, max_poll_interval: int = 60, connection_retry_interval: int = 30):
        """
        Start the batch job, poll its status and wait till it finishes (or fails)
        :param print: print/logging function to show progress/status
        :param max_poll_interval: maximum number of seconds to sleep between status polls
        :param connection_retry_interval: how long to wait when status poll failed due to connection issue
        :return:
        """
        # TODO: make `max_poll_interval`, `connection_retry_interval` class constants or instance properties?
        self.start_job()
        # Start with fast polling.
        poll_interval = min(5, max_poll_interval)
        status = None
        start_time = time.time()
        while True:
            # TODO: also allow a hard time limit on this infinite poll loop?
            elapsed = str(datetime.timedelta(seconds=time.time() - start_time))
            try:
                job_info = self.describe_job()
            except ConnectionError as e:
                print("{t} Connection error while querying job status: {e}".format(t=elapsed, e=e))
                time.sleep(connection_retry_interval)
                continue

            status = job_info.get("status", "N/A")
            print("{t} Job {i!r}: {s} (progress {p})".format(
                t=elapsed, i=self.job_id, s=status,
                p='{p}%'.format(p=job_info["progress"]) if "progress" in job_info else "N/A"
            ))
            if status not in ('submitted', 'created', 'queued', 'running'):
                break

            # Sleep for next poll (and adaptively make polling less frequent)
            time.sleep(poll_interval)
            poll_interval = min(1.25 * poll_interval, max_poll_interval)

        elapsed = str(datetime.timedelta(seconds=time.time() - start_time))
        if status != "finished":
            raise JobFailedException("Batch job {i} didn't finish properly. Status: {s} (after {t}).".format(
                i=self.job_id, s=status, t=elapsed
            ), job=self)

        return self


class ResultAsset:
    """
    Result asset of a batch job (e.g. a GeoTIFF or JSON file)
    """

    def __init__(self, job: RESTJob, name: str, href: str, metadata: dict):
        self.job = job
        self.name = name
        self.href = href
        self.metadata = metadata

    def download(self, target: Optional[Union[Path, str]] = None, chunk_size=None) -> Path:
        """
        Download asset to given location

        :param target: download target path
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


class JobResults:
    """
    Results of a batch job: listing of output files (URLs) and
    some metadata.
    """

    def __init__(self, job: RESTJob):
        self._job = job
        self._results_url = "/jobs/{j}/results".format(j=self._job.job_id)
        self._results = None

    def get_metadata(self, force=False) -> dict:
        """Get batch job results metadata (parsed JSON)"""
        if self._results is None or force:
            self._results = self._job.connection.get(self._results_url, expected_status=200).json()
        return self._results

    # TODO: provide methods for `stac_version`, `id`, `geometry`, `properties`, `links`, ...?

    def get_assets(self) -> Dict[str, ResultAsset]:
        metadata = self.get_metadata()
        if "assets" in metadata:
            # API 1.0 style: dictionary mapping filenames to metadata dict (with at least a "href" field)
            assets = metadata["assets"]
        else:
            # Best effort translation of on old style to "assets" style (#134)
            assets = {a["href"].split("/")[-1]: a for a in metadata["links"]}
        return {
            name: ResultAsset(job=self._job, name=name, href=asset["href"], metadata=asset)
            for name, asset in assets.items()
        }

    def get_asset(self, name: str = None) -> ResultAsset:
        """Get single asset by name or without name if there is only one."""
        # TODO: also support getting a single asset by type or role?
        assets = self.get_assets()
        if len(assets) == 0:
            raise OpenEoClientException("No assets in result.")
        if name in assets:
            return assets[name]
        elif name is None and len(assets) == 1:
            return assets.popitem()[1]
        else:
            raise OpenEoClientException(
                "Failed to get single asset (name {n!r}) from {a}".format(n=name, a=list(assets.keys()))
            )

    def download_file(self, target: Union[Path, str] = None, name: str = None) -> Path:
        """
        Download single asset.

        :param target: path to download to. Can be an existing directory
            (in which case the filename advertised by backend will be used)
            or full file name. By default, the working directory will be used.
        :param name: asset name to download (not required when there is only one asset)
        :return: path of downloaded asset
        """
        return self.get_asset(name=name).download(target=target)

    def download_files(self, target: Union[Path, str] = None) -> List[Path]:
        """
        Download all assets to given folder.

        :param target: path to folder to download to (must be a folder if it already exists)
        :return: list of paths to the downloaded assets.
        """
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))
        return [a.download(target) for a in self.get_assets().values()]


@deprecated("Use `JobResults` instead")
class _Result:
    """Wrapper around `JobResults` to adapt old deprecated "Result" API."""

    # TODO: deprecated: remove this

    def __init__(self, job):
        self.results = JobResults(job=job)

    def download_file(self, target: Union[str, Path] = None) -> Path:
        return self.results.download_file(target=target)

    def download_files(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))
        return {
            asset.download(target): asset.metadata
            for name, asset in self.results.get_assets().items()
        }

    def load_json(self) -> dict:
        return self.results.get_asset().load_json()

    def load_bytes(self) -> bytes:
        return self.results.get_asset().load_bytes()
