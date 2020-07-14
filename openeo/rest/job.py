import datetime
import logging
import time
import typing
import urllib.request
from pathlib import Path
from typing import List, Union, Dict

from deprecated import deprecated
from requests import ConnectionError

from openeo.job import Job, JobResult, JobLogEntry
from openeo.rest import OpenEoClientException, JobFailedException
from openeo.util import ensure_dir

if hasattr(typing, 'TYPE_CHECKING') and typing.TYPE_CHECKING:
    # Only import this for type hinting purposes. Runtime import causes circular dependency issues.
    # Note: the `hasattr` check is necessary for Python versions before 3.5.2.
    from openeo.rest.connection import Connection

logger = logging.getLogger(__name__)


class RESTJobResult(JobResult):
    def __init__(self, url):
        self.url = url

    def save_as(self, target_file):
        urllib.request.urlretrieve(self.url, target_file)


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

    def list_results(self, type=None):
        """ Get document with download links."""
        # GET /jobs/{job_id}/results
        raise NotImplementedError

    def _download_get_assets(self) -> Dict[str, dict]:
        results_url = "/jobs/{}/results".format(self.job_id)
        response = self.connection.get(results_url, expected_status=200).json()
        if "assets" in response:
            # API 1.0 style: dictionary mapping filenames to metadata dict (with at least a "href" field)
            return response["assets"]
        else:
            # Best effort translation of on old style to "assets" style (#134)
            return {a["href"].split("/")[-1]: a for a in response["links"]}

    def _download_url(self, url: str, path: Path):
        ensure_dir(path.parent)
        with path.open('wb') as handle:
            # TODO: finetune download parameters/chunking?
            response = self.connection.get(url, stream=True)
            for block in response.iter_content(1024):
                if not block:
                    break
                handle.write(block)
        return path

    def download_result(self, target: Union[str, Path] = None) -> Path:
        """
        Download single job result to the target file path or into folder (current working dir by default).
        
        Fails if there are multiple result files.

        :param target: String or path where the file should be downloaded to.
        """
        assets = self._download_get_assets()
        if len(assets) != 1:
            raise OpenEoClientException(
                "Expected one result file to download, but got {c}: {u!r}".format(c=len(assets), u=assets))
        filename, metadata = assets.popitem()
        url = metadata["href"]

        target = Path(target or Path.cwd())
        if target.is_dir():
            target = target / filename

        self._download_url(url, target)
        return target

    def download_results(self, target: Union[str, Path] = None) -> Dict[Path, dict]:
        """
        Download job results into given folder (current working dir by default).

        The names of the files are taken directly from the backend.

        :param target: String/path, folder where to put the result files.
        :return: file_list: Dict containing the downloaded file path as value and asset metadata
        """
        target = Path(target or Path.cwd())
        if target.exists() and not target.is_dir():
            raise OpenEoClientException("The target argument must be a folder. Got {t!r}".format(t=str(target)))

        assets = {target / f: m for (f, m) in self._download_get_assets().items()}
        if len(assets) == 0:
            raise OpenEoClientException("Expected at least one result file to download, but got 0.")

        for path, metadata in assets.items():
            self._download_url(metadata["href"], path)

        return assets

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

    @deprecated
    def results(self) -> List[RESTJobResult]:
        """ Returns this job's results. """
        return [RESTJobResult(link['href']) for link in self.connection.job_results(self.job_id)['links']]

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
