import os
import datetime
import logging
import pathlib
import time
import typing
import urllib.request
from typing import List, Union

from openeo.job import Job, JobResult, JobLogEntry
from openeo.rest import OpenEoClientException, JobFailedException
from requests import ConnectionError

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
        request = self.connection.get("/jobs/{}".format(self.job_id))
        return self.connection.parse_json_response(request)

    def update_job(self, process_graph=None, output_format=None,
                   output_parameters=None, title=None, description=None,
                   plan=None, budget=None, additional=None):
        """ Update a job."""
        # PATCH /jobs/{job_id}
        pass

    def delete_job(self):
        """ Delete a job."""
        # DELETE /jobs/{job_id}
        request = self.connection.delete("/jobs/{}".format(self.job_id))

        return request.status_code

    def estimate_job(self):
        """ Calculate an time/cost estimate for a job."""
        # GET /jobs/{job_id}/estimate
        request = self.connection.get("/jobs/{}/estimate".format(self.job_id))

        return self.connection.parse_json_response(request)

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
        pass

    def download_result(self, target: Union[str, pathlib.Path]):
        """
        Download job results to the target file path.
        Fails if there are multiple files or if target is a folder (if you need that, use "download_results" instead).

        :param target: String or path where the file should be downloaded to.
        """

        if os.path.isdir(target):
            raise OpenEoClientException(
                "The target argument must not be a folder, but a file path. Got ({})".format(str(target)))

        results_url = "/jobs/{}/results".format(self.job_id)
        r = self.connection.get(results_url, expected_status=200)

        if self.connection._api_version.at_least("1.0.0"):
            links = r.json()["assets"]
            if len(links) != 1:
                raise OpenEoClientException(
                    "Expected one result file to download, but got {c}".format(c=len(links)))

            _, result_href = links.popitem()
            result_href = result_href["href"]
        else:
            links = r.json()["links"]
            if len(links) != 1:
                raise OpenEoClientException(
                    "Expected one result file to download, but got {c}".format(c=len(links)))
            result_href = links[0]["href"]

        target = pathlib.Path(target)
        with target.open('wb') as handle:
            response = self.connection.get(result_href, stream=True)
            for block in response.iter_content(1024):
                if not block:
                    break
                handle.write(block)

    def download_results(self, target: Union[str, pathlib.Path]=None) -> dict:
        """
        Download job results to the target folder path. The names of the files are taken directly from the backend.
        target is not set: it stores the result files at the execution path.
        target is set to a folder path: All download files will be downloaded into this path.

        :param target: String path, where to put the result files.
        :return: file_list: Dict containing the downloaded file path as value and the href of the file as key.
        """

        results_url = "/jobs/{}/results".format(self.job_id)
        r = self.connection.get(results_url, expected_status=200)

        download_dict = {}
        target = pathlib.Path(target or pathlib.Path.cwd())
        if not os.path.isdir(target):
            raise OpenEoClientException(
                "The target argument has to be an existing folder. Got ({})".format(str(target)))

        if self.connection._api_version.at_least("1.0.0"):
            links = r.json()["assets"]
            for key, val in links.items():
                download_dict[val["href"]] = os.path.join(target, key)
        else:
            links = r.json()["links"]
            for link in links:
                download_dict[link["href"]] = os.path.join(target, link["href"].split("/")[-1])

        if len(download_dict) == 0:
            raise OpenEoClientException("Expected at least one result file to download, but got 0.")

        for href, file in download_dict.items():
            target = pathlib.Path(file)
            with target.open('wb') as handle:
                response = self.connection.get(href, stream=True)
                for block in response.iter_content(1024):
                    if not block:
                        break
                    handle.write(block)
        return download_dict

    # TODO: All below methods are deprecated (at least not specified in the coreAPI)
    def download(self, outputfile: str, outputformat=None):
        """ Download the result as a raster."""
        try:
            return self.connection.download_job(self.job_id, outputfile, outputformat)
        except ConnectionAbortedError as e:
            return print(str(e))

    def status(self):
        """ Returns the status of the job."""
        return self.connection.job_info(self.job_id)['status']

    def queue(self):
        """ Queues the job. """
        return self.connection.queue_job(self.job_id)

    def results(self) -> List[RESTJobResult]:
        """ Returns this job's results. """
        return [RESTJobResult(link['href']) for link in self.connection.job_results(self.job_id)['links']]

    """ Retrieve job logs."""
    def logs(self, offset=None) -> List[JobLogEntry]:
        return[JobLogEntry(log_entry['id'], log_entry['level'], log_entry['message'])
               for log_entry in self.connection.job_logs(self.job_id, offset)['logs']]

    @classmethod
    def run_synchronous(cls, job, outputfile: Union[str, pathlib.Path],
                        print=print, max_poll_interval=60, connection_retry_interval=30):
        job.start_job()

        job_id = job.job_id
        status = None
        poll_interval = min(5, max_poll_interval)
        start_time = time.time()
        while True:
            # TODO: also allow a hard time limit on this infinite poll loop?
            elapsed = str(datetime.timedelta(seconds=time.time() - start_time))
            try:
                job_info = job.describe_job()
            except ConnectionError as e:
                print("{t} Connection error while querying job status: {e}".format(t=elapsed, e=e))
                time.sleep(connection_retry_interval)
                continue

            status = job_info.get("status", "N/A")
            print("{t} Job {i}: {s} (progress {p})".format(
                t=elapsed, i=job_id, s=status,
                p='{p}%'.format(p=job_info["progress"]) if "progress" in job_info else "N/A"
            ))
            if status not in ('submitted', 'created', 'queued', 'running'):
                break

            time.sleep(poll_interval)
            poll_interval = min(1.25 * poll_interval, max_poll_interval)

        elapsed = str(datetime.timedelta(seconds=time.time() - start_time))
        if status == 'finished':
            job.download_result(outputfile)
        else:
            raise JobFailedException("Batch job {i} didn't finish properly. Status: {s} (after {t}).".format(
                i=job_id, s=status, t=elapsed
            ), job)

        return job
