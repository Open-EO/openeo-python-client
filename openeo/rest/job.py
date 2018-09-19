from openeo.rest.rest_session import RESTSession
from ..job import Job, JobResult
from typing import List
import urllib.request


class ClientJobResult(JobResult):
    def __init__(self, url):
        self.url = url

    def save_as(self, target_file):
        urllib.request.urlretrieve(self.url, target_file)


class ClientJob(Job):

    def __init__(self, job_id: str, session:RESTSession):
        super().__init__(job_id)
        self.session = session

    def download(self, outputfile:str, outputformat=None):
        """ Download the result as a raster."""
        try:
            return self.session.download_job(self.job_id, outputfile, outputformat)
        except ConnectionAbortedError as e:
            return print(str(e))

    def status(self):
        """ Returns the status of the job."""
        return self.session.job_info(self.job_id)['status']

    def queue(self):
        """ Queues the job. """
        return self.session.queue_job(self.job_id)

    def results(self) -> List[ClientJobResult]:
        """ Returns this job's results. """
        return [ClientJobResult(link['href']) for link in self.session.job_results(self.job_id)['links']]
