from openeo.rest.rest_session import RESTSession
from ..job import Job


class ClientJob(Job):

    def __init__(self, job_id: str, session:RESTSession):
        super().__init__(job_id)
        self.session = session

    def download(self, outputfile:str, outputformat=None):
        return self.session.download_job(self.job_id, outputfile, outputformat)

    def status(self):
        return self.session.job_status(self.job_id)

    def queue(self):
        return self.session.queue_job(self.job_id)
