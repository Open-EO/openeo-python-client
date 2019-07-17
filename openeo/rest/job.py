from openeo.connection import Connection
from openeo.processgraph import ProcessGraph
from openeo.job import Job, JobResult
from typing import List
import urllib.request
import requests


class RESTJobResult(JobResult):
    def __init__(self, url):
        self.url = url

    def save_as(self, target_file):
        urllib.request.urlretrieve(self.url, target_file)


class RESTJob(Job):

    def __init__(self, job_id: str, connection: Connection):
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
        request = self.connection.post("/jobs/{}/results".format(self.job_id), postdata=None)

        return request.status_code

    def stop_job(self):
        """ Stop / cancel job processing."""
        # DELETE /jobs/{job_id}/results
        request = self.connection.delete("/jobs/{}/results".format(self.job_id))

        return request.status_code

    def list_results(self, type=None):
        """ Get document with download links."""
        # GET /jobs/{job_id}/results
        pass

    def download_results(self, target):
        """ Download job results."""
        # GET /jobs/{job_id}/results > ...

        download_url = "/jobs/{}/results".format(self.job_id)
        r = self.connection.get(download_url, stream = True)

        if r.status_code == 200:

            url = r.json()
            if "links" in url:
                download_url = url["links"][0]
                if "href" in download_url:
                    download_url = download_url["href"]

            auth_header = self.connection.authent.get_header()

            with open(target, 'wb') as handle:
                response = requests.get(download_url, stream=True, headers=auth_header)

                if not response.ok:
                    print (response)

                for block in response.iter_content(1024):

                    if not block:
                        break

                    handle.write(block)
        else:
            raise ConnectionAbortedError(r.text)
        return r.status_code

# TODO: All below methods are deprecated (at least not specified in the coreAPI)
    def download(self, outputfile:str, outputformat=None):
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
