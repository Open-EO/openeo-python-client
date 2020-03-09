from abc import ABC

from typing import List


class JobResult:
    def save_as(self, target_file) -> None:
        pass


class JobLogEntry:
    def __init__(self, log_id, level, message):
        self.log_id = log_id
        self.level = level
        self.message = message


class Job(ABC):
    """Represents the result of creating a new Job out of a process graph. Jobs are stored in the
    backend and can be executed directly (in batch), or evaluated lazily."""

    def __init__(self, job_id: str):
        self.job_id = job_id

    def describe_job(self):
        """ Get all job information."""
        # GET /jobs/{job_id}
        pass

    def update_job(self, process_graph=None, output_format=None,
                   output_parameters=None, title=None, description=None,
                   plan=None, budget=None, additional=None):
        """ Update a job."""
        # PATCH /jobs/{job_id}
        pass

    def delete_job(self):
        """ Delete a job."""
        # DELETE /jobs/{job_id}
        pass

    def estimate_job(self):
        """ Calculate an time/cost estimate for a job."""
        # GET /jobs/{job_id}/estimate
        pass

    def start_job(self):
        """ Start / queue a job for processing."""
        # POST /jobs/{job_id}/results
        pass

    def stop_job(self):
        """ Stop / cancel job processing."""
        # DELETE /jobs/{job_id}/results
        pass

    def list_results(self, type=None):
        """ Get document with download links."""
        # GET /jobs/{job_id}/results
        pass

    def download_results(self, target):
        """ Download job results."""
        # GET /jobs/{job_id}/results > ...
        pass

    def logs(self, offset=None) -> List[JobLogEntry]:
        """ Retrieve job logs."""
        pass

# TODO: All below methods are depricated (at least not specified in the coreAPI)
    def download(self, outputfile: str, outputformat: str):
        """ Download the result as a raster."""
        pass

    # TODO: Maybe add a job status class.
    def status(self):
        """ Returns the status of the job."""
        pass

    def queue(self):
        """ Queues the job. """
        pass

    def results(self) -> List[JobResult]:
        pass

    def __repr__(self):
        return '<{c} job_id={i!r}>'.format(c=self.__class__.__name__, i=self.job_id)