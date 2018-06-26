from abc import ABC


class Job(ABC):
    """Represents the result of creating a new Job out of a process graph. Jobs are stored in the
    backend and can be executed directly (in batch), or evaluated lazily."""

    def __init__(self, job_id: str):
        self.job_id = job_id

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