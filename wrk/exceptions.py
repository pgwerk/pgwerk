class WrkError(Exception):
    """Base error for wrk."""


class JobNotFound(WrkError):
    """Job ID does not exist in the database."""


class JobTimeout(WrkError):
    """Job exceeded its configured timeout."""


class WorkerShutdown(WrkError):
    """Worker received a shutdown signal."""


class DependencyFailed(WrkError):
    """A job dependency failed, blocking this job from running."""


class JobError(WrkError):
    """Raised by apply/map when a job fails or is aborted."""

    def __init__(self, job: object) -> None:
        super().__init__(f"Job {getattr(job, 'id', '?')} {getattr(job, 'status', '?')}: {getattr(job, 'error', None)}")
        self.job = job
