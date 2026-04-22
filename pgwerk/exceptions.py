class WerkError(Exception):
    """Base error for pgwerk."""


class JobNotFound(WerkError):
    """Job ID does not exist in the database."""


class JobTimeout(WerkError):
    """Job exceeded its configured timeout."""


class WorkerShutdown(WerkError):
    """Worker received a shutdown signal."""


class DependencyFailed(WerkError):
    """A job dependency failed, blocking this job from running."""


class JobError(WerkError):
    """Raised by apply/map when a job fails or is aborted."""

    def __init__(self, job: object) -> None:
        super().__init__(f"Job {getattr(job, 'id', '?')} {getattr(job, 'status', '?')}: {getattr(job, 'error', None)}")
        self.job = job
