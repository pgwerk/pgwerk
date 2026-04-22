# Exceptions

All `werk` exceptions inherit from `WrkError`.

## WrkError

```python
class WrkError(Exception): ...
```

Base exception for all wrk errors.

---

## JobNotFound

```python
class JobNotFound(WrkError): ...
```

Raised by `app.get_job()` when no job with the given ID exists in the database.

```python
from pgwerk import JobNotFound

try:
    job = await app.get_job("nonexistent-id")
except JobNotFound:
    print("Job not found")
```

---

## JobTimeout

```python
class JobTimeout(WrkError): ...
```

Raised inside a job handler when the job exceeds its configured `_timeout`.

---

## JobError

```python
class JobError(WrkError):
    job: Job
```

Raised by `app.apply()` and `app.map()` when a job reaches a failed or aborted terminal state. The `job` attribute holds the terminal `Job` object for inspection.

```python
from pgwerk import JobError

try:
    result = await app.apply(risky_func, timeout=30)
except JobError as exc:
    print(f"Job {exc.job.id} failed: {exc.job.error}")
```

---

## DependencyFailed

```python
class DependencyFailed(WrkError): ...
```

Raised when a job cannot start because one of its dependencies failed and `allow_failure` was not set on the `Dependency`.

---

## WorkerShutdown

```python
class WorkerShutdown(WrkError): ...
```

Internal exception signalling that the worker received a shutdown signal. Not normally raised to application code.
