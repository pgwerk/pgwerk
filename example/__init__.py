"""
Shared app instance used by both producer and worker.

Set PGWERK_DSN to point at your Postgres database, e.g.:
    export PGWERK_DSN="postgresql://user:pass@localhost/mydb"
"""

import os

from tests import Wrk


DSN = os.environ.get("PGWERK_DSN", "postgresql://werk:wrk@localhost/wrk")

app = Wrk(DSN)
