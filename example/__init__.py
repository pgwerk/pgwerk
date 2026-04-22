"""
Shared app instance used by both producer and worker.

Set WRK_DSN to point at your Postgres database, e.g.:
    export WRK_DSN="postgresql://user:pass@localhost/mydb"
"""

import os

from pgwerk import Wrk


DSN = os.environ.get("WRK_DSN", "postgresql://wrk:wrk@localhost/wrk")

app = Wrk(DSN)
