"""
Shared app instance used by both producer and worker.

Set PGWERK_DSN to point at your Postgres database, e.g.:
    export PGWERK_DSN="postgresql://user:pass@localhost/mydb"
"""

import os

from pgwerk import Werk


DSN = os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk")

app = Werk(DSN)
