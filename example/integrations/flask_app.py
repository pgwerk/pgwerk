"""Flask + Flask-SQLAlchemy: enqueue a job after the transaction commits.

Guarantee: **enqueue-on-commit** (not same-transaction). Flask-SQLAlchemy is
synchronous, so its psycopg connection can't be passed to PGWerk's ``async``
enqueue. We hook SQLAlchemy's ``after_commit`` event instead: the job is sent
only once the session has durably committed, so there is never a job for a row
that rolled back.

Run:
    export PGWERK_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk"
    uv run python example/integrations/flask_app.py
    # POST /signup  {"email": "a@b.com"}
"""

from __future__ import annotations

import os
import asyncio

from flask import Flask
from flask import jsonify
from flask import request
from sqlalchemy import event
from flask_sqlalchemy import SQLAlchemy

from pgwerk import Werk
from example.integrations.tasks import send_welcome_email

DSN = os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DSN.replace("postgresql://", "postgresql+psycopg://", 1)
db = SQLAlchemy(app)

werk = Werk(DSN)


class User(db.Model):
    __tablename__ = "demo_users"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, unique=True)


def _enqueue_welcome(user_id: int, email: str) -> None:
    async def _run() -> None:
        async with werk:
            await werk.enqueue(send_welcome_email, user_id=user_id, email=email)

    asyncio.run(_run())


@event.listens_for(db.session, "after_commit")
def _flush_pending_jobs(session) -> None:
    # Drain jobs staged on the session during the request; fires post-commit.
    for user_id, email in getattr(session, "_pending_welcome", []):
        _enqueue_welcome(user_id, email)
    session._pending_welcome = []


@app.post("/signup")
def signup():
    user = User(email=request.json["email"])
    db.session.add(user)
    db.session.flush()  # assign user.id
    # Stage the enqueue; the after_commit listener sends it once committed.
    pending = getattr(db.session, "_pending_welcome", [])
    pending.append((user.id, user.email))
    db.session._pending_welcome = pending
    db.session.commit()
    return jsonify({"user_id": user.id})


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        asyncio.run(werk.connect())  # create the PGWerk schema once
    app.run(debug=True)
