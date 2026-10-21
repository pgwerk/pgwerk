"""FastAPI + async SQLAlchemy: enqueue a job inside the request's transaction.

Guarantee: **same transaction**. We hand PGWerk the *same* psycopg
``AsyncConnection`` that SQLAlchemy is using for the request, so the job INSERT
is part of the unit of work. If the transaction rolls back, the user row and the
job both vanish — there is no "email sent for a user that was never created".

Run:
    export PGWERK_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk"
    uv run uvicorn example.integrations.fastapi_app:app --reload
    # POST /signup  {"email": "a@b.com"}
"""

from __future__ import annotations

import os

from contextlib import asynccontextmanager

from fastapi import Depends
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from pgwerk import Werk
from example.integrations.tasks import send_welcome_email

DSN = os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk")
# SQLAlchemy must use the psycopg (v3) async driver so its raw connection is a
# psycopg.AsyncConnection — the exact type PGWerk's `_conn` expects.
engine = create_async_engine(DSN.replace("postgresql://", "postgresql+psycopg://", 1))
Session = async_sessionmaker(engine, expire_on_commit=False)

werk = Werk(DSN)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "demo_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String, unique=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await werk.connect()  # creates the PGWerk schema on first run
    yield
    await werk.disconnect()
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


async def get_session() -> AsyncSession:
    async with Session() as session:
        yield session


class SignupIn(BaseModel):
    email: str


@app.post("/signup")
async def signup(body: SignupIn, session: AsyncSession = Depends(get_session)):
    user = User(email=body.email)
    session.add(user)
    await session.flush()  # assigns user.id without committing yet

    # The crux: pull the raw psycopg AsyncConnection out of SQLAlchemy's session
    # and enqueue on it. No commit happens here — it rides the session's tx.
    sa_conn = await session.connection()
    raw = await sa_conn.get_raw_connection()
    pg_conn = raw.driver_connection  # psycopg.AsyncConnection

    await werk.enqueue(send_welcome_email, user_id=user.id, email=user.email, _conn=pg_conn)

    await session.commit()  # user row AND job commit together, atomically
    return {"user_id": user.id}
