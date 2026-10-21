"""Litestar + async SQLAlchemy: enqueue a job inside the request's transaction.

Guarantee: **same transaction** — identical mechanism to the FastAPI example.
We reach through SQLAlchemy's async session to its raw psycopg ``AsyncConnection``
and pass it to ``enqueue(_conn=...)``, so the job rides the request's unit of
work and commits (or rolls back) atomically with the user row.

Run:
    export PGWERK_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk"
    uv run litestar --app example.integrations.litestar_app:app run
    # POST /signup  {"email": "a@b.com"}
"""

from __future__ import annotations

import os

from litestar import Litestar
from litestar import post
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from pgwerk import Werk
from example.integrations.tasks import send_welcome_email

DSN = os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk")
engine = create_async_engine(DSN.replace("postgresql://", "postgresql+psycopg://", 1))
Session = async_sessionmaker(engine, expire_on_commit=False)

werk = Werk(DSN)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "demo_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String, unique=True)


async def on_startup() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await werk.connect()


async def on_shutdown() -> None:
    await werk.disconnect()
    await engine.dispose()


@post("/signup")
async def signup(data: dict) -> dict:
    async with Session() as session:
        user = User(email=data["email"])
        session.add(user)
        await session.flush()

        sa_conn = await session.connection()
        raw = await sa_conn.get_raw_connection()
        pg_conn = raw.driver_connection  # psycopg.AsyncConnection

        await werk.enqueue(send_welcome_email, user_id=user.id, email=user.email, _conn=pg_conn)

        await session.commit()  # user row AND job commit together
        return {"user_id": user.id}


app = Litestar([signup], on_startup=[on_startup], on_shutdown=[on_shutdown])
