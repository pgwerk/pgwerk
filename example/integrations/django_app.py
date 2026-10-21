"""Django: enqueue a job only after the request's transaction commits.

Guarantee: **enqueue-on-commit** (not same-transaction). Django's ORM is
synchronous and holds a *sync* psycopg connection, which cannot be handed to
PGWerk's ``async`` enqueue. Pretending otherwise would be a lie, so instead we
register the enqueue with ``transaction.on_commit``: Django runs the callback
only if the transaction actually commits. You never get a job for a user row
that rolled back — you just don't share the literal transaction.

This is a single-file Django app for illustration. Run:
    export PGWERK_DSN="postgresql://pgwerk:pgwerk@localhost/pgwerk"
    uv run python example/integrations/django_app.py migrate
    uv run python example/integrations/django_app.py runserver
    # POST /signup  {"email": "a@b.com"}
"""

from __future__ import annotations

import os
import sys
import json
import asyncio

import django

from django.db import models
from django.db import transaction
from django.urls import path
from django.http import JsonResponse
from django.conf import settings

DSN = os.environ.get("PGWERK_DSN", "postgresql://pgwerk:pgwerk@localhost/pgwerk")

if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="demo-only",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth", __name__],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": DSN.rsplit("/", 1)[-1],
                "USER": "pgwerk",
                "PASSWORD": "pgwerk",
                "HOST": "localhost",
            }
        },
    )
    django.setup()

from pgwerk import Werk  # noqa: E402  (must follow settings.configure)
from example.integrations.tasks import send_welcome_email  # noqa: E402

werk = Werk(DSN)


class User(models.Model):
    email = models.EmailField(unique=True)

    class Meta:
        app_label = __name__


def _enqueue_welcome(user_id: int, email: str) -> None:
    """Sync bridge: PGWerk uses its own pooled async connection here.

    Because this runs from ``on_commit``, the user row is already durably
    committed, so a brand-new connection enqueuing the job is perfectly safe.
    """
    asyncio.run(_enqueue(user_id, email))


async def _enqueue(user_id: int, email: str) -> None:
    async with werk:
        await werk.enqueue(send_welcome_email, user_id=user_id, email=email)


@transaction.atomic
def signup(request):
    body = json.loads(request.body)
    user = User.objects.create(email=body["email"])
    # Scheduled, not executed: fires only if this @atomic block commits.
    transaction.on_commit(lambda: _enqueue_welcome(user.id, user.email))
    return JsonResponse({"user_id": user.id})


urlpatterns = [path("signup", signup)]

if __name__ == "__main__":
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
