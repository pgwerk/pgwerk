"""
Simulates a live application by enqueuing jobs at random intervals indefinitely.
Press Ctrl+C to stop.

    cd example
    python producer.py
"""

import os
import uuid
import random
import signal
import string
import asyncio
import logging

from typing import Callable

from example.tasks import export_report
from example.tasks import process_webhook
from example.tasks import transcode_video
from example.tasks import sync_crm_contact
from example.tasks import generate_thumbnail
from example.tasks import send_invoice_email
from example.tasks import send_welcome_email
from example.tasks import charge_subscription
from example.tasks import send_password_reset
from example.tasks import refresh_search_index
from example.tasks import send_push_notification
from example.tasks import cleanup_expired_sessions

from pgwerk import Werk
from pgwerk import Retry


app = Werk(os.environ.get("PGWERK_DSN", "postgresql://werk:wrk@localhost/wrk"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_stop = asyncio.Event()


def _rand_email(name: str) -> str:
    domains = ["example.com", "acme.io", "testco.dev", "mail.net"]
    return f"{name.lower()}@{random.choice(domains)}"


def _rand_user_id() -> int:
    return random.randint(1000, 9999)


def _rand_order_id() -> str:
    return "ORD-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _rand_asset_id() -> str:
    return str(uuid.uuid4())[:8]


FIRST_NAMES = ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "henry", "iris", "jack"]

REPORT_TYPES = ["monthly_revenue", "user_activity", "churn_analysis", "invoice_summary", "ad_performance"]

PUSH_TITLES = [
    ("Your order has shipped", "Track your package now."),
    ("New message", "You have 1 unread message."),
    ("Payment received", "Thanks! Your invoice has been paid."),
    ("Security alert", "A new login was detected on your account."),
    ("Weekly summary ready", "Check out your activity for this week."),
]

WEBHOOK_SOURCES = ["stripe", "github", "shopify", "twilio", "sendgrid"]
WEBHOOK_EVENTS = {
    "stripe": ["payment_intent.succeeded", "invoice.payment_failed", "customer.subscription.updated"],
    "github": ["push", "pull_request", "issues"],
    "shopify": ["orders/create", "refunds/create", "customers/update"],
    "twilio": ["message.delivered", "call.completed"],
    "sendgrid": ["email.opened", "email.bounced", "email.unsubscribed"],
}

ENTITY_TYPES = ["product", "article", "user", "order", "review"]


async def enqueue_random_job() -> None:
    """Pick a random job type and enqueue it with realistic arguments."""
    choice = random.randint(0, 11)
    user_id = _rand_user_id()
    name = random.choice(FIRST_NAMES)
    func: Callable[..., None] | None = None
    kwargs: dict = {}
    extra: dict = {}

    if choice == 0:
        func = send_welcome_email
        kwargs = {"user_id": user_id, "email": _rand_email(name)}
        extra = {"_queue": "email"}

    elif choice == 1:
        func = send_password_reset
        kwargs = {"user_id": user_id, "email": _rand_email(name), "token": uuid.uuid4().hex[:24]}
        extra = {"_queue": "email", "_delay": random.randint(0, 5)}

    elif choice == 2:
        func = send_invoice_email
        kwargs = {
            "order_id": _rand_order_id(),
            "customer_email": _rand_email(name),
            "amount_cents": random.randint(500, 50000),
        }
        extra = {"_queue": "email"}

    elif choice == 3:
        func = generate_thumbnail
        kwargs = {
            "asset_id": _rand_asset_id(),
            "source_url": f"https://storage.example.com/uploads/{uuid.uuid4()}.jpg",
            "width": random.choice([240, 480, 720, 1280]),
            "height": random.choice([135, 270, 405, 720]),
        }
        extra = {"_queue": "media"}

    elif choice == 4:
        func = transcode_video
        kwargs = {
            "video_id": _rand_asset_id(),
            "source_url": f"https://storage.example.com/raw/{uuid.uuid4()}.mov",
            "format": random.choice(["mp4", "webm"]),
        }
        extra = {"_queue": "media"}

    elif choice == 5:
        func = sync_crm_contact
        kwargs = {
            "user_id": user_id,
            "event": random.choice(["signup", "profile_update", "subscription_change", "deletion_request"]),
        }
        extra = {"_queue": "default"}

    elif choice == 6:
        func = charge_subscription
        kwargs = {
            "subscription_id": f"sub_{uuid.uuid4().hex[:14]}",
            "amount_cents": random.choice([999, 1999, 4999, 9999, 29900]),
        }
        extra = {"_queue": "billing", "_retry": Retry(max=4, intervals=[10, 30, 120])}

    elif choice == 7:
        func = refresh_search_index
        kwargs = {"entity_type": random.choice(ENTITY_TYPES), "entity_id": str(random.randint(1, 99999))}
        extra = {"_queue": "default"}

    elif choice == 8:
        func = export_report
        kwargs = {
            "report_type": random.choice(REPORT_TYPES),
            "user_id": user_id,
            "params": {"format": "csv", "period": "last_30_days"},
        }
        extra = {"_queue": "default"}

    elif choice == 9:
        title, body = random.choice(PUSH_TITLES)
        func = send_push_notification
        kwargs = {"user_id": user_id, "title": title, "body": body}
        extra = {"_queue": "default"}

    elif choice == 10:
        source = random.choice(WEBHOOK_SOURCES)
        func = process_webhook
        kwargs = {
            "source": source,
            "event_type": random.choice(WEBHOOK_EVENTS[source]),
            "payload_id": uuid.uuid4().hex[:16],
        }
        extra = {"_queue": "default"}

    else:
        func = cleanup_expired_sessions
        kwargs = {"older_than_days": random.choice([7, 14, 30, 90])}
        extra = {"_queue": "default"}

    job = await app.enqueue(func, **kwargs, **extra)
    assert job is not None
    logger.info("→ enqueued %-32s queue=%-8s id=%s", func.__name__, extra.get("_queue", "default"), job.id)


async def main() -> None:
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, _stop.set)
    loop.add_signal_handler(signal.SIGTERM, _stop.set)

    logger.info("Producer started — enqueuing jobs every 0.5–4s. Press Ctrl+C to stop.")

    async with app:
        while not _stop.is_set():
            try:
                await enqueue_random_job()
            except Exception as exc:
                logger.error("Failed to enqueue job: %s", exc)

            delay = random.uniform(0.05, 0.1)
            try:
                await asyncio.wait_for(_stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    logger.info("Producer stopped.")


asyncio.run(main())
