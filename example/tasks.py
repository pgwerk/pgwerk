"""
Job handler functions.

Imported by the worker at runtime via their dotted paths.
Can also be imported directly in the producer to pass to app.enqueue().
"""

import asyncio
import logging
import random

from wrk.schemas import Context


logger = logging.getLogger(__name__)


async def send_welcome_email(ctx: Context, user_id: int, email: str) -> dict:
    logger.info("[job %s] Sending welcome email to %s (user %d)", ctx.job.id, email, user_id)
    await asyncio.sleep(random.uniform(0.05, 0.3))
    return {"status": "sent", "to": email}


async def send_password_reset(ctx: Context, user_id: int, email: str, token: str) -> dict:
    logger.info("[job %s] Sending password reset to user %d (%s) token=%s…", ctx.job.id, user_id, email, token[:8])
    await asyncio.sleep(random.uniform(0.05, 0.2))
    return {"status": "sent", "to": email}


async def send_invoice_email(ctx: Context, order_id: str, customer_email: str, amount_cents: int) -> dict:
    logger.info("[job %s] Sending invoice for order %s ($%.2f) to %s", ctx.job.id, order_id, amount_cents / 100, customer_email)
    await asyncio.sleep(random.uniform(0.1, 0.4))
    return {"status": "sent", "order_id": order_id}


async def generate_thumbnail(ctx: Context, asset_id: str, source_url: str, width: int, height: int) -> dict:
    logger.info("[job %s] Generating %dx%d thumbnail for asset %s from %s", ctx.job.id, width, height, asset_id, source_url)
    await asyncio.sleep(random.uniform(0.5, 2.0))
    return {"asset_id": asset_id, "thumbnail_url": f"https://cdn.example.com/thumbs/{asset_id}_{width}x{height}.jpg"}


async def transcode_video(ctx: Context, video_id: str, source_url: str, format: str = "mp4") -> dict:
    logger.info("[job %s] Transcoding video %s (%s) to %s", ctx.job.id, video_id, source_url, format)
    await asyncio.sleep(random.uniform(3.0, 8.0))
    return {"video_id": video_id, "output_url": f"https://cdn.example.com/videos/{video_id}.{format}"}


async def sync_crm_contact(ctx: Context, user_id: int, event: str) -> dict:
    logger.info("[job %s] Syncing CRM contact for user %d (event: %s)", ctx.job.id, user_id, event)
    await asyncio.sleep(random.uniform(0.2, 0.8))
    return {"user_id": user_id, "synced": True}


async def charge_subscription(ctx: Context, subscription_id: str, amount_cents: int, currency: str = "usd") -> dict:
    logger.info("[job %s] Charging subscription %s for %d %s", ctx.job.id, subscription_id, amount_cents, currency)
    await asyncio.sleep(random.uniform(0.3, 1.0))
    if random.random() < 0.05:
        raise RuntimeError("Payment gateway timeout — will retry")
    return {"subscription_id": subscription_id, "charged": amount_cents, "currency": currency}


async def refresh_search_index(ctx: Context, entity_type: str, entity_id: str) -> dict:
    logger.info("[job %s] Refreshing search index for %s/%s", ctx.job.id, entity_type, entity_id)
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {"indexed": True, "entity": f"{entity_type}/{entity_id}"}


async def export_report(ctx: Context, report_type: str, user_id: int, params: dict) -> dict:
    logger.info("[job %s] Exporting %s report for user %d (params=%s)", ctx.job.id, report_type, user_id, params)
    await asyncio.sleep(random.uniform(1.0, 4.0))
    return {"report_type": report_type, "download_url": f"https://app.example.com/reports/{ctx.job.id}.csv"}


async def send_push_notification(ctx: Context, user_id: int, title: str, body: str) -> dict:
    logger.info("[job %s] Push notification to user %d: %s — %s", ctx.job.id, user_id, title, body)
    await asyncio.sleep(random.uniform(0.05, 0.15))
    return {"user_id": user_id, "delivered": True}


async def process_webhook(ctx: Context, source: str, event_type: str, payload_id: str) -> dict:
    logger.info("[job %s] Processing %s webhook (%s) id=%s", ctx.job.id, source, event_type, payload_id)
    await asyncio.sleep(random.uniform(0.1, 0.6))
    return {"source": source, "event_type": event_type, "processed": True}


async def cleanup_expired_sessions(ctx: Context, older_than_days: int = 30) -> dict:
    logger.info("[job %s] Cleaning up sessions older than %d days", ctx.job.id, older_than_days)
    await asyncio.sleep(random.uniform(0.5, 1.5))
    deleted = random.randint(0, 500)
    return {"deleted": deleted}
