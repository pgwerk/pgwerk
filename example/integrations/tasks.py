"""Shared job handlers for the framework integration examples."""

from __future__ import annotations

import logging

logger = logging.getLogger("pgwerk.example")


async def send_welcome_email(user_id: int, email: str) -> None:
    """A trivial handler standing in for real work (sending an email, etc.)."""
    logger.info("sending welcome email to user_id=%s <%s>", user_id, email)
