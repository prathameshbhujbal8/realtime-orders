"""
db_listener.py — PostgreSQL LISTEN/NOTIFY background task.

A single, long-lived connection subscribes to the 'orders_updates' channel.
When a notification arrives it parses the JSON payload from the trigger and
hands it to ConnectionManager.broadcast() so every WebSocket client is
notified immediately — no polling required.
"""

import asyncio
import json
import logging
import os

import asyncpg
from dotenv import load_dotenv

from app.websocket_manager import manager

load_dotenv()

logger = logging.getLogger(__name__)

# The asyncio Task running _listen_loop(); kept so we can cancel it cleanly.
_listener_task: asyncio.Task | None = None

# Name must match the NOTIFY channel in trigger.sql
CHANNEL = "orders_updates"


async def _listen_loop() -> None:
    """
    Core loop:
      1. Open a *dedicated* asyncpg connection (not from the pool — the
         pool recycles connections and would drop our LISTEN registration).
      2. Register a synchronous callback that queues notifications.
      3. Block forever with conn.wait_closed() so the event loop stays free
         for other coroutines between notifications.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")

    conn: asyncpg.Connection = await asyncpg.connect(dsn=dsn)
    logger.info("Listener connection established — subscribing to '%s'", CHANNEL)

    # asyncpg delivers notifications to a *synchronous* callback.
    # We schedule the async handler via the running event loop.
    loop = asyncio.get_event_loop()

    def _on_notify(
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """Called synchronously by asyncpg on every NOTIFY."""
        loop.create_task(_handle_notification(payload))

    await conn.add_listener(CHANNEL, _on_notify)
    logger.info("Listening on channel '%s'", CHANNEL)

    try:
        # Keep the connection alive indefinitely until the task is cancelled.
        await conn.wait_closed()
    except asyncio.CancelledError:
        logger.info("Listener task cancelled — cleaning up")
    finally:
        try:
            await conn.remove_listener(CHANNEL, _on_notify)
            await conn.close()
        except Exception:
            pass  # connection may already be gone during shutdown


async def _handle_notification(raw_payload: str) -> None:
    """
    Parse the JSON string sent by the PostgreSQL trigger and broadcast it
    to all connected WebSocket clients via the ConnectionManager.
    """
    try:
        data = json.loads(raw_payload)
        logger.info(
            "📣  Notification  op=%s  id=%s",
            data.get("operation"),
            data.get("record", {}).get("id"),
        )
        await manager.broadcast(data)
    except json.JSONDecodeError as exc:
        logger.error("Could not decode notification payload: %s — %s", raw_payload, exc)
    except Exception as exc:
        logger.exception("Unexpected error handling notification: %s", exc)


# ---------------------------------------------------------------------------
# Public API called from main.py lifespan
# ---------------------------------------------------------------------------

async def start_listener() -> None:
    """Launch the listener loop as a background asyncio task."""
    global _listener_task
    _listener_task = asyncio.create_task(_listen_loop(), name="pg-listener")
    logger.info("PostgreSQL listener task started")


async def stop_listener() -> None:
    """Cancel the background task and wait for it to finish."""
    global _listener_task
    if _listener_task and not _listener_task.done():
        _listener_task.cancel()
        try:
            await _listener_task
        except asyncio.CancelledError:
            pass
    _listener_task = None
    logger.info("PostgreSQL listener task stopped")
