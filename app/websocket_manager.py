"""
websocket_manager.py — WebSocket connection registry & broadcaster.

ConnectionManager keeps a set of active WebSocket connections and exposes
a single broadcast() coroutine that fans out a JSON message to every one
of them.  Stale / closed connections are silently pruned on each send.
"""

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Thread-safe (single-event-loop) registry for live WebSocket clients."""

    def __init__(self) -> None:
        # Use a set so connect/disconnect are O(1)
        self._active: set[WebSocket] = set()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self, websocket: WebSocket) -> None:
        """Accept the handshake and register the connection."""
        await websocket.accept()
        self._active.add(websocket)
        logger.info(
            "WebSocket connected  — total clients: %d", len(self._active)
        )

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a connection (called on close or error)."""
        self._active.discard(websocket)
        logger.info(
            "WebSocket disconnected — total clients: %d", len(self._active)
        )

    # ------------------------------------------------------------------
    # Broadcasting
    # ------------------------------------------------------------------

    async def broadcast(self, payload: dict[str, Any]) -> None:
        """
        Send *payload* as JSON text to every connected client.

        Clients that have disconnected since the last broadcast are detected
        here and removed from the active set so they don't accumulate.
        """
        if not self._active:
            return  # nothing to do — fast-path

        message = json.dumps(payload)
        dead: set[WebSocket] = set()

        # asyncio.gather lets all sends happen concurrently instead of
        # sequentially, which matters when there are many clients.
        results = await asyncio.gather(
            *[ws.send_text(message) for ws in self._active],
            return_exceptions=True,
        )

        # Prune any connections that errored out
        for ws, result in zip(self._active, results):
            if isinstance(result, Exception):
                logger.warning("Failed to send to a client — removing: %s", result)
                dead.add(ws)

        self._active -= dead

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def client_count(self) -> int:
        return len(self._active)


# Singleton shared across the whole application
manager = ConnectionManager()
