"""
main.py — FastAPI application entry point.
Starts the server, connects to the database pool, launches the
PostgreSQL LISTEN/NOTIFY listener, and mounts all routers.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import connect_db, disconnect_db
from app.db_listener import start_listener, stop_listener
from app.routes.orders import router as orders_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Async context manager executed once at startup and once at shutdown.
    Order matters: DB pool must exist before the listener starts.
    """
    logger.info("🚀  Starting up …")
    await connect_db()          # initialise asyncpg connection pool
    await start_listener()      # start LISTEN loop in a background task
    logger.info("✅  Ready to serve requests")

    yield                       # ← application runs here

    logger.info("🛑  Shutting down …")
    await stop_listener()       # cancel the background listener task
    await disconnect_db()       # close all pool connections
    logger.info("👋  Goodbye")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Realtime Orders API",
    description="FastAPI + PostgreSQL LISTEN/NOTIFY + WebSockets demo",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow the HTML client (served from any origin during development) to connect.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the orders REST + WebSocket router
app.include_router(orders_router)


@app.get("/health", tags=["meta"])
async def health():
    """Simple health-check endpoint."""
    return {"status": "ok"}
