"""
routes/orders.py — REST CRUD endpoints + WebSocket hub for the orders table.

Endpoints
─────────
GET     /orders               list all orders
GET     /orders/{id}          fetch one order
POST    /orders               create an order
PATCH   /orders/{id}          update status / fields
DELETE  /orders/{id}          delete an order
WS      /ws/orders            WebSocket — pushed notifications
"""

import logging
from datetime import datetime
from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from app.database import get_pool
from app.websocket_manager import manager

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

OrderStatus = Literal["pending", "shipped", "delivered"]


class OrderCreate(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=120)
    product_name: str = Field(..., min_length=1, max_length=120)
    status: OrderStatus = "pending"


class OrderUpdate(BaseModel):
    customer_name: Optional[str] = Field(None, min_length=1, max_length=120)
    product_name: Optional[str] = Field(None, min_length=1, max_length=120)
    status: Optional[OrderStatus] = None


class OrderOut(BaseModel):
    id: int
    customer_name: str
    product_name: str
    status: str
    updated_at: datetime

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row) -> dict:
    """Convert an asyncpg Record to a plain dict for the response."""
    return {
        "id": row["id"],
        "customer_name": row["customer_name"],
        "product_name": row["product_name"],
        "status": row["status"],
        "updated_at": row["updated_at"].isoformat(),
    }


# ---------------------------------------------------------------------------
# REST — list
# ---------------------------------------------------------------------------

@router.get("/orders", response_model=list[OrderOut], tags=["orders"])
async def list_orders():
    """Return all orders ordered by most-recently updated."""
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders ORDER BY updated_at DESC"
        )
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# REST — get one
# ---------------------------------------------------------------------------

@router.get("/orders/{order_id}", response_model=OrderOut, tags=["orders"])
async def get_order(order_id: int):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# REST — create
# ---------------------------------------------------------------------------

@router.post("/orders", response_model=OrderOut, status_code=201, tags=["orders"])
async def create_order(body: OrderCreate):
    """
    Insert a new order.  The DB trigger will NOTIFY 'orders_updates' and
    every WebSocket client will receive the INSERT event automatically.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO orders (customer_name, product_name, status)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            body.customer_name,
            body.product_name,
            body.status,
        )
    logger.info("Created order id=%d", row["id"])
    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# REST — update
# ---------------------------------------------------------------------------

@router.patch("/orders/{order_id}", response_model=OrderOut, tags=["orders"])
async def update_order(order_id: int, body: OrderUpdate):
    """
    Update one or more fields of an existing order.
    Only supplied fields are modified; updated_at is refreshed by the trigger.
    """
    # Build a dynamic SET clause from only the fields that were provided
    fields = body.model_dump(exclude_none=True)
    if not fields:
        raise HTTPException(status_code=422, detail="No fields to update")

    set_clauses = []
    values = []
    for i, (key, val) in enumerate(fields.items(), start=1):
        set_clauses.append(f"{key} = ${i}")
        values.append(val)

    values.append(order_id)
    query = (
        f"UPDATE orders SET {', '.join(set_clauses)}, updated_at = NOW() "
        f"WHERE id = ${len(values)} RETURNING *"
    )

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, *values)
    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    logger.info("Updated order id=%d  fields=%s", order_id, list(fields.keys()))
    return _row_to_dict(row)


# ---------------------------------------------------------------------------
# REST — delete
# ---------------------------------------------------------------------------

@router.delete("/orders/{order_id}", status_code=204, tags=["orders"])
async def delete_order(order_id: int):
    """
    Delete an order.  The trigger fires a DELETE notification to all clients.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM orders WHERE id = $1", order_id)

    # asyncpg returns "DELETE N" — N = 0 means nothing was deleted
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    logger.info("Deleted order id=%d", order_id)
    # 204 No Content — no body returned


# ---------------------------------------------------------------------------
# WebSocket hub
# ---------------------------------------------------------------------------

@router.websocket("/ws/orders")
async def websocket_orders(websocket: WebSocket):
    """
    Clients connect here to receive real-time order change events.
    The server pushes JSON messages; clients do not need to send anything
    (though we consume incoming messages to avoid backpressure buildup).
    """
    await manager.connect(websocket)
    try:
        while True:
            # We don't expect client messages, but we must await something
            # so the coroutine yields — receive_text() also detects disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("WebSocket error: %s", exc)
        manager.disconnect(websocket)
