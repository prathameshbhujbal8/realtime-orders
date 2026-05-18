# Realtime Orders — FastAPI + PostgreSQL LISTEN/NOTIFY + WebSockets

A production-style demonstration of **zero-polling realtime database updates**
using PostgreSQL's native pub/sub, asyncpg, FastAPI, and WebSockets.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Why LISTEN/NOTIFY Instead of Polling](#2-why-listennotify-instead-of-polling)
3. [Scalability Discussion](#3-scalability-discussion)
4. [Project Structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Setup Steps](#6-setup-steps)
7. [Running the Backend](#7-running-the-backend)
8. [Running the Frontend Client](#8-running-the-frontend-client)
9. [REST API Reference](#9-rest-api-reference)
10. [Example Workflow](#10-example-workflow)
11. [WebSocket Payload Examples](#11-websocket-payload-examples)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Browser Clients                            │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  client/index.html  (HTML + Vanilla JS)                     │   │
│   │                                                              │   │
│   │  REST calls (fetch)  ────────────────────────┐              │   │
│   │  WebSocket (ws://)   ──────────────────────┐ │              │   │
│   └────────────────────────────────────────────┼─┼──────────────┘   │
└───────────────────────────────────────────────┼─┼────────────────── ┘
                                                │ │
                              ┌─────────────────▼─▼──────────────────┐
                              │           FastAPI Server              │
                              │                                       │
                              │  ┌──────────────────────────────────┐ │
                              │  │  routes/orders.py                │ │
                              │  │   • GET /orders                  │ │
                              │  │   • GET /orders/{id}             │ │
                              │  │   • POST /orders                 │ │
                              │  │   • PATCH /orders/{id}           │ │
                              │  │   • DELETE /orders/{id}          │ │
                              │  │   • WS /ws/orders  ◄── clients   │ │
                              │  └──────────────────────────────────┘ │
                              │                                       │
                              │  ┌──────────────────────────────────┐ │
                              │  │  websocket_manager.py            │ │
                              │  │   ConnectionManager (singleton)  │ │
                              │  │   • connect / disconnect         │ │
                              │  │   • broadcast → all WS clients   │ │
                              │  └─────────────────┬────────────────┘ │
                              │                    │ broadcasts        │
                              │  ┌─────────────────▼────────────────┐ │
                              │  │  db_listener.py                  │ │
                              │  │   asyncio Task (background)       │ │
                              │  │   LISTEN orders_updates           │ │
                              │  └─────────────────▲────────────────┘ │
                              └────────────────────┼──────────────────┘
                                                   │  NOTIFY (asyncpg)
                              ┌────────────────────▼──────────────────┐
                              │          PostgreSQL                    │
                              │                                       │
                              │  orders table                         │
                              │   ┌────────────────────────────────┐  │
                              │   │  AFTER INSERT/UPDATE/DELETE    │  │
                              │   │  trigger → notify_orders_change│  │
                              │   │  → pg_notify('orders_updates') │  │
                              │   └────────────────────────────────┘  │
                              └───────────────────────────────────────┘
```

**Data flow for a mutation:**

1. Client calls `POST /orders` (or PATCH / DELETE).
2. FastAPI executes the SQL via the asyncpg **pool**.
3. PostgreSQL fires the row-level **trigger** automatically.
4. The trigger calls `pg_notify('orders_updates', <json>)`.
5. The **dedicated listener connection** (not pooled) receives the notification.
6. `db_listener.py` schedules `_handle_notification()` as an asyncio task.
7. `ConnectionManager.broadcast()` fans the JSON out to every live WebSocket.
8. Each browser renders the update instantly — **no polling, no delay**.

---

## 2. Why LISTEN/NOTIFY Instead of Polling

| Concern | Polling | LISTEN/NOTIFY |
|---|---|---|
| **Latency** | Up to the poll interval (e.g. 1–5 s) | Sub-millisecond after commit |
| **DB load** | Constant `SELECT` queries, even when idle | Zero overhead when nothing changes |
| **Scalability** | Load grows linearly with clients × poll rate | One persistent connection per server process |
| **Correctness** | Can miss rapid successive changes between polls | Every committed transaction fires exactly one notification |
| **Complexity** | Simple but wasteful | Slightly more setup; built into every Postgres instance |

PostgreSQL's `LISTEN/NOTIFY` is a first-class feature — no extensions, no extra
services, no extra infrastructure. The notification is sent *within the same
transaction* that mutated the data, so clients never see a stale window.

---

## 3. Scalability Discussion

### Single server (current implementation)

- One asyncpg pool (2–10 connections) handles all REST traffic.
- One *dedicated* asyncpg connection handles all LISTEN callbacks.
- `ConnectionManager` holds every WebSocket in memory.
- Suitable for thousands of concurrent WebSocket clients on a single Uvicorn
  worker (asyncio is non-blocking).

### Horizontal scaling

When you add multiple FastAPI processes/machines:

- **Problem:** Each process has its own `ConnectionManager`. A NOTIFY received
  by process A won't be forwarded to clients connected to process B.
- **Solution 1 — Redis Pub/Sub:** Each process subscribes to a shared Redis
  channel. Process A publishes; all processes (including B) receive and
  broadcast to their local clients. Simple and widely used.
- **Solution 2 — Each process LISTENs independently:** All processes subscribe
  to the same PostgreSQL channel. PostgreSQL delivers the notification to
  *every* listening connection, so each process independently broadcasts to
  its own clients. No extra broker needed, but you need to manage N persistent
  connections.
- **Solution 3 — Message broker (Kafka / RabbitMQ):** For very high throughput
  or complex routing; overkill for most applications.

### Database side

- The trigger is pure PL/pgSQL and executes in microseconds.
- `pg_notify` payload is capped at 8 000 bytes. For very wide rows, send only
  the primary key and let clients re-fetch via REST.
- Index on `updated_at` keeps the REST list endpoint fast as the table grows.

---

## 4. Project Structure

```
realtime-orders/
│
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI app, lifespan, CORS, router mount
│   ├── database.py           # asyncpg pool management
│   ├── websocket_manager.py  # ConnectionManager singleton
│   ├── db_listener.py        # PostgreSQL LISTEN background task
│   └── routes/
│       ├── __init__.py
│       └── orders.py         # REST CRUD + WebSocket endpoint
│
├── sql/
│   └── trigger.sql           # Schema, trigger function, trigger attachment
│
├── client/
│   └── index.html            # Self-contained HTML/JS/CSS realtime client
│
├── requirements.txt
├── env.example               # Copy to .env and fill in credentials
└── README.md
```

---

## 5. Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 + |
| PostgreSQL | 13 + |
| pip | any recent |

---

## 6. Setup Steps

### 6.1 Clone / copy the project

```bash
cd realtime-orders
```

### 6.2 Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
```

### 6.3 Install dependencies

```bash
pip install -r requirements.txt
```

### 6.4 Configure the environment

```bash
cp env.example .env
# Open .env and set DATABASE_URL to your PostgreSQL connection string
# e.g. DATABASE_URL=postgresql://postgres:secret@localhost:5432/orders_db
```

### 6.5 Create the database (if it doesn't exist)

```bash
createdb orders_db     # or use psql / pgAdmin
```

### 6.6 Apply the schema and trigger

```bash
psql -d orders_db -f sql/trigger.sql
```

You should see output like:
```
CREATE TABLE
CREATE INDEX
CREATE FUNCTION
DROP TRIGGER
CREATE TRIGGER
```

---

## 7. Running the Backend

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The server starts and logs:
```
INFO  Starting up …
INFO  Connecting to PostgreSQL …
INFO  PostgreSQL pool ready  (min=2, max=10)
INFO  Listener connection established — subscribing to 'orders_updates'
INFO  Listening on channel 'orders_updates'
INFO  Ready to serve requests
```

Interactive API docs: http://localhost:8000/docs

---

## 8. Running the Frontend Client

The client is a **single static HTML file** — no build step required.

**Option A — open directly in a browser:**

```
open client/index.html       # macOS
xdg-open client/index.html   # Linux
# or just double-click it in your file manager
```

**Option B — serve with Python (avoids CORS edge cases):**

```bash
cd client
python -m http.server 3000
# then open http://localhost:3000
```

The client auto-connects to `ws://localhost:8000/ws/orders` and calls
`http://localhost:8000/orders` for the initial data load.

---

## 9. REST API Reference

| Method | Path | Body | Description |
|---|---|---|---|
| `GET` | `/orders` | — | List all orders (newest first) |
| `GET` | `/orders/{id}` | — | Fetch one order |
| `POST` | `/orders` | `{customer_name, product_name, status?}` | Create order |
| `PATCH` | `/orders/{id}` | any subset of order fields | Update order |
| `DELETE` | `/orders/{id}` | — | Delete order |
| `GET` | `/health` | — | Health check |
| `WS` | `/ws/orders` | — | WebSocket push channel |

---

## 10. Example Workflow

1. Open the frontend client in **two browser tabs**.
2. In tab 1, create an order: customer = "Alice", product = "Laptop".
3. **Both tabs** instantly show the new row (green flash) — no refresh.
4. In tab 1, click **ship** on the new order.
5. **Both tabs** immediately update the status badge to "shipped" (blue flash).
6. Click **deliver** — badge turns green in both tabs.
7. Click **del** — the row fades out of both tabs.

---

## 11. WebSocket Payload Examples

The WebSocket endpoint pushes raw JSON. No custom framing or envelope beyond
what the PostgreSQL trigger produces.

### INSERT

```json
{
  "channel":   "orders_updates",
  "operation": "INSERT",
  "table":     "orders",
  "timestamp": "2024-11-01T14:32:05Z",
  "record": {
    "id":            42,
    "customer_name": "Alice Smith",
    "product_name":  "Mechanical Keyboard",
    "status":        "pending",
    "updated_at":    "2024-11-01T14:32:05.123456+00:00"
  }
}
```

### UPDATE

```json
{
  "channel":   "orders_updates",
  "operation": "UPDATE",
  "table":     "orders",
  "timestamp": "2024-11-01T14:35:22Z",
  "record": {
    "id":            42,
    "customer_name": "Alice Smith",
    "product_name":  "Mechanical Keyboard",
    "status":        "shipped",
    "updated_at":    "2024-11-01T14:35:22.654321+00:00"
  }
}
```

### DELETE

```json
{
  "channel":   "orders_updates",
  "operation": "DELETE",
  "table":     "orders",
  "timestamp": "2024-11-01T14:40:10Z",
  "record": {
    "id":            42,
    "customer_name": "Alice Smith",
    "product_name":  "Mechanical Keyboard",
    "status":        "shipped",
    "updated_at":    "2024-11-01T14:35:22.654321+00:00"
  }
}
```

---

## Notes

- The `.env` file is intentionally excluded from version control. Never commit
  real credentials.
- `trigger.sql` is idempotent — you can re-run it safely (`IF NOT EXISTS`,
  `CREATE OR REPLACE`, `DROP TRIGGER IF EXISTS`).
- The WebSocket client reconnects automatically after a 3-second delay if the
  server restarts.
