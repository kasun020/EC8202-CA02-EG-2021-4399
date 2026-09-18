"""
FastAPI Real-Time Web Telemetry Dashboard for Kasun's Food Streaming Pipeline.
Runs on port 8052 with WebSocket support and interactive food order dispatchers.
"""

import asyncio
import json
import threading
import logging
from pathlib import Path
from typing import Dict, Any, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_ORDERS,
    WEB_HOST,
    WEB_PORT
)
from src.consumer import StreamingMetricsAggregator, FoodOrderConsumer
from src.retry_worker import FoodRetryWorker
from src.producer import FoodOrderProducer
from src.dlq_auditor import FoodDLQAuditor

logger = logging.getLogger("FoodWebDashboard")
BASE_DIR = Path(__file__).resolve().parent

# Shared State
aggregator = StreamingMetricsAggregator()
producer = FoodOrderProducer()
active_sockets: List[WebSocket] = []

# Background Consumer & Retry Worker
main_consumer = FoodOrderConsumer(group_id="kasun-web-consumer-group")
main_consumer.aggregator = aggregator
retry_worker = FoodRetryWorker(group_id="kasun-web-retry-group", backoff_base=1.0)


def start_workers():
    t1 = threading.Thread(target=main_consumer.start_polling, kwargs={"timeout": 0.5}, daemon=True)
    t2 = threading.Thread(target=retry_worker.start, kwargs={"timeout": 0.5}, daemon=True)
    t1.start()
    t2.start()


start_workers()

app = FastAPI(title="CeylonBites Live Order Stream")
static_folder = BASE_DIR / "static"
static_folder.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_folder)), name="static")


class FoodOrderPayload(BaseModel):
    mode: str = "normal"  # normal, transient, permanent, corrupt
    count: int = 1


@app.get("/")
async def root():
    return FileResponse(str(static_folder / "index.html"))


@app.get("/api/metrics")
async def get_current_metrics():
    return aggregator.get_state()


@app.get("/api/dlq")
async def get_rejected_records():
    auditor = FoodDLQAuditor()
    return auditor.fetch_rejected_orders(max_records=20)


@app.post("/api/order")
async def trigger_food_order(payload: FoodOrderPayload):
    dispatched_list = []
    for _ in range(max(1, min(payload.count, 15))):
        order = producer.generate_order(failure_type=payload.mode)
        is_corrupt = (payload.mode == "corrupt")
        ok = producer.dispatch(order, send_corrupt=is_corrupt)
        dispatched_list.append({"order": order, "success": ok})
    producer.flush()
    await broadcast_state()
    return {"status": "dispatched", "items": dispatched_list}


async def broadcast_state():
    state = aggregator.get_state()
    msg = json.dumps({"type": "telemetry", "data": state})
    for s in list(active_sockets):
        try:
            await s.send_text(msg)
        except Exception:
            if s in active_sockets:
                active_sockets.remove(s)


@app.websocket("/ws")
async def ws_telemetry(ws: WebSocket):
    await ws.accept()
    active_sockets.append(ws)
    await ws.send_text(json.dumps({"type": "telemetry", "data": aggregator.get_state()}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        if ws in active_sockets:
            active_sockets.remove(ws)


def run_server():
    import uvicorn
    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT, log_level="warning")


if __name__ == "__main__":
    run_server()
