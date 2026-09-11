"""
Real-Time Food Order Consumer & Metrics Aggregator.
Consumes from 'food-orders', computes running average price,
routes transient kitchen delays to 'food-orders-retry', and routes poison orders to 'food-orders-dlq'.
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable
from confluent_kafka import Consumer, Producer, KafkaError
from rich.console import Console
from rich.table import Table
from rich.live import Live

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_ORDERS,
    TOPIC_FOOD_RETRY,
    TOPIC_FOOD_DLQ,
    GROUP_FOOD_MAIN,
    MAX_RETRY_ATTEMPTS
)
from src.avro_codec import FoodOrderCodec, FoodOrderDeserializationException

logger = logging.getLogger("FoodOrderConsumer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
console = Console()


class StreamingMetricsAggregator:
    """
    Real-time streaming metrics tracker.
    Incrementally computes running average price in O(1) time and O(1) space.
    """

    def __init__(self):
        self.received_total: int = 0
        self.completed_count: int = 0
        self.total_revenue: float = 0.0
        self.running_average: float = 0.0
        self.min_order_price: Optional[float] = None
        self.max_order_price: Optional[float] = None
        self.dish_summary: Dict[str, Dict[str, Any]] = {}
        self.retry_count: int = 0
        self.dlq_count: int = 0
        self.recent_order: Optional[Dict[str, Any]] = None

    def add_completed_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Incorporates a completed meal order into running calculations."""
        dish = str(order["product"])
        price = float(order["price"])

        self.received_total += 1
        self.completed_count += 1
        self.total_revenue += price
        self.running_average = self.total_revenue / self.completed_count

        if self.min_order_price is None or price < self.min_order_price:
            self.min_order_price = price
        if self.max_order_price is None or price > self.max_order_price:
            self.max_order_price = price

        if dish not in self.dish_summary:
            self.dish_summary[dish] = {"orders": 0, "sum": 0.0, "avg": 0.0}

        entry = self.dish_summary[dish]
        entry["orders"] += 1
        entry["sum"] += price
        entry["avg"] = entry["sum"] / entry["orders"]

        self.recent_order = order
        return self.get_state()

    def mark_retry_event(self):
        self.received_total += 1
        self.retry_count += 1

    def mark_dlq_event(self):
        self.received_total += 1
        self.dlq_count += 1

    def get_state(self) -> Dict[str, Any]:
        """Returns snapshot of running aggregation."""
        return {
            "received_orders": self.received_total,
            "completed_orders": self.completed_count,
            "total_revenue": round(self.total_revenue, 2),
            "running_average_price": round(self.running_average, 2),
            "min_price": round(self.min_order_price, 2) if self.min_order_price is not None else 0.0,
            "max_price": round(self.max_order_price, 2) if self.max_order_price is not None else 0.0,
            "retry_events": self.retry_count,
            "dlq_events": self.dlq_count,
            "unique_dishes": len(self.dish_summary),
            "recent_order": self.recent_order
        }

    def create_rich_table(self) -> Table:
        """Constructs an emerald & gold styled CLI metrics dashboard."""
        table = Table(title="GourmetExpress Real-Time Food Stream Aggregator", style="bold green")
        table.add_column("Telemetry Metric", style="bold white")
        table.add_column("Live Metric Value", style="bold green", justify="right")

        table.add_row("Orders Completed", str(self.completed_count))
        table.add_row("Total Culinary Revenue", f"${self.total_revenue:,.2f}")
        table.add_row("Running Average Meal Price", f"${self.running_average:,.2f}")
        table.add_row("Min / Max Dish Price", f"${self.min_order_price or 0.0:,.2f} / ${self.max_order_price or 0.0:,.2f}")
        table.add_row("Kitchen Delays (Retries)", f"[yellow]{self.retry_count}[/yellow]")
        table.add_row("Unserviceable (DLQ)", f"[red]{self.dlq_count}[/red]")
        if self.recent_order:
            table.add_row("Latest Dish", f"{self.recent_order['orderId']} - {self.recent_order['product']} (${self.recent_order['price']:.2f})")
        return table


class FoodOrderConsumer:
    """
    Subscribes to 'food-orders', deserializes Avro payloads,
    updates real-time running aggregates, and manages retry & DLQ routing.
    """

    def __init__(self,
                 bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
                 registry_url: str = SCHEMA_REGISTRY_URL,
                 group_id: str = GROUP_FOOD_MAIN,
                 on_update: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.codec = FoodOrderCodec(registry_url=registry_url)
        self.aggregator = StreamingMetricsAggregator()
        self.on_update = on_update
        self.is_active = False

        c_conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        self.consumer = Consumer(c_conf)

        p_conf = {
            "bootstrap.servers": self.bootstrap,
            "client.id": "food-router-producer",
            "acks": "all"
        }
        self.router = Producer(p_conf)

    def route_to_retry(self, key: Optional[bytes], value: bytes, reason: str, attempt: int = 1):
        headers = [
            ("x-retry-attempt", str(attempt).encode("utf-8")),
            ("x-source-topic", TOPIC_FOOD_ORDERS.encode("utf-8")),
            ("x-delay-reason", reason.encode("utf-8")),
            ("x-event-time", datetime.now(timezone.utc).isoformat().encode("utf-8"))
        ]
        self.router.produce(topic=TOPIC_FOOD_RETRY, key=key, value=value, headers=headers)
        self.router.poll(0)
        self.aggregator.mark_retry_event()
        logger.warning(f"[RETRY DISPATCHED] Key: {key} -> '{TOPIC_FOOD_RETRY}' | Attempt: {attempt} | Reason: {reason}")

    def route_to_dlq(self, key: Optional[bytes], value: bytes, reason: str, details: str, retries: int = 0):
        headers = [
            ("x-retry-count", str(retries).encode("utf-8")),
            ("x-source-topic", TOPIC_FOOD_ORDERS.encode("utf-8")),
            ("x-dlq-cause", reason.encode("utf-8")),
            ("x-rejection-info", details.encode("utf-8")),
            ("x-event-time", datetime.now(timezone.utc).isoformat().encode("utf-8"))
        ]
        self.router.produce(topic=TOPIC_FOOD_DLQ, key=key, value=value, headers=headers)
        self.router.poll(0)
        self.aggregator.mark_dlq_event()
        logger.error(f"[DLQ DISPATCHED] Key: {key} -> '{TOPIC_FOOD_DLQ}' | Cause: {reason} | Info: {details}")

    def process_record(self, msg) -> None:
        raw_val = msg.value()
        raw_key = msg.key()

        # Step 1: Avro Deserialization
        try:
            order = self.codec.decode(raw_val)
        except FoodOrderDeserializationException as de:
            logger.error(f"Failed to decode Avro food order: {de}")
            self.route_to_dlq(
                key=raw_key,
                value=raw_val,
                reason="CorruptAvroBinary",
                details=str(de),
                retries=0
            )
            self.consumer.commit(message=msg, asynchronous=False)
            return

        dish = order.get("product", "")
        price = order.get("price", 0.0)

        # Step 2: Permanent Error (Poison order / Expired kitchen ticket)
        if dish.startswith("EXPIRED_TICKET_") or price <= 0 or price > 5000.0:
            err = f"Unserviceable food order: ID {order.get('orderId')}, Dish {dish}, Price {price}"
            self.route_to_dlq(
                key=raw_key,
                value=raw_val,
                reason="ExpiredOrUnserviceableOrder",
                details=err,
                retries=0
            )
            self.consumer.commit(message=msg, asynchronous=False)
            return

        # Step 3: Transient Error (Kitchen Line Busy / Courier Unavailable)
        if dish.startswith("KITCHEN_TIMEOUT_"):
            logger.info(f"Kitchen delay for order {order.get('orderId')}. Forwarding to retry queue...")
            self.route_to_retry(
                key=raw_key,
                value=raw_val,
                reason="KitchenLineCapacityFull",
                attempt=1
            )
            self.consumer.commit(message=msg, asynchronous=False)
            return

        # Step 4: Normal Valid Order -> Real-time Running Avg Aggregation
        metrics = self.aggregator.add_completed_order(order)
        logger.info(
            f"[ORDER COMPLETED] ID: {order['orderId']} | Dish: {order['product']} | "
            f"Price: ${order['price']:.2f} | Running Avg: ${metrics['running_average_price']:.2f} "
            f"(Total Orders: {metrics['completed_orders']})"
        )

        if self.on_update:
            self.on_update(metrics)

        self.consumer.commit(message=msg, asynchronous=False)

    def start_polling(self, max_records: Optional[int] = None, timeout: float = 1.0, show_dashboard: bool = False):
        self.consumer.subscribe([TOPIC_FOOD_ORDERS])
        self.is_active = True
        logger.info(f"FoodOrderConsumer listening on '{TOPIC_FOOD_ORDERS}'...")

        count = 0
        try:
            if show_dashboard:
                with Live(self.aggregator.create_rich_table(), refresh_per_second=4, console=console) as live:
                    while self.is_active:
                        msg = self.consumer.poll(timeout=timeout)
                        if msg is None:
                            continue
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            logger.error(f"Kafka error: {msg.error()}")
                            break
                        self.process_record(msg)
                        count += 1
                        live.update(self.aggregator.create_rich_table())
                        if max_records and count >= max_records:
                            break
            else:
                while self.is_active:
                    msg = self.consumer.poll(timeout=timeout)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                    self.process_record(msg)
                    count += 1
                    if max_records and count >= max_records:
                        break
        except KeyboardInterrupt:
            logger.info("FoodOrderConsumer halted by operator.")
        finally:
            self.shutdown()

    def shutdown(self):
        self.is_active = False
        self.router.flush(timeout=5.0)
        self.consumer.close()
        logger.info("FoodOrderConsumer stopped cleanly.")


def main():
    parser = argparse.ArgumentParser(description="Food Order Streaming Consumer")
    parser.add_argument("--max", type=int, default=None, help="Maximum orders to process")
    parser.add_argument("--live", action="store_true", help="Display live table dashboard")
    args = parser.parse_args()

    consumer = FoodOrderConsumer()
    consumer.start_polling(max_records=args.max, show_dashboard=args.live)


if __name__ == "__main__":
    main()
