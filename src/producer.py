"""
Food Order Dispatcher & Producer for GourmetExpress Streaming Pipeline.
Produces purchase transactions to 'food-orders' with Avro serialization and fault simulation.
"""

import argparse
import random
import time
import logging
from typing import Dict, Any, Optional, List
from confluent_kafka import Producer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_ORDERS
)
from src.avro_codec import FoodOrderCodec, FoodOrderSerializationException

logger = logging.getLogger("FoodOrderProducer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

MENU_ITEMS = [
    ("Truffle Mushroom Burger", 18.50),
    ("Wood-Fired Margherita Pizza", 22.00),
    ("Salmon Nigiri Omakase Platter", 38.50),
    ("Wagyu Tonkotsu Ramen", 24.00),
    ("Rigatoni Truffle Carbonara", 26.50),
    ("Mediterranean Grilled Sea Bass", 34.00),
    ("Thai Green Coconut Curry", 19.50),
    ("Matcha Basque Cheesecake", 12.50),
    ("Dragon Roll Sushi Special", 29.00),
    ("Burrata Caprese Crostini", 16.50)
]


class FoodOrderProducer:
    """Dispatches culinary meal purchase transactions to Kafka."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 registry_url: str = SCHEMA_REGISTRY_URL):
        self.bootstrap_servers = bootstrap_servers
        self.codec = FoodOrderCodec(registry_url=registry_url)
        try:
            self.codec.register_schema()
        except Exception as e:
            logger.warning(f"Schema registration fallback active: {e}")

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "gourmet-order-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 300
        }
        self.producer = Producer(conf)
        self.order_sequence = 2000

    def _delivery_callback(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for food order: {err}")
        else:
            logger.info(f"Delivered food order to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

    def generate_order(
        self,
        order_id: Optional[str] = None,
        product: Optional[str] = None,
        price: Optional[float] = None,
        failure_type: str = "normal"
    ) -> Dict[str, Any]:
        """Builds a culinary purchase transaction dictionary."""
        self.order_sequence += 1
        oid = order_id or f"FOOD-{self.order_sequence}"

        if product:
            item_name = product
            item_price = price if price is not None else 25.0
        else:
            chosen_item, base_price = random.choice(MENU_ITEMS)
            # Add slight randomized price variation (+- 10%)
            variation = round(random.uniform(-2.0, 5.0), 2)
            item_name = chosen_item
            item_price = round(max(5.0, base_price + variation), 2)

        if failure_type == "transient":
            item_name = f"KITCHEN_TIMEOUT_{item_name}"
        elif failure_type == "permanent":
            item_name = f"EXPIRED_TICKET_{item_name}"

        return {
            "orderId": oid,
            "product": item_name,
            "price": float(item_price)
        }

    def dispatch(
        self,
        order: Dict[str, Any],
        topic: str = TOPIC_FOOD_ORDERS,
        send_corrupt: bool = False
    ) -> bool:
        """Serializes and publishes order event to Kafka."""
        try:
            key = order["orderId"].encode("utf-8")
            if send_corrupt:
                payload = b"\x00\x00\x00\x01\xDE\xAD\xBE\xEF_POISON_FOOD_PAYLOAD"
            else:
                payload = self.codec.encode(order)

            headers = [("content-type", b"avro/binary")]
            self.producer.produce(
                topic=topic,
                key=key,
                value=payload,
                headers=headers,
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            logger.info(f"[DISPATCHED] -> Topic: {topic} | Order: {order.get('orderId')} | Dish: {order.get('product')} | Price: ${order.get('price'):.2f}")
            return True
        except Exception as ex:
            logger.error(f"Failed to dispatch order {order}: {ex}")
            return False

    def flush(self, timeout: float = 10.0):
        self.producer.flush(timeout)

    def simulate_stream(self, count: int = 10, interval: float = 0.5,
                        transient_indices: Optional[List[int]] = None,
                        permanent_indices: Optional[List[int]] = None,
                        corrupt_indices: Optional[List[int]] = None):
        """Simulates food ordering rush with controlled fault injections."""
        transient_indices = transient_indices or []
        permanent_indices = permanent_indices or []
        corrupt_indices = corrupt_indices or []

        logger.info(f"Starting food order stream ({count} orders, interval: {interval}s)...")
        for i in range(1, count + 1):
            if i in transient_indices:
                mode = "transient"
            elif i in permanent_indices:
                mode = "permanent"
            else:
                mode = "normal"

            order = self.generate_order(failure_type=mode)
            is_corrupt = (i in corrupt_indices)
            self.dispatch(order, send_corrupt=is_corrupt)
            time.sleep(interval)

        self.flush()
        logger.info("Food order stream simulation completed.")


def main():
    parser = argparse.ArgumentParser(description="Food Order Producer (GourmetExpress)")
    parser.add_argument("--count", type=int, default=10, help="Number of food orders to dispatch")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between orders")
    parser.add_argument("--transient", type=int, nargs="*", default=[], help="Indices for kitchen timeout (e.g. 2 4)")
    parser.add_argument("--permanent", type=int, nargs="*", default=[], help="Indices for expired tickets (e.g. 5)")
    parser.add_argument("--corrupt", type=int, nargs="*", default=[], help="Indices for corrupt raw bytes")
    args = parser.parse_args()

    producer = FoodOrderProducer()
    producer.simulate_stream(
        count=args.count,
        interval=args.interval,
        transient_indices=args.transient,
        permanent_indices=args.permanent,
        corrupt_indices=args.corrupt
    )


if __name__ == "__main__":
    main()
