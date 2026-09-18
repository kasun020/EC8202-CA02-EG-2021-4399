"""
Delayed Retry Worker for Food Orders.
Processes messages from 'food-orders-retry', applies backoff,
attempts delivery recovery, and escalates to 'food-orders-dlq' upon exhausting retries.
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_RETRY,
    TOPIC_FOOD_DLQ,
    GROUP_FOOD_RETRY,
    MAX_RETRY_ATTEMPTS,
    RETRY_DELAY_BASE
)
from src.avro_codec import FoodOrderCodec

logger = logging.getLogger("FoodRetryWorker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class FoodRetryWorker:
    """Consumes from retry topic, applies backoff, and escalates to DLQ if max retries exceeded."""

    def __init__(self,
                 bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
                 registry_url: str = SCHEMA_REGISTRY_URL,
                 group_id: str = GROUP_FOOD_RETRY,
                 max_attempts: int = MAX_RETRY_ATTEMPTS,
                 backoff_base: float = RETRY_DELAY_BASE):
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.codec = FoodOrderCodec(registry_url=registry_url)
        self.active = False

        c_conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        self.consumer = Consumer(c_conf)

        p_conf = {
            "bootstrap.servers": self.bootstrap,
            "client.id": "food-retry-producer",
            "acks": "all"
        }
        self.producer = Producer(p_conf)

    def parse_attempt_header(self, headers) -> int:
        if not headers:
            return 1
        for k, v in headers:
            if k in ("x-retry-attempt", "x-retry-count"):
                try:
                    return int(v.decode("utf-8"))
                except Exception:
                    return 1
        return 1

    def forward_to_dlq(self, key: Optional[bytes], value: bytes, attempts: int, cause: str):
        headers = [
            ("x-retry-count", str(attempts).encode("utf-8")),
            ("x-source-topic", TOPIC_FOOD_RETRY.encode("utf-8")),
            ("x-dlq-cause", cause.encode("utf-8")),
            ("x-rejection-info", f"Exhausted {self.max_attempts} kitchen retry cycles".encode("utf-8")),
            ("x-event-time", datetime.now(timezone.utc).isoformat().encode("utf-8"))
        ]
        self.producer.produce(topic=TOPIC_FOOD_DLQ, key=key, value=value, headers=headers)
        self.producer.poll(0)
        logger.error(f"[ESCALATED TO DLQ] Key {key} sent to '{TOPIC_FOOD_DLQ}' after {attempts} attempts.")

    def reschedule_retry(self, key: Optional[bytes], value: bytes, next_attempt: int, reason: str):
        headers = [
            ("x-retry-attempt", str(next_attempt).encode("utf-8")),
            ("x-source-topic", TOPIC_FOOD_RETRY.encode("utf-8")),
            ("x-delay-reason", reason.encode("utf-8")),
            ("x-event-time", datetime.now(timezone.utc).isoformat().encode("utf-8"))
        ]
        self.producer.produce(topic=TOPIC_FOOD_RETRY, key=key, value=value, headers=headers)
        self.producer.poll(0)
        logger.info(f"[RE-SCHEDULED] Re-queued retry cycle {next_attempt}/{self.max_attempts} on '{TOPIC_FOOD_RETRY}'.")

    def handle_retry_record(self, msg):
        raw_val = msg.value()
        raw_key = msg.key()
        attempt = self.parse_attempt_header(msg.headers())

        logger.info(f"[PROCESSING RETRY] Attempt {attempt}/{self.max_attempts} for key {raw_key}...")
        sleep_duration = self.backoff_base * attempt
        logger.info(f"Applying backoff delay: sleeping {sleep_duration:.1f}s...")
        time.sleep(sleep_duration)

        try:
            order = self.codec.decode(raw_val)
        except Exception as e:
            logger.error(f"Cannot decode payload on retry: {e}. Moving to DLQ.")
            self.forward_to_dlq(raw_key, raw_val, attempt, "MalformedPayloadInRetry")
            self.consumer.commit(message=msg, asynchronous=False)
            return

        dish = order.get("product", "")
        # Simulated recovery:
        # If it was a persistent failure, re-attempt until max.
        # Otherwise, kitchen clears after delay and succeeds!
        if attempt < 2 and "PERSISTENT" in dish:
            if attempt + 1 > self.max_attempts:
                self.forward_to_dlq(raw_key, raw_val, attempt, "KitchenUnavailableMaxExceeded")
            else:
                self.reschedule_retry(raw_key, raw_val, attempt + 1, "KitchenStillBusy")
        else:
            clean_dish = dish.replace("KITCHEN_TIMEOUT_", "").replace("PERSISTENT_", "")
            logger.info(
                f"[RETRY SUCCESSFUL] Order {order.get('orderId')} recovered on attempt {attempt}! "
                f"Dish: {clean_dish}, Price: LKR {order.get('price'):.2f}"
            )

        self.consumer.commit(message=msg, asynchronous=False)

    def start(self, max_messages: Optional[int] = None, timeout: float = 1.0):
        self.consumer.subscribe([TOPIC_FOOD_RETRY])
        self.active = True
        logger.info(f"FoodRetryWorker listening on '{TOPIC_FOOD_RETRY}'...")

        count = 0
        try:
            while self.active:
                msg = self.consumer.poll(timeout=timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error in retry worker: {msg.error()}")
                    break
                self.handle_retry_record(msg)
                count += 1
                if max_messages and count >= max_messages:
                    break
        except KeyboardInterrupt:
            logger.info("FoodRetryWorker stopped by user.")
        finally:
            self.stop()

    def stop(self):
        self.active = False
        self.producer.flush(timeout=5.0)
        self.consumer.close()
        logger.info("FoodRetryWorker shutdown complete.")


def main():
    parser = argparse.ArgumentParser(description="Food Order Retry Worker")
    parser.add_argument("--max", type=int, default=None, help="Max retry records to process")
    args = parser.parse_args()

    worker = FoodRetryWorker()
    worker.start(max_messages=args.max)


if __name__ == "__main__":
    main()
