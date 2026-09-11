"""
Dead Letter Queue (DLQ) Auditor for Food Order Stream.
Inspects 'food-orders-dlq' and formats diagnostic headers and payloads.
"""

import argparse
import logging
import time
from typing import List, Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_DLQ,
    GROUP_FOOD_DLQ
)
from src.avro_codec import FoodOrderCodec

logger = logging.getLogger("FoodDLQAuditor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
console = Console()


class FoodDLQAuditor:
    """Reads and inspects rejected food orders from the Dead Letter Queue."""

    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
                 registry_url: str = SCHEMA_REGISTRY_URL,
                 group_id: str = GROUP_FOOD_DLQ):
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.codec = FoodOrderCodec(registry_url=registry_url)

    def fetch_rejected_orders(self, max_records: int = 50) -> List[Dict[str, Any]]:
        unique_group = f"{self.group_id}-{int(time.time())}"
        conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": unique_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        }
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC_FOOD_DLQ])

        records = []
        try:
            logger.info(f"Connecting to '{TOPIC_FOOD_DLQ}' to inspect rejected meal orders...")
            empty_count = 0
            while len(records) < max_records:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    empty_count += 1
                    if empty_count >= 3:
                        break
                    continue
                empty_count = 0

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    logger.error(f"Kafka error reading DLQ: {msg.error()}")
                    break

                headers = {}
                if msg.headers():
                    for k, v in msg.headers():
                        headers[k] = v.decode("utf-8", errors="replace") if v else ""

                raw_v = msg.value()
                decoded_payload = None
                decode_err = None
                try:
                    decoded_payload = self.codec.decode(raw_v)
                except Exception as e:
                    decode_err = str(e)
                    decoded_payload = f"<Raw Corrupt Bytes: {raw_v[:25]!r}...>"

                item = {
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8", errors="replace") if msg.key() else "N/A",
                    "cause": headers.get("x-dlq-cause", "Unknown"),
                    "info": headers.get("x-rejection-info", "None"),
                    "retries": headers.get("x-retry-count", "0"),
                    "source_topic": headers.get("x-source-topic", "food-orders"),
                    "payload": decoded_payload,
                    "decode_error": decode_err
                }
                records.append(item)
        finally:
            consumer.close()

        return records

    def display_report(self, records: Optional[List[Dict[str, Any]]] = None):
        if records is None:
            records = self.fetch_rejected_orders()

        if not records:
            console.print(Panel("[green]Food Order DLQ is clear. No rejected transactions.[/green]", title="DLQ Status"))
            return

        table = Table(title=f"Dead Letter Queue (food-orders-dlq) - Rejected Orders: {len(records)}", style="bold red")
        table.add_column("Offset", style="dim", width=8)
        table.add_column("Order ID", style="bold cyan", width=14)
        table.add_column("Rejection Cause", style="bold yellow", width=25)
        table.add_column("Retries", style="magenta", width=8)
        table.add_column("Origin Topic", style="blue", width=18)
        table.add_column("Payload & Diagnostic Info", style="white")

        for r in records:
            p_str = str(r["payload"])
            details = f"{p_str}\n[dim red]Diagnostic: {r['info']}[/dim red]"
            table.add_row(
                str(r["offset"]),
                str(r["key"]),
                r["cause"],
                str(r["retries"]),
                r["source_topic"],
                details
            )

        console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Audit Food Order DLQ")
    parser.add_argument("--max", type=int, default=50, help="Max records to audit")
    args = parser.parse_args()

    auditor = FoodDLQAuditor()
    recs = auditor.fetch_rejected_orders(max_records=args.max)
    auditor.display_report(recs)


if __name__ == "__main__":
    main()
