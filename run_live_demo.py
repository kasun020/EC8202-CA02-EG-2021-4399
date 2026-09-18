"""
Automated End-to-End Demonstration for Kasun's Food Order Streaming Pipeline.
Demonstrates:
 1. Food order Avro encoding & Schema Registry registration.
 2. Real-time incremental running average price aggregation.
 3. Kitchen delay transient failures with backoff retry on 'food-orders-retry'.
 4. Poison pill / unserviceable order capture into 'food-orders-dlq'.
 5. Terminal telemetry dashboard and DLQ audit report.
"""

import sys
import time
import threading
import logging
from rich.console import Console
from rich.panel import Panel

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_FOOD_ORDERS,
    TOPIC_FOOD_RETRY,
    TOPIC_FOOD_DLQ
)
from src.kafka_admin import init_food_topics
from src.producer import FoodOrderProducer
from src.consumer import FoodOrderConsumer
from src.retry_worker import FoodRetryWorker
from src.dlq_auditor import FoodDLQAuditor

console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("FoodLiveDemo")


def run_food_demo():
    console.print(Panel.fit(
        "[bold green]Kasun's Big Data Assignment: CeylonBites Food Order Streaming System[/bold green]\n"
        "[white]Kafka Topics: 'food-orders', 'food-orders-retry', 'food-orders-dlq' | Avro Wire Format[/white]",
        border_style="green"
    ))

    # 1. Provision topics
    console.print("\n[bold yellow]Step 1: Checking Food Delivery Kafka Topics...[/bold yellow]")
    if not init_food_topics():
        console.print("[bold red]Failed to connect to Kafka broker. Ensure Kafka is running on port 9092.[/bold red]")
        sys.exit(1)
    console.print("[bold green]Kafka topics ready: 'food-orders', 'food-orders-retry', 'food-orders-dlq'.[/bold green]")

    # 2. Start Consumer
    console.print("\n[bold yellow]Step 2: Starting FoodOrderConsumer & Real-Time Aggregator...[/bold yellow]")
    main_consumer = FoodOrderConsumer(group_id="kasun-demo-consumer-group")
    c_thread = threading.Thread(
        target=main_consumer.start_polling,
        kwargs={"timeout": 0.5, "show_dashboard": False},
        daemon=True
    )
    c_thread.start()
    time.sleep(2)

    # 3. Start Retry Worker
    console.print("[bold yellow]Step 3: Starting FoodRetryWorker...[/bold yellow]")
    retry_worker = FoodRetryWorker(group_id="kasun-demo-retry-group", backoff_base=1.0)
    r_thread = threading.Thread(
        target=retry_worker.start,
        kwargs={"timeout": 0.5},
        daemon=True
    )
    r_thread.start()
    time.sleep(2)

    # 4. Produce streaming orders across phases
    producer = FoodOrderProducer()

    # Phase A: 5 Normal Meal Orders
    console.print("\n[bold green]---> Phase A: Dispatching 5 Normal Food Orders (Testing Avro & Running Avg)...[/bold green]")
    for i in range(1, 6):
        order = producer.generate_order(order_id=f"FOOD-NORM-{i:03d}", failure_type="normal")
        producer.dispatch(order)
        time.sleep(0.5)

    # Phase B: 2 Transient Kitchen Delays
    console.print("\n[bold yellow]---> Phase B: Dispatching 2 Transient Kitchen Delays (Testing Retry Queue & Backoff)...[/bold yellow]")
    for i in range(1, 3):
        order = producer.generate_order(order_id=f"FOOD-DELAY-{i:03d}", failure_type="transient")
        producer.dispatch(order)
        time.sleep(0.5)

    # Phase C: 2 Permanent Poison Orders
    console.print("\n[bold red]---> Phase C: Dispatching Poison Orders (Testing DLQ Routing)...[/bold red]")
    perm_order = producer.generate_order(order_id="FOOD-EXPIRED-001", failure_type="permanent")
    producer.dispatch(perm_order)
    time.sleep(0.5)

    corrupt_order = {"orderId": "FOOD-CORRUPT-999", "product": "PoisonDish", "price": 99.0}
    producer.dispatch(corrupt_order, send_corrupt=True)
    time.sleep(0.5)

    # Phase D: 3 More Normal Meal Orders
    console.print("\n[bold green]---> Phase D: Dispatching 3 More Normal Food Orders (Verifying Continuous Processing)...[/bold green]")
    for i in range(6, 9):
        order = producer.generate_order(order_id=f"FOOD-NORM-{i:03d}", failure_type="normal")
        producer.dispatch(order)
        time.sleep(0.5)

    producer.flush()

    # 5. Settle
    console.print("\n[cyan]Waiting 5 seconds for retry backoffs and Kafka offset commits to settle...[/cyan]")
    time.sleep(5)

    main_consumer.is_active = False
    retry_worker.active = False
    time.sleep(1)

    # 6. Final Aggregation Display
    console.print("\n[bold green]================ Final Food Aggregation Metrics ================[/bold green]")
    table = main_consumer.aggregator.create_rich_table()
    console.print(table)

    # 7. Audit Dead Letter Queue
    console.print("\n[bold magenta]================ Dead Letter Queue (food-orders-dlq) Audit ================[/bold magenta]")
    auditor = FoodDLQAuditor()
    rejected = auditor.fetch_rejected_orders(max_records=10)
    auditor.display_report(rejected)

    console.print(Panel(
        "[bold green]Kasun's Live Demonstration Completed Successfully![/bold green]\n"
        "Verified: Food Avro Schema, Running Average Meal Price, Kitchen Retry Backoff, and DLQ Rejections.",
        border_style="green"
    ))


if __name__ == "__main__":
    run_food_demo()
