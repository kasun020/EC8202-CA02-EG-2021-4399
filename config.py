"""
Configuration settings for Kavindu's Real-Time Food Order Streaming System.
Defines Kafka brokers, Schema Registry, and dedicated food-order topics.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_FILE_PATH = os.getenv("SCHEMA_FILE_PATH", str(BASE_DIR / "schemas" / "order.avsc"))

# Kafka & Schema Registry Endpoints
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8086")

# Food Order Streaming Topics
TOPIC_FOOD_ORDERS = os.getenv("TOPIC_FOOD_ORDERS", "food-orders")
TOPIC_FOOD_RETRY = os.getenv("TOPIC_FOOD_RETRY", "food-orders-retry")
TOPIC_FOOD_DLQ = os.getenv("TOPIC_FOOD_DLQ", "food-orders-dlq")

FOOD_SYSTEM_TOPICS = [TOPIC_FOOD_ORDERS, TOPIC_FOOD_RETRY, TOPIC_FOOD_DLQ]

# Consumer Groups
GROUP_FOOD_MAIN = os.getenv("GROUP_FOOD_MAIN", "food-orders-consumer-group")
GROUP_FOOD_RETRY = os.getenv("GROUP_FOOD_RETRY", "food-retry-worker-group")
GROUP_FOOD_DLQ = os.getenv("GROUP_FOOD_DLQ", "food-dlq-auditor-group")

# Fault Tolerance & Exponential Backoff Settings
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_DELAY_BASE = float(os.getenv("RETRY_DELAY_BASE", "1.5"))

# Live Web Dashboard
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8052"))
