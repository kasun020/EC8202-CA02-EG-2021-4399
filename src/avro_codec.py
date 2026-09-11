"""
Avro Serialization & Deserialization Codec for Food Order Transactions.
Implements Confluent Schema Registry framing with binary Avro payloads.
"""

import io
import json
import struct
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import requests
import fastavro

from config import SCHEMA_FILE_PATH, SCHEMA_REGISTRY_URL, TOPIC_FOOD_ORDERS

logger = logging.getLogger("FoodOrderCodec")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class FoodOrderSerializationException(Exception):
    """Raised when food order encoding or validation fails."""
    pass


class FoodOrderDeserializationException(Exception):
    """Raised when food order decoding fails or binary payload is corrupt."""
    pass


class FoodOrderCodec:
    """
    Serializes and deserializes food order records adhering to:
    [0x00 magic byte][4-byte big-endian Schema ID][Fastavro binary payload]
    """

    def __init__(self, schema_file: str = SCHEMA_FILE_PATH, registry_url: str = SCHEMA_REGISTRY_URL):
        self.schema_file = schema_file
        self.registry_url = registry_url
        self.raw_schema_json = self._read_schema(schema_file)
        self.parsed_schema = fastavro.parse_schema(self.raw_schema_json)
        self.schema_id = 1

    def _read_schema(self, path: str) -> Dict[str, Any]:
        file_path = Path(path)
        if not file_path.is_file():
            raise FileNotFoundError(f"Missing Avro schema at {path}")
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def register_schema(self, subject: Optional[str] = None) -> int:
        """Registers the schema with Schema Registry, returning the assigned schema ID."""
        subject = subject or f"{TOPIC_FOOD_ORDERS}-value"
        endpoint = f"{self.registry_url.rstrip('/')}/subjects/{subject}/versions"
        body = {"schema": json.dumps(self.raw_schema_json)}
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

        try:
            res = requests.post(endpoint, json=body, headers=headers, timeout=3.0)
            if res.status_code in (200, 201):
                self.schema_id = res.json().get("id", 1)
                logger.info(f"Registered schema for subject '{subject}'. Assigned Schema ID: {self.schema_id}")
            else:
                logger.warning(f"Schema Registry warning ({res.status_code}): {res.text}. Falling back to ID={self.schema_id}")
        except Exception as err:
            logger.warning(f"Failed to connect to Schema Registry at {endpoint}: {err}. Defaulting to ID={self.schema_id}")

        return self.schema_id

    def validate(self, order: Dict[str, Any]) -> None:
        """Ensures the order dictionary complies with business requirements."""
        if not isinstance(order, dict):
            raise FoodOrderSerializationException("Order record must be a dictionary.")

        for required in ("orderId", "product", "price"):
            if required not in order:
                raise FoodOrderSerializationException(f"Missing mandatory field '{required}' in order.")

        if not isinstance(order["orderId"], str) or not order["orderId"].strip():
            raise FoodOrderSerializationException("Field 'orderId' must be a non-empty string.")

        if not isinstance(order["product"], str) or not order["product"].strip():
            raise FoodOrderSerializationException("Field 'product' must be a non-empty string.")

        try:
            p = float(order["price"])
            if p < 0:
                raise FoodOrderSerializationException(f"Negative price {p} is invalid for a food order.")
        except (ValueError, TypeError) as err:
            raise FoodOrderSerializationException(f"Invalid price value: {err}")

    def encode(self, order: Dict[str, Any]) -> bytes:
        """Encodes an order dictionary into Avro binary with Confluent wire format."""
        try:
            self.validate(order)
            record = {
                "orderId": str(order["orderId"]),
                "product": str(order["product"]),
                "price": float(order["price"])
            }
            bio = io.BytesIO()
            fastavro.schemaless_writer(bio, self.parsed_schema, record)
            payload = bio.getvalue()

            # Wire format: magic byte 0 + 4-byte big-endian schema ID
            prefix = struct.pack(">bI", 0, self.schema_id)
            return prefix + payload
        except FoodOrderSerializationException:
            raise
        except Exception as e:
            raise FoodOrderSerializationException(f"Avro encoding failed: {e}") from e

    def decode(self, raw_bytes: bytes) -> Dict[str, Any]:
        """Decodes Avro binary bytes into an order dictionary."""
        if not raw_bytes or not isinstance(raw_bytes, (bytes, bytearray)):
            raise FoodOrderDeserializationException("Raw payload must be non-empty bytes.")

        bio = io.BytesIO(raw_bytes)
        try:
            # Check for wire format header
            if raw_bytes[0] == 0 and len(raw_bytes) > 5:
                bio.seek(5)
            else:
                bio.seek(0)
            return fastavro.schemaless_reader(bio, self.parsed_schema)
        except Exception as err:
            raise FoodOrderDeserializationException(f"Failed to decode Avro food order payload: {err}") from err
