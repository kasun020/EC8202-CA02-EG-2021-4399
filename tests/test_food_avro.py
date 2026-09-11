"""
Unit tests for Food Order Avro Schema and Codec.
"""

import pytest
from src.avro_codec import FoodOrderCodec, FoodOrderSerializationException, FoodOrderDeserializationException


@pytest.fixture
def codec():
    return FoodOrderCodec()


def test_food_schema_structure(codec):
    """Verify that order.avsc defines required orderId, product, and price fields."""
    assert codec.parsed_schema is not None
    names = [f["name"] for f in codec.raw_schema_json["fields"]]
    assert "orderId" in names
    assert "product" in names
    assert "price" in names


def test_food_order_codec_roundtrip(codec):
    """Verify encoding and decoding fidelity with Confluent wire format."""
    meal_order = {
        "orderId": "FOOD-2001",
        "product": "Truffle Mushroom Burger",
        "price": 24.50
    }
    encoded = codec.encode(meal_order)
    assert isinstance(encoded, bytes)
    assert len(encoded) > 5
    assert encoded[0] == 0  # Magic byte

    decoded = codec.decode(encoded)
    assert decoded["orderId"] == meal_order["orderId"]
    assert decoded["product"] == meal_order["product"]
    assert abs(decoded["price"] - meal_order["price"]) < 0.001


def test_invalid_food_orders(codec):
    """Verify missing fields and negative prices are rejected."""
    with pytest.raises(FoodOrderSerializationException):
        codec.encode({"product": "Burger", "price": 10.0})

    with pytest.raises(FoodOrderSerializationException):
        codec.encode({"orderId": "FOOD-1", "product": "", "price": 10.0})

    with pytest.raises(FoodOrderSerializationException):
        codec.encode({"orderId": "FOOD-1", "product": "Burger", "price": -5.0})


def test_corrupt_payload_handling(codec):
    """Verify that malformed bytes raise FoodOrderDeserializationException."""
    bad_bytes = b"\x00\x00\x00\x01\xDE\xAD\xBE\xEF_BAD_DATA"
    with pytest.raises(FoodOrderDeserializationException):
        codec.decode(bad_bytes)
