"""
Unit tests for StreamingMetricsAggregator and Food Retry Worker logic.
"""

import pytest
from src.consumer import StreamingMetricsAggregator
from src.retry_worker import FoodRetryWorker


def test_running_average_incremental_calculation():
    """Verify incremental running average formula accuracy."""
    agg = StreamingMetricsAggregator()
    assert agg.running_average == 0.0

    # Meal 1: $20.00 -> avg $20.00
    m1 = agg.add_completed_order({"orderId": "1", "product": "Burger", "price": 20.0})
    assert m1["completed_orders"] == 1
    assert m1["running_average_price"] == 20.00
    assert m1["min_price"] == 20.00
    assert m1["max_price"] == 20.00

    # Meal 2: $40.00 -> avg $30.00
    m2 = agg.add_completed_order({"orderId": "2", "product": "Pizza", "price": 40.0})
    assert m2["completed_orders"] == 2
    assert m2["running_average_price"] == 30.00
    assert m2["min_price"] == 20.00
    assert m2["max_price"] == 40.00

    # Meal 3: $60.00 -> avg $40.00
    m3 = agg.add_completed_order({"orderId": "3", "product": "Omakase", "price": 60.0})
    assert m3["completed_orders"] == 3
    assert m3["running_average_price"] == 40.00
    assert m3["total_revenue"] == 120.00


def test_dish_breakdown_statistics():
    """Verify dish-level aggregation alongside running totals."""
    agg = StreamingMetricsAggregator()
    agg.add_completed_order({"orderId": "1", "product": "Ramen", "price": 25.0})
    agg.add_completed_order({"orderId": "2", "product": "Sushi", "price": 50.0})
    agg.add_completed_order({"orderId": "3", "product": "Ramen", "price": 35.0})

    stats = agg.dish_summary
    assert "Ramen" in stats
    assert stats["Ramen"]["orders"] == 2
    assert stats["Ramen"]["sum"] == 60.0
    assert stats["Ramen"]["avg"] == 30.0


def test_retry_attempt_header_parsing():
    """Verify header extraction from Kafka messages in retry worker."""
    worker = FoodRetryWorker(bootstrap="localhost:9092")
    headers = [("x-retry-attempt", b"2")]
    assert worker.parse_attempt_header(headers) == 2
    assert worker.parse_attempt_header([]) == 1
    assert worker.parse_attempt_header(None) == 1
