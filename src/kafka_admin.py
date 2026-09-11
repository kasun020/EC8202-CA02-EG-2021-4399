"""
Kafka Admin Utility to provision Food Order topics on the cluster.
"""

import logging
from typing import List
from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_BOOTSTRAP_SERVERS, FOOD_SYSTEM_TOPICS

logger = logging.getLogger("FoodKafkaAdmin")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def init_food_topics(
    bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
    topics: List[str] = FOOD_SYSTEM_TOPICS,
    partitions: int = 1,
    replicas: int = 1
) -> bool:
    """Verifies that all food delivery Kafka topics exist, creating any that are absent."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        cluster_metadata = admin.list_topics(timeout=10.0)
        current = set(cluster_metadata.topics.keys())
        logger.info(f"Connected to Kafka broker at {bootstrap}. Existing topics: {current}")

        to_create = [
            NewTopic(name, num_partitions=partitions, replication_factor=replicas)
            for name in topics if name not in current
        ]

        if not to_create:
            logger.info(f"All required food order topics {topics} already exist.")
            return True

        logger.info(f"Creating food streaming topics: {[t.topic for t in to_create]}...")
        futures = admin.create_topics(to_create)
        for t_name, f in futures.items():
            f.result()
            logger.info(f"Topic '{t_name}' created successfully.")
        return True
    except Exception as ex:
        logger.error(f"Error provisioning Kafka topics: {ex}")
        return False


if __name__ == "__main__":
    init_food_topics()
