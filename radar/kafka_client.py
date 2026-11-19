"""Real librdkafka clients, shared authentication and explicit topic creation."""
from __future__ import annotations

import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from radar.config import Settings

LOG = logging.getLogger(__name__)


def client_config(settings: Settings) -> dict:
    cfg: dict = {"bootstrap.servers": settings.kafka_bootstrap_servers,
                 "reconnect.backoff.ms": 500, "reconnect.backoff.max.ms": 5000}
    fields = {
        "KAFKA_SECURITY_PROTOCOL": "security.protocol",
        "KAFKA_SASL_MECHANISM": "sasl.mechanism",
        "KAFKA_SASL_USERNAME": "sasl.username",
        "KAFKA_SASL_PASSWORD": "sasl.password",
        "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
    }
    for env, key in fields.items():
        if os.getenv(env):
            cfg[key] = os.environ[env]
    return cfg


def make_producer(settings: Settings) -> Producer:
    return Producer({
        **client_config(settings),
        "client.id": "ai-pain-point-producer",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "message.timeout.ms": 60000,
        "linger.ms": 10,
    })


def make_consumer(settings: Settings, group: str) -> Consumer:
    return Consumer({
        **client_config(settings),
        "client.id": f"radar-{group}",
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
        "isolation.level": "read_committed",
    })


def ensure_topic(settings: Settings, attempts: int = 20) -> None:
    """Wait for Kafka and create the domain topic; never silently change brokers."""
    for attempt in range(attempts):
        try:
            admin = AdminClient(client_config(settings))
            metadata = admin.list_topics(timeout=8)
            if settings.kafka_topic in metadata.topics:
                error = metadata.topics[settings.kafka_topic].error
                if error is None:
                    return
            future = admin.create_topics([
                NewTopic(settings.kafka_topic, num_partitions=3, replication_factor=1,
                         config={"retention.ms": "604800000"})
            ])[settings.kafka_topic]
            try:
                future.result(timeout=20)
            except Exception as exc:
                inner = exc.args[0] if exc.args else None
                if not hasattr(inner, "code") or inner.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
            LOG.info("Kafka topic is ready: %s", settings.kafka_topic)
            return
        except Exception as exc:
            if attempt + 1 == attempts:
                raise RuntimeError("Kafka is unavailable or topic creation was denied") from exc
            LOG.warning("Waiting for Kafka (%s/%s)", attempt + 1, attempts)
            time.sleep(min(2 + attempt, 10))
