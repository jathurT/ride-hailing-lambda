"""Avro serialization against the Schema Registry.

Message VALUES are Avro; message KEYS are plain UTF-8 strings. A key that is one
field needs no evolution story, and string keys stay readable in Kafka UI and in
kafka-console-consumer - which matters because Avro's real cost is that you cannot
`cat` a message (plan/02 section 10).
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, StringSerializer

SCHEMA_DIR = Path(__file__).parent / "schemas"

_string_serializer = StringSerializer("utf_8")


@lru_cache(maxsize=8)
def load_schema(filename: str) -> str:
    """Read a schema file. Cached - these never change at runtime."""
    return json.dumps(json.loads((SCHEMA_DIR / filename).read_text()))


@lru_cache(maxsize=1)
def registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


@lru_cache(maxsize=8)
def avro_serializer(url: str, schema_file: str) -> AvroSerializer:
    return AvroSerializer(registry_client(url), load_schema(schema_file))


def serialize_key(key: str, topic: str) -> bytes:
    return _string_serializer(key, SerializationContext(topic, "key"))


def serialize_value(value: dict[str, Any], topic: str, url: str, schema_file: str) -> bytes:
    return avro_serializer(url, schema_file)(value, SerializationContext(topic, "value"))
