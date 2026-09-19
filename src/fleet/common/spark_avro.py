"""Decoding Confluent-framed Avro inside Spark.

Confluent's serializer does not emit bare Avro. Every message value is:

    byte 0        magic byte, always 0x00
    bytes 1..4    schema id, big-endian int32
    bytes 5..     the Avro payload

Spark's `from_avro` expects the payload alone, so the five-byte header must be
stripped first. Getting this wrong does not raise a helpful error - `from_avro`
returns a row of nulls, and the pipeline quietly produces empty aggregates.

We deliberately do NOT look the schema id up in the registry per row. The reader
schema is loaded once from the same `.avsc` file the producer registered, and Avro's
resolution rules handle a producer running a compatible older writer schema. Since
the registry is configured BACKWARD-compatible (plan/04 §2), a reader on the current
schema can always read what an older producer wrote.

The alternative - a Python UDF calling the Confluent deserialiser per row - would
force JVM/Python serialisation for every event and defeat Catalyst. See plan/05 §3.2
for the same argument applied to zone lookup.
"""

from __future__ import annotations

from typing import Any

CONFLUENT_HEADER_BYTES = 5


def strip_confluent_header(column: str = "value") -> Any:
    """Drop the 5-byte magic + schema-id prefix from a Kafka value column.

    Spark's `substring` is 1-indexed, so position 6 is the first payload byte.
    Expressed as a SQL expression rather than Column.substr() because substr()
    requires both arguments to be the same type, and the length here is itself a
    column.
    """
    from pyspark.sql.functions import expr

    start = CONFLUENT_HEADER_BYTES + 1
    return expr(f"substring({column}, {start}, length({column}) - {CONFLUENT_HEADER_BYTES})")


def deserialize_avro_value(df: Any, schema_json: str, output_col: str = "payload") -> Any:
    """Add a struct column decoded from the Kafka `value` column.

    Rows that fail to decode become null rather than killing the batch: a single
    corrupt message must not stop the stream. `mode=PERMISSIVE` is the default, and
    the caller is expected to filter nulls into the DLQ so the failure stays visible.
    """
    from pyspark.sql.avro.functions import from_avro

    payload = strip_confluent_header("value")
    return df.withColumn(output_col, from_avro(payload, schema_json, {"mode": "PERMISSIVE"}))


def confluent_envelope(payload_col: Any, schema_id: int) -> Any:
    """Wrap a bare Avro payload in Confluent's wire format for writing back to Kafka.

    The inverse of `strip_confluent_header`. Without this, anything we produce from
    Spark is unreadable by Kafka UI, by the Confluent deserialiser, and by any
    consumer that expects registry-framed messages - so the DLQ would become a
    write-only store that nobody can inspect, which defeats its purpose.

    The schema id is looked up once at job start-up rather than hard-coded, because
    it is assigned by the registry and differs between a fresh stack and a rebuilt one.
    """
    from pyspark.sql.functions import concat, lit, unhex

    # magic byte 0x00 followed by the schema id as a big-endian int32
    header = format(0, "02x") + format(schema_id, "08x")
    return concat(unhex(lit(header)), payload_col)


def registered_schema_id(registry_url: str, subject: str) -> int:
    """Current schema id for a subject, from the registry."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient({"url": registry_url}).get_latest_version(subject).schema_id
