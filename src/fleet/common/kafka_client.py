"""Kafka producer construction.

Single place where producer configuration is assembled, so every setting has one
home and one comment. Rationale for each value is in plan/04 section 3; the two most
likely viva questions are answered inline.
"""

from __future__ import annotations

from typing import Any

from confluent_kafka import Producer

from fleet.common import config


def producer_config(client_id: str) -> dict[str, Any]:
    k = config.kafka()
    return {
        "bootstrap.servers": k.bootstrap,
        # The module taught idempotent producers explicitly. Broker-side dedup by
        # producer id + sequence number gives exactly-once *within a session*.
        "enable.idempotence": True,
        # A no-op at replication.factor=1, but it is the correct production setting
        # and leaving it at 1 would be a latent bug the moment replicas are added.
        "acks": "all",
        "retries": 10,
        "retry.backoff.ms": 200,
        # Safe above 1 *because* idempotence is on: the broker reorders by sequence
        # number, so pipelining cannot break per-partition ordering. Without
        # idempotence this would have to be 1.
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "snappy",
        # At ~240 events/s this batches roughly 5 events - throughput without
        # meaningful latency.
        "linger.ms": 20,
        "batch.size": 65536,
        "queue.buffering.max.messages": 100_000,
        "client.id": client_id,
    }


def build_producer(client_id: str) -> Producer:
    return Producer(producer_config(client_id))
