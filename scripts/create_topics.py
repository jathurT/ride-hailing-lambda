"""Create the Kafka topics declaratively.

Topics are never auto-created (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`), because a
topic that appears by accident gets default settings - and for the compacted
registry topic the default policy would be *wrong* in a way nothing would report.

Idempotent: re-running is a no-op.

Design rationale for every number here is in plan/04 section 1.
"""

from __future__ import annotations

import sys

from confluent_kafka.admin import AdminClient, NewTopic

from fleet.common import config
from fleet.common.logging import configure, get_logger

configure(service="init", stage="orchestrate")
log = get_logger()


def topic_specs() -> list[NewTopic]:
    k = config.kafka()
    rf = k.replication_factor
    return [
        # The raw event stream. Three independent consumer groups read it.
        # 6 partitions: caps consumer parallelism (we run 4 Spark tasks, with
        # headroom to 6 without re-partitioning a live topic), and spreads 150
        # vehicle keys ~25 per partition so nothing hotspots.
        NewTopic(
            k.telemetry_topic,
            num_partitions=k.telemetry_partitions,
            replication_factor=rf,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str(k.telemetry_retention_ms),
                "retention.bytes": str(k.telemetry_retention_bytes),
                "compression.type": "snappy",
            },
        ),
        # LOG COMPACTED. Vehicle reference data is a *table*, not an event history:
        # we want the current value per vehicle, forever. Reading from earliest
        # materialises it. This is the textbook compaction case.
        NewTopic(
            k.registry_topic,
            num_partitions=k.registry_partitions,
            replication_factor=rf,
            config={
                "cleanup.policy": "compact",
                "min.cleanable.dirty.ratio": "0.1",
                "segment.ms": "60000",
            },
        ),
        # Business alerts. Deliberately NOT compacted - an alert history is a log;
        # keeping only the latest alert per vehicle would erase the incident record.
        NewTopic(
            k.alerts_topic,
            num_partitions=k.alerts_partitions,
            replication_factor=rf,
            config={"cleanup.policy": "delete", "retention.ms": "604800000"},
        ),
        # Dead letters. Single partition: low volume, and ordering across the whole
        # DLQ is more useful than parallelism when you are debugging.
        NewTopic(
            k.dlq_topic,
            num_partitions=1,
            replication_factor=rf,
            config={"cleanup.policy": "delete", "retention.ms": "1209600000"},
        ),
    ]


def main() -> int:
    k = config.kafka()
    admin = AdminClient({"bootstrap.servers": k.bootstrap})

    existing = set(admin.list_topics(timeout=15).topics)
    wanted = topic_specs()
    to_create = [t for t in wanted if t.topic not in existing]

    for t in wanted:
        if t.topic in existing:
            log.info("topic_exists", topic=t.topic)

    if not to_create:
        log.info("topics_ready", created=0, total=len(wanted))
        return 0

    failed = 0
    for topic, fut in admin.create_topics(to_create).items():
        try:
            fut.result()
            spec = next(t for t in to_create if t.topic == topic)
            log.info(
                "topic_created",
                topic=topic,
                partitions=spec.num_partitions,
                cleanup_policy=spec.config.get("cleanup.policy"),
            )
        except Exception as exc:
            log.error("topic_create_failed", topic=topic, error=str(exc))
            failed += 1

    log.info("topics_ready", created=len(to_create) - failed, total=len(wanted))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
