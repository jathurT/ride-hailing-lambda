"""Seed the log-compacted vehicle registry topic.

Runs once at start-up and exits. The topic is a *table*, not an event history: we
want the current record per vehicle, forever, which is precisely what compaction
gives (`cleanup.policy=compact`, "keeps only the latest record per key").

The streaming job reads this topic from `earliest` and broadcasts it as a static
DataFrame for stream-static enrichment (plan/05 section 3.2).
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from fleet.common import config
from fleet.common.kafka_client import build_producer
from fleet.common.logging import configure, get_logger
from fleet.common.serialization import serialize_key, serialize_value
from fleet.common.simclock import read_anchor
from fleet.ingestion.generators.fleet import build_fleet, registry_records

SCHEMA_FILE = "vehicle_registry.v1.avsc"


def main() -> int:
    obs = config.observability()
    configure(
        service="registry-producer", stage="ingest", level=obs.log_level, json_output=obs.log_json
    )
    log = get_logger()

    sim, fleet_cfg, kafka_cfg = config.sim(), config.fleet(), config.kafka()
    clock = read_anchor(Path(sim.state_path))

    vehicles = build_fleet(fleet_cfg.size, fleet_cfg.random_seed, sim.epoch_sim)
    records = registry_records(vehicles, onboarded=date(2025, 1, 1))

    producer = build_producer("registry-producer")
    topic = kafka_cfg.registry_topic
    failures = 0

    def report(err: object, _msg: object) -> None:
        nonlocal failures
        if err is not None:
            failures += 1
            log.error("registry_delivery_failed", error=str(err))

    for rec in records:
        producer.produce(
            topic,
            key=serialize_key(str(rec["vehicle_id"]), topic),
            value=serialize_value(rec, topic, kafka_cfg.schema_registry_url, SCHEMA_FILE),
            on_delivery=report,
        )
    producer.flush(timeout=30)

    log.info(
        "registry_seeded",
        topic=topic,
        vehicles=len(records),
        failures=failures,
        sim_day=clock.sim_day_index(),
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
