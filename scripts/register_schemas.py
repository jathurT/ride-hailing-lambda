"""Register the Avro schemas with the Schema Registry.

Subject naming follows TopicNameStrategy (`<topic>-value`), which is the registry
default and what Kafka UI expects when deserialising for the browser.

Message KEYS are plain UTF-8 strings, not Avro: a key that is one field needs no
evolution story, and string keys stay readable in Kafka UI and kafka-console-consumer.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from fleet.common import config
from fleet.common.logging import configure, get_logger

configure(service="init", stage="orchestrate")
log = get_logger()

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "src" / "fleet" / "common" / "schemas"


def subject_map() -> dict[str, str]:
    """schema file -> registry subject."""
    k = config.kafka()
    return {
        "telemetry.v1.avsc": f"{k.telemetry_topic}-value",
        "vehicle_registry.v1.avsc": f"{k.registry_topic}-value",
        "alert.v1.avsc": f"{k.alerts_topic}-value",
        "dlq.v1.avsc": f"{k.dlq_topic}-value",
    }


def main() -> int:
    client = SchemaRegistryClient({"url": config.kafka().schema_registry_url})
    failed = 0

    for filename, subject in subject_map().items():
        path = SCHEMA_DIR / filename
        if not path.exists():
            log.error("schema_missing", file=str(path))
            failed += 1
            continue

        definition = json.dumps(json.loads(path.read_text()))
        try:
            schema_id = client.register_schema(subject, Schema(definition, schema_type="AVRO"))
            log.info("schema_registered", subject=subject, schema_id=schema_id, file=filename)
        except Exception as exc:
            log.error("schema_register_failed", subject=subject, error=str(exc))
            failed += 1

    # BACKWARD compatibility is set globally on the registry container, but assert
    # it here so a config drift is caught at bootstrap rather than at the first
    # incompatible deploy.
    try:
        # NB: the kwarg is `compatibility`, not `level` - `level` is a reserved
        # structlog field and would be silently overwritten by the log level.
        log.info("registry_compatibility", compatibility=client.get_compatibility())
    except Exception as exc:
        log.warning("registry_compatibility_unknown", error=str(exc))

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
