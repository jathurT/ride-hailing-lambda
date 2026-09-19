"""Create the lake buckets and apply the mart schema. Idempotent.

Runs inside the init container, after the storage services report healthy.
"""

from __future__ import annotations

import sys
from pathlib import Path

from fleet.common import config
from fleet.common.logging import get_logger
from fleet.store.lake import PREFIXES, ensure_bucket

log = get_logger()

DDL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ddl"


def s3_client():
    import boto3

    s = config.storage()
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_access_key,
        aws_secret_access_key=s.minio_secret_key,
        region_name="us-east-1",
    )


def init_lake() -> None:
    s = config.storage()
    created = ensure_bucket(s3_client(), s.lake_bucket)
    log.info("lake_ready", bucket=s.lake_bucket, created=created, prefixes=len(PREFIXES))


def init_mart() -> None:
    import psycopg

    s = config.storage()
    files = sorted(DDL_DIR.glob("*.sql"))
    if not files:
        log.error("no_ddl_found", dir=str(DDL_DIR))
        raise SystemExit(1)

    with psycopg.connect(s.postgres_dsn, autocommit=True) as conn:
        for f in files:
            conn.execute(f.read_text())
            log.info("ddl_applied", file=f.name)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'mart'"
            )
            tables = cur.fetchone()[0]
    log.info("mart_ready", schema="mart", tables=tables)


def seed_dimensions() -> None:
    """Populate the static dimensions. Facts arrive from the batch layer."""
    import psycopg

    from fleet.common.zones import ZONES

    s = config.storage()
    with psycopg.connect(s.postgres_dsn, autocommit=True) as conn, conn.cursor() as cur:
        # psycopg3 puts executemany on the cursor, not the connection.
        cur.executemany(
            """INSERT INTO mart.dim_zone (zone_id, zone_name, zone_class, demand_weight)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (zone_id) DO UPDATE
                 SET zone_name = EXCLUDED.zone_name,
                     zone_class = EXCLUDED.zone_class,
                     demand_weight = EXCLUDED.demand_weight""",
            [(z.zone_id, z.name, str(z.zone_class), z.demand_weight) for z in ZONES],
        )
        cur.execute("SELECT count(*) FROM mart.dim_zone")
        n = cur.fetchone()[0]
    log.info("dimensions_seeded", dim_zone_rows=n)


def check_redis() -> None:
    """The speed view holds no schema, but a reachability check at bootstrap beats
    discovering it is down from a failed micro-batch three minutes later."""
    import redis

    s = config.storage()
    client = redis.from_url(s.redis_url)
    client.ping()
    log.info("speed_view_ready", url=s.redis_url, persistence="disabled by design")


def main() -> int:
    init_lake()
    init_mart()
    seed_dimensions()
    check_redis()
    return 0


if __name__ == "__main__":
    sys.exit(main())
