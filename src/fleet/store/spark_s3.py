"""Spark configuration for talking to MinIO over the s3a:// connector.

Isolated in one place because getting it wrong is the single most reliable way to
lose an afternoon on this project: the symptom is a ClassNotFoundException or a
403 at the first write, long after the job has otherwise started fine.

Three settings are non-obvious and all three are required against MinIO:

  fs.s3a.endpoint            point at MinIO instead of AWS
  fs.s3a.path.style.access   MinIO serves buckets as a path, not a subdomain;
                             without this the client resolves `bucket.minio` and
                             fails DNS
  fs.s3a.connection.ssl.enabled=false   MinIO here is plain HTTP
"""

from __future__ import annotations

from typing import Any

from fleet.common import config


def s3a_options() -> dict[str, str]:
    s = config.storage()
    endpoint = s.minio_endpoint.replace("http://", "").replace("https://", "")
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": s.minio_access_key,
        "spark.hadoop.fs.s3a.secret.key": s.minio_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # Write directly rather than staging in a temp prefix and renaming. Object
        # stores have no atomic rename, so the default committer copies every file
        # a second time at commit.
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    }


def build_session(app_name: str, master: str = "local[4]", **extra: str) -> Any:
    """A SparkSession configured for the lake.

    `local[4]` by default - see ADR-006. Structured Streaming semantics are
    identical in local mode; only executor distribution differs.
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in {**s3a_options(), **extra}.items():
        builder = builder.config(key, value)
    # 200 shuffle partitions is the default and is badly wrong at this scale: it
    # creates 200 near-empty tasks whose scheduling dominates the runtime.
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    # Pin the timezone rather than inheriting the image's default. `sim_date` is
    # derived with to_date(event_time), so a non-UTC session would shift partition
    # boundaries by the local offset and file events under the wrong simulated day
    # - silently, and only visibly wrong at the day boundary. The container happens
    # to default to UTC today; that is not something to depend on.
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    return builder.getOrCreate()
