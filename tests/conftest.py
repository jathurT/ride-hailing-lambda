"""Shared test fixtures.

The Spark fixture is session-scoped because starting a JVM costs ~5 seconds and
these tests would otherwise be too slow to run habitually - and a test suite people
avoid running is worth very little.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Spark 3.5 supports Java 8/11/17. This host's default is Java 21, which fails with
# obscure reflection errors rather than a clear message, so pin an supported JVM if
# one is present. Inside the containers this is irrelevant - the image ships 17.
_JAVA_11 = Path("/usr/lib/jvm/java-11-openjdk-amd64")
_JAVA_17 = Path("/usr/lib/jvm/java-17-openjdk-amd64")
for candidate in (_JAVA_17, _JAVA_11):
    if candidate.exists():
        os.environ.setdefault("JAVA_HOME", str(candidate))
        break


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession for transform tests.

    `local[2]` with 2 shuffle partitions: the 200 default creates 200 near-empty
    tasks whose scheduling dominates the runtime of a test operating on ten rows.
    """
    pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("fleet-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # The JVM's default timezone must be UTC too, not just the SQL session's.
        # collect() converts timestamps using the JVM default, so on a machine set
        # to Asia/Colombo a correct UTC value comes back shifted by +5:30 and every
        # timestamp assertion silently compares the wrong things.
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    _ = pyspark
