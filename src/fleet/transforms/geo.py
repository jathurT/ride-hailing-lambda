"""Geospatial maths as Spark Column expressions.

Deliberately NOT a Python UDF. A UDF forces per-row serialisation across the
JVM/Python boundary and is opaque to Catalyst - the module taught that Spark's
advantage comes from the Catalyst optimiser, and a UDF discards it. Built from
`pyspark.sql.functions` these stay inside the JVM and get whole-stage code
generation.

`fleet.common.geo` holds the pure-Python equivalents used by the simulator.
`test_haversine_matches_python` asserts the two agree, so the simulator's notion of
distance and the batch layer's cannot drift apart.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance between two points, in kilometres."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dlambda = F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlambda / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))
