"""Great-circle distance.

Pure-Python here for the simulator. The batch layer needs the *same* calculation
over a DataFrame; that version is built from `pyspark.sql.functions` as a Column
expression rather than a Python UDF, so it stays inside the JVM and Catalyst can
optimise it (plan/06 section 2.1). The two are kept numerically identical and a test
asserts they agree.
"""

from __future__ import annotations

import math

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def interpolate(
    lat1: float, lon1: float, lat2: float, lon2: float, fraction: float
) -> tuple[float, float]:
    """Linear interpolation along a straight line between two points.

    A real fleet follows a road network. We do not model roads: the distances and
    durations are still self-consistent, which is what the profitability arithmetic
    needs, and the simplification is declared in the report's limitations.
    """
    f = min(max(fraction, 0.0), 1.0)
    return (lat1 + (lat2 - lat1) * f, lon1 + (lon2 - lon1) * f)
