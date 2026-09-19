"""The 12 operating zones of a fictional city.

Zones are the unit the business question asks about ("utilization and earnings by
**area**"), so they are defined once here and shared by the simulator, the streaming
job and the batch job.

Two design notes worth carrying into the report:

1. **Zones have different characters.** If every zone had the same demand, the
   "earnings by area" panel would be flat noise and the daily report would have
   nothing to say. The weights below produce a genuinely uneven fleet: CBD zones
   earn, outskirt zones do not, and that is what makes a vehicle like V113
   *become* unprofitable rather than being randomly unlucky.

2. **Lookup is by bounding box, not by a Python UDF.** In Spark this becomes a
   broadcast join with a range condition, which stays inside the JVM and is
   optimised by Catalyst. A Python UDF would force per-row serialisation across the
   JVM/Python boundary. See plan/05 section 3.2.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

# A 4x3 grid over a fictional coastal city. Coordinates are plausible rather than
# real; nothing depends on them being a specific place.
LAT_MIN, LAT_MAX = 6.85, 6.99
LON_MIN, LON_MAX = 79.82, 79.94
GRID_COLS, GRID_ROWS = 4, 3

LAT_STEP = (LAT_MAX - LAT_MIN) / GRID_ROWS
LON_STEP = (LON_MAX - LON_MIN) / GRID_COLS


class ZoneClass(StrEnum):
    CBD = "cbd"
    AIRPORT = "airport"
    RESIDENTIAL = "residential"
    INDUSTRIAL = "industrial"
    OUTSKIRTS = "outskirts"


@dataclass(frozen=True, slots=True)
class Zone:
    zone_id: str
    name: str
    zone_class: ZoneClass
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    demand_weight: float
    """Multiplier on trip-start rate. CBD is 6x an outskirt zone."""
    congestion: float
    """0..1. Reduces average speed, which raises time-based fare and lowers
    distance covered - so congested zones are not automatically more profitable."""

    @property
    def centre(self) -> tuple[float, float]:
        return ((self.lat_min + self.lat_max) / 2, (self.lon_min + self.lon_max) / 2)

    def contains(self, lat: float, lon: float) -> bool:
        # Half-open on the upper edge so adjacent zones never both claim a point.
        return self.lat_min <= lat < self.lat_max and self.lon_min <= lon < self.lon_max


def _box(row: int, col: int) -> tuple[float, float, float, float]:
    return (
        LAT_MIN + row * LAT_STEP,
        LAT_MIN + (row + 1) * LAT_STEP,
        LON_MIN + col * LON_STEP,
        LON_MIN + (col + 1) * LON_STEP,
    )


# (row, col, name, class, demand_weight, congestion)
_SPEC: list[tuple[int, int, str, ZoneClass, float, float]] = [
    (2, 1, "Fort", ZoneClass.CBD, 1.8, 0.75),
    (2, 2, "Union Place", ZoneClass.CBD, 1.8, 0.70),
    (2, 3, "Airport Road", ZoneClass.AIRPORT, 1.4, 0.30),
    (1, 0, "Kelaniya", ZoneClass.RESIDENTIAL, 1.0, 0.45),
    (1, 1, "Maradana", ZoneClass.RESIDENTIAL, 1.0, 0.55),
    (1, 2, "Borella", ZoneClass.RESIDENTIAL, 1.0, 0.50),
    (1, 3, "Rajagiriya", ZoneClass.RESIDENTIAL, 1.0, 0.40),
    (2, 0, "Wattala", ZoneClass.RESIDENTIAL, 1.0, 0.35),
    (0, 1, "Ratmalana", ZoneClass.INDUSTRIAL, 0.6, 0.25),
    (0, 2, "Kotte Works", ZoneClass.INDUSTRIAL, 0.6, 0.20),
    (0, 0, "Moratuwa South", ZoneClass.OUTSKIRTS, 0.3, 0.10),
    (0, 3, "Kaduwela", ZoneClass.OUTSKIRTS, 0.3, 0.10),
]

ZONES: tuple[Zone, ...] = tuple(
    Zone(
        zone_id=f"Z{i + 1:02d}",
        name=name,
        zone_class=cls,
        lat_min=(b := _box(row, col))[0],
        lat_max=b[1],
        lon_min=b[2],
        lon_max=b[3],
        demand_weight=weight,
        congestion=congestion,
    )
    for i, (row, col, name, cls, weight, congestion) in enumerate(_SPEC)
)

BY_ID: dict[str, Zone] = {z.zone_id: z for z in ZONES}


def zone_for(lat: float | None, lon: float | None) -> Zone | None:
    """Which zone contains this point? None if outside the city bounding box.

    Returning None rather than a default is deliberate: an out-of-bounds GPS fix is
    a data-quality signal, and it is counted rather than silently bucketed into a
    real zone. See plan/05 section 3.1.
    """
    if lat is None or lon is None:
        return None
    return next((z for z in ZONES if z.contains(lat, lon)), None)


def in_city_bounds(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return LAT_MIN <= lat < LAT_MAX and LON_MIN <= lon < LON_MAX


def as_broadcast_rows() -> list[dict[str, object]]:
    """Zone table in the shape Spark broadcasts for the enrichment range-join."""
    return [
        {
            "zone_id": z.zone_id,
            "zone_name": z.name,
            "zone_class": str(z.zone_class),
            "lat_min": z.lat_min,
            "lat_max": z.lat_max,
            "lon_min": z.lon_min,
            "lon_max": z.lon_max,
            "demand_weight": z.demand_weight,
        }
        for z in ZONES
    ]
