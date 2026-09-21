"""Zone geometry and lookup."""

from __future__ import annotations

import pytest

from fleet.common.zones import (
    BY_ID,
    LAT_MAX,
    LAT_MIN,
    LON_MAX,
    LON_MIN,
    ZONES,
    ZoneClass,
    in_city_bounds,
    zone_for,
)


class TestGrid:
    def test_twelve_zones(self):
        assert len(ZONES) == 12

    def test_ids_are_unique_and_sequential(self):
        assert [z.zone_id for z in ZONES] == [f"Z{i:02d}" for i in range(1, 13)]

    def test_zones_tile_the_city_without_overlap(self):
        """Every zone occupies a distinct grid cell. An overlap would make zone
        assignment order-dependent, and a gap would silently drop events."""
        assert len({(z.lat_min, z.lon_min) for z in ZONES}) == 12

    def test_every_point_in_bounds_maps_to_exactly_one_zone(self):
        for i in range(1, 30):
            for j in range(1, 30):
                lat = LAT_MIN + (LAT_MAX - LAT_MIN) * i / 30
                lon = LON_MIN + (LON_MAX - LON_MIN) * j / 30
                assert sum(z.contains(lat, lon) for z in ZONES) == 1


class TestLookup:
    def test_city_centre_resolves(self):
        assert zone_for(6.9271, 79.8612) is not None

    @pytest.mark.parametrize("lat,lon", [(0.0, 0.0), (51.5, -0.12), (6.5, 79.86), (6.92, 90.0)])
    def test_out_of_bounds_returns_none(self, lat, lon):
        """None rather than a default zone: an out-of-bounds fix is a data-quality
        signal and must be counted, not silently bucketed into a real zone."""
        assert zone_for(lat, lon) is None
        assert not in_city_bounds(lat, lon)

    def test_null_coordinates_return_none(self):
        assert zone_for(None, None) is None
        assert zone_for(6.92, None) is None

    def test_zone_centres_resolve_to_themselves(self):
        for z in ZONES:
            assert zone_for(*z.centre) is z


class TestEconomics:
    def test_demand_weights_span_a_meaningful_range(self):
        """If every zone had the same demand the 'earnings by area' answer would be
        flat noise and no vehicle could *become* unprofitable for a real reason."""
        weights = [z.demand_weight for z in ZONES]
        assert max(weights) / min(weights) >= 5.0

    def test_cbd_outearns_outskirts(self):
        cbd = [z for z in ZONES if z.zone_class is ZoneClass.CBD]
        out = [z for z in ZONES if z.zone_class is ZoneClass.OUTSKIRTS]
        assert min(z.demand_weight for z in cbd) > max(z.demand_weight for z in out)

    def test_every_zone_class_is_represented(self):
        assert {z.zone_class for z in ZONES} == set(ZoneClass)

    def test_v113_home_zone_is_an_outskirt(self):
        """The unprofitable-vehicle narrative depends on Z11 being low-demand."""
        assert BY_ID["Z11"].zone_class is ZoneClass.OUTSKIRTS


class TestBroadcastShape:
    def test_rows_carry_the_bounds_spark_joins_on(self):
        from fleet.common.zones import as_broadcast_rows

        rows = as_broadcast_rows()
        assert len(rows) == 12
        assert {"zone_id", "lat_min", "lat_max", "lon_min", "lon_max"} <= set(rows[0])
