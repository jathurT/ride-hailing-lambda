"""The merge boundary - the most important test in the repository (plan/07 section 7).

`split_interval` is pure, so every case here is a table of dates. No Redis, no
Postgres, no Spark, no clock. That is the reason the function takes `sim_today` as an
argument instead of reading the simulated clock: a rule this load-bearing should be
checkable without a stack running.

The invariant at the bottom is the one that actually matters. Every individual case
below could pass while the rule was still wrong for some range nobody thought to
write down; the invariant asserts the property itself - each requested date is owned
by exactly one side, or is explicitly reported as owned by neither.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from fleet.serving.merge import split_interval

D = date


def _dates(a: date, b: date) -> set[date]:
    return {a + timedelta(days=i) for i in range((b - a).days + 1)}


def _batch_dates(iv) -> set[date]:
    if iv.batch_from is None or iv.batch_to is None:
        return set()
    return _dates(iv.batch_from, iv.batch_to)


# (name, hwm, from, to, sim_today, expect_batch, expect_speed, expect_uncovered)
CASES = [
    (
        "cold start - batch has never run",
        None,
        D(2026, 3, 5),
        D(2026, 3, 7),
        D(2026, 3, 7),
        set(),
        D(2026, 3, 7),
        {D(2026, 3, 5), D(2026, 3, 6)},
    ),
    (
        "wholly inside the batch view - redis is never asked",
        D(2026, 3, 9),
        D(2026, 3, 2),
        D(2026, 3, 4),
        D(2026, 3, 10),
        _dates(D(2026, 3, 2), D(2026, 3, 4)),
        None,
        set(),
    ),
    (
        "boundary date belongs to batch, not speed",
        D(2026, 3, 6),
        D(2026, 3, 4),
        D(2026, 3, 6),
        D(2026, 3, 7),
        _dates(D(2026, 3, 4), D(2026, 3, 6)),
        None,
        set(),
    ),
    (
        "wholly after the watermark - postgres owns nothing",
        D(2026, 3, 4),
        D(2026, 3, 5),
        D(2026, 3, 5),
        D(2026, 3, 5),
        set(),
        D(2026, 3, 5),
        set(),
    ),
    (
        "spans the boundary, batch one day behind - no gap",
        D(2026, 3, 6),
        D(2026, 3, 4),
        D(2026, 3, 7),
        D(2026, 3, 7),
        _dates(D(2026, 3, 4), D(2026, 3, 6)),
        D(2026, 3, 7),
        set(),
    ),
    (
        "spans the boundary, batch three days behind - the gap is reported",
        D(2026, 3, 4),
        D(2026, 3, 2),
        D(2026, 3, 8),
        D(2026, 3, 8),
        _dates(D(2026, 3, 2), D(2026, 3, 4)),
        D(2026, 3, 8),
        {D(2026, 3, 5), D(2026, 3, 6), D(2026, 3, 7)},
    ),
    (
        "single day, today, watermark on yesterday",
        D(2026, 3, 6),
        D(2026, 3, 7),
        D(2026, 3, 7),
        D(2026, 3, 7),
        set(),
        D(2026, 3, 7),
        set(),
    ),
    (
        "range ends before today - speed has nothing to say",
        D(2026, 3, 3),
        D(2026, 3, 2),
        D(2026, 3, 5),
        D(2026, 3, 9),
        _dates(D(2026, 3, 2), D(2026, 3, 3)),
        None,
        {D(2026, 3, 4), D(2026, 3, 5)},
    ),
]


@pytest.mark.parametrize(
    ("name", "hwm", "frm", "to", "today", "exp_batch", "exp_speed", "exp_uncovered"),
    CASES,
    ids=[c[0] for c in CASES],
)
def test_split_interval(name, hwm, frm, to, today, exp_batch, exp_speed, exp_uncovered):
    iv = split_interval(hwm, frm, to, today)
    assert _batch_dates(iv) == exp_batch, f"{name}: batch range wrong"
    assert iv.speed_date == exp_speed, f"{name}: speed date wrong"
    assert set(iv.uncovered) == exp_uncovered, f"{name}: uncovered set wrong"


@pytest.mark.parametrize(
    ("name", "hwm", "frm", "to", "today", "exp_batch", "exp_speed", "exp_uncovered"),
    CASES,
    ids=[c[0] for c in CASES],
)
def test_every_requested_date_has_exactly_one_owner(
    name, hwm, frm, to, today, exp_batch, exp_speed, exp_uncovered
):
    """The property the whole merge exists to guarantee.

    No date may be served by both views - that is a double count, and in a revenue
    figure it is the bug this project has already been bitten by once. No date may
    vanish either: if neither view owns it, it must appear in `uncovered` so the
    response can say so out loud.
    """
    iv = split_interval(hwm, frm, to, today)
    batch = _batch_dates(iv)
    speed = {iv.speed_date} if iv.speed_date else set()

    assert batch & speed == set(), f"{name}: a date is served by BOTH views"
    assert batch & set(iv.uncovered) == set(), f"{name}: a batch date is also 'uncovered'"
    assert speed & set(iv.uncovered) == set(), f"{name}: the speed date is also 'uncovered'"
    assert batch | speed | set(iv.uncovered) == _dates(frm, to), f"{name}: a date vanished"


def test_batch_is_not_queried_when_it_owns_nothing():
    """`needs_batch` is false, so the endpoint answers with Postgres unreachable."""
    iv = split_interval(D(2026, 3, 4), D(2026, 3, 5), D(2026, 3, 5), D(2026, 3, 5))
    assert not iv.needs_batch
    assert iv.needs_speed


def test_speed_is_not_queried_when_it_owns_nothing():
    """`needs_speed` is false, so the endpoint answers with Redis unreachable."""
    iv = split_interval(D(2026, 3, 9), D(2026, 3, 2), D(2026, 3, 4), D(2026, 3, 10))
    assert iv.needs_batch
    assert not iv.needs_speed


def test_cold_start_says_so_and_is_not_an_error():
    iv = split_interval(None, D(2026, 3, 1), D(2026, 3, 1), D(2026, 3, 1))
    assert "no batch data available" in iv.consistency
    assert iv.speed_date == D(2026, 3, 1)


def test_gap_is_named_in_the_consistency_string():
    iv = split_interval(D(2026, 3, 4), D(2026, 3, 2), D(2026, 3, 8), D(2026, 3, 8))
    assert "neither view" in iv.consistency
    assert "behind the simulated clock" in iv.consistency


def test_inverted_range_is_rejected_loudly():
    with pytest.raises(ValueError, match="precedes"):
        split_interval(D(2026, 3, 4), D(2026, 3, 8), D(2026, 3, 2), D(2026, 3, 8))


def test_a_future_date_is_not_reported_as_the_batch_layer_lagging():
    """Two causes of an uncovered date, only one of which is a fault.

    A date after the simulated clock has simply not happened yet. Describing it as
    "the batch layer is behind" sends someone looking for a problem that does not
    exist - and during a live demo that is the worst possible false alarm.
    """
    iv = split_interval(D(2026, 3, 4), D(2026, 3, 2), D(2026, 3, 6), D(2026, 3, 5))
    assert set(iv.uncovered) == {D(2026, 3, 6)}
    assert "in the future" in iv.consistency
    assert "behind the simulated clock" not in iv.consistency
    assert "only reached 2026-03-05" in iv.consistency


def test_a_real_gap_and_a_future_date_are_reported_separately():
    iv = split_interval(D(2026, 3, 2), D(2026, 3, 1), D(2026, 3, 8), D(2026, 3, 5))
    assert "behind the simulated clock" in iv.consistency, "3-03..3-05 is a genuine gap"
    assert "in the future" in iv.consistency, "3-06..3-08 has not happened yet"
