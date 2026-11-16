"""Unit tests for the pooled Kaplan-Meier math and the site-side aggregation."""
import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "coordinator"))
sys.path.insert(0, str(ROOT / "site"))

from coord_app.km import km_curve  # noqa: E402
from site_app.aggregate import risk_rows  # noqa: E402

# Site A: (5,death) (8,censored) (8,death) (12,death)
HIST_A = [(5, 1, 0), (8, 1, 1), (12, 1, 0)]
# Site B: (5,censored) (9,death) (12,death) (20,censored)
HIST_B = [(5, 0, 1), (9, 1, 0), (12, 1, 0), (20, 0, 1)]

POINTS = [5, 8, 9, 12]  # union of both sites' death days


def test_site_risk_rows_death_before_censor():
    rows_a = {r["t"]: r for r in risk_rows(HIST_A, POINTS)}
    rows_b = {r["t"]: r for r in risk_rows(HIST_B, POINTS)}
    # Site A
    assert rows_a[5]["n_risk"] == 4 and rows_a[5]["d"] == 1
    # at t=8 the case censored on day 8 is still at risk (death first)
    assert rows_a[8]["n_risk"] == 3 and rows_a[8]["d"] == 1
    assert rows_a[9]["n_risk"] == 1 and rows_a[9]["d"] == 0
    assert rows_a[12]["n_risk"] == 1 and rows_a[12]["d"] == 1
    # Site B: case censored on day 5 still counts at t=5
    assert rows_b[5]["n_risk"] == 4 and rows_b[5]["d"] == 0
    assert rows_b[8]["n_risk"] == 3 and rows_b[8]["d"] == 0
    assert rows_b[9]["n_risk"] == 3 and rows_b[9]["d"] == 1
    assert rows_b[12]["n_risk"] == 2 and rows_b[12]["d"] == 1


def test_site_risk_rows_edge_points():
    rows = {r["t"]: r for r in risk_rows(HIST_B, [1, 10, 20, 21])}
    assert rows[1]["n_risk"] == 4          # everyone at risk before day 5
    assert rows[10]["n_risk"] == 2         # days 12 and 20
    assert rows[20]["n_risk"] == 1         # the case censored on day 20
    assert rows[20]["d"] == 0
    assert rows[21]["n_risk"] == 0         # beyond all follow-up


def test_km_curve_combined():
    rows_a = risk_rows(HIST_A, POINTS)
    rows_b = risk_rows(HIST_B, POINTS)
    curve = km_curve(POINTS, [rows_a, rows_b])
    assert [r["t"] for r in curve] == POINTS
    assert [(r["n_risk"], r["d"]) for r in curve] == [(8, 1), (6, 1), (4, 1), (3, 2)]
    expected = [7 / 8, 7 / 8 * 5 / 6, 7 / 8 * 5 / 6 * 3 / 4, 7 / 8 * 5 / 6 * 3 / 4 * 1 / 3]
    for row, want in zip(curve, expected):
        assert math.isclose(row["survival"], want, rel_tol=1e-12)
    assert math.isclose(curve[-1]["survival"], 0.1822916666666666, rel_tol=1e-12)


def test_km_curve_empty():
    assert km_curve([], [[], []]) == []


def test_km_curve_missing_point_treated_as_zero():
    # defensive: a site that did not answer a point contributes 0/0
    curve = km_curve([5], [[{"t": 5, "n_risk": 4, "d": 1}], []])
    assert curve[0]["n_risk"] == 4 and curve[0]["d"] == 1
    assert math.isclose(curve[0]["survival"], 0.75)
