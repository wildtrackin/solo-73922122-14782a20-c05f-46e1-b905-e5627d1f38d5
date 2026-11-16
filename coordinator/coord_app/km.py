"""Pooled Kaplan-Meier computation over per-site aggregate rows (pure, no I/O)."""
from __future__ import annotations


def km_curve(points: list[int], per_site_rows: list[list[dict]]) -> list[dict]:
    """Combined Kaplan-Meier curve on the union of all sites' death days.

    `points` are the sorted union of death days. `per_site_rows` holds, per
    site, rows {"t", "n_risk", "d"} evaluated at those points. Risk sets count
    same-day censored cases as still at risk (deaths before censoring).
    """
    maps = [{row["t"]: row for row in rows} for rows in per_site_rows]
    survival = 1.0
    curve = []
    for t in points:
        n = sum(m[t]["n_risk"] for m in maps if t in m)
        d = sum(m[t]["d"] for m in maps if t in m)
        if n > 0 and d > 0:
            survival *= 1.0 - d / n
        curve.append({"t": t, "n_risk": n, "d": d, "survival": survival})
    return curve
