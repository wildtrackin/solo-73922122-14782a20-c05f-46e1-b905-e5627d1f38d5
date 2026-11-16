"""Pure aggregation over a day-level histogram (unit-testable, no I/O)."""
from __future__ import annotations

from bisect import bisect_left
from typing import Iterable


def risk_rows(hist: Iterable[tuple[int, int, int]], points: list[int]) -> list[dict]:
    """Compute (n_risk, d) at each requested time point.

    `hist` yields (day, deaths, censored) tuples. n_risk(t) counts every case
    whose follow-up days are >= t, so a case censored on day t is still at risk
    for the deaths occurring on day t: same-day deaths are processed before
    same-day censoring (standard Kaplan-Meier convention).
    """
    weight: dict[int, int] = {}
    deaths_at: dict[int, int] = {}
    for day, deaths, censored in hist:
        weight[day] = weight.get(day, 0) + deaths + censored
        deaths_at[day] = deaths_at.get(day, 0) + deaths
    days = sorted(weight)
    suffix = [0] * (len(days) + 1)
    for i in range(len(days) - 1, -1, -1):
        suffix[i] = suffix[i + 1] + weight[days[i]]
    rows = []
    for t in points:
        i = bisect_left(days, t)
        rows.append({"t": t, "n_risk": suffix[i], "d": deaths_at.get(t, 0)})
    return rows
