"""Deterministic demo data (at most 200 cases, positive days, death/censored)."""
from __future__ import annotations

import random

from .models import Case


def seed_cases(seed: str, n: int) -> list[Case]:
    rng = random.Random(f"km-site-{seed}")
    cases: list[Case] = []
    for i in range(min(n, 200)):
        days = min(int(rng.expovariate(1.0 / 220.0)) + 1, 1500)
        event = "death" if rng.random() < 0.62 else "censored"
        cases.append(Case(case_no=f"{seed}-{i + 1:04d}", days=days, event=event))
    return cases
