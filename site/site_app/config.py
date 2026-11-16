"""Configuration for a hospital site node."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SiteConfig:
    site_id: str = "site"
    # SQLAlchemy async URL. Docker Compose uses PostgreSQL; tests may use sqlite.
    database_url: str = "sqlite+aiosqlite:///./site.db"
    # Hard cap of simultaneously active (not expired) snapshots on this site.
    max_snapshots: int = 2
    # A snapshot is released automatically after this many seconds, so a dead
    # or unreachable coordinator can never hold slots forever.
    snapshot_ttl_seconds: float = 300.0
    # Artificial per-request latency on the compute endpoints (slow-site demo).
    response_delay_seconds: float = 0.0
    # Demo data: deterministic seed, at most 200 cases per site.
    seed: str = "site"
    seed_cases: int = 150
    sweep_interval_seconds: float = 2.0
    # How long a "round finalized" marker is kept (late snapshot requests for
    # such rounds are rejected without occupying a slot).
    finalized_retention_seconds: float = 86400.0

    @classmethod
    def from_env(cls) -> "SiteConfig":
        site_id = os.environ.get("SITE_ID", "site")
        return cls(
            site_id=site_id,
            database_url=os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./site.db"),
            max_snapshots=int(os.environ.get("MAX_SNAPSHOTS", "2")),
            snapshot_ttl_seconds=float(os.environ.get("SNAPSHOT_TTL_SECONDS", "300")),
            response_delay_seconds=float(os.environ.get("RESPONSE_DELAY_SECONDS", "0")),
            seed=os.environ.get("SEED", site_id),
            seed_cases=min(int(os.environ.get("SEED_CASES", "150")), 200),
            sweep_interval_seconds=float(os.environ.get("SWEEP_INTERVAL_SECONDS", "2")),
            finalized_retention_seconds=float(os.environ.get("FINALIZED_RETENTION_SECONDS", "86400")),
        )
