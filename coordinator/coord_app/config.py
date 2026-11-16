"""Coordinator configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CoordConfig:
    database_url: str = "sqlite+aiosqlite:///./coordinator.db"
    site_a_url: str = "http://localhost:8001"
    site_b_url: str = "http://localhost:8002"
    # A slow site may take this long per call before the round fails.
    site_request_timeout: float = 30.0
    # Short timeout used for release/finalize calls during cleanup/recovery.
    site_cleanup_timeout: float = 5.0
    # Grace period for worker tasks to finish cleanup on shutdown.
    shutdown_timeout: float = 15.0

    @classmethod
    def from_env(cls) -> "CoordConfig":
        return cls(
            database_url=os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./coordinator.db"),
            site_a_url=os.environ.get("SITE_A_URL", "http://localhost:8001"),
            site_b_url=os.environ.get("SITE_B_URL", "http://localhost:8002"),
            site_request_timeout=float(os.environ.get("SITE_REQUEST_TIMEOUT", "30")),
            site_cleanup_timeout=float(os.environ.get("SITE_CLEANUP_TIMEOUT", "5")),
            shutdown_timeout=float(os.environ.get("SHUTDOWN_TIMEOUT", "15")),
        )
