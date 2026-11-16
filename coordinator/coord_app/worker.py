"""Round orchestration: snapshot both sites, combine aggregates, always release.

Guarantees implemented here:
  * both sites must contribute before any result is published;
  * cancellation or any site failure stops the whole round;
  * both sites' snapshots are released (and the round finalized on the sites)
    BEFORE the round's terminal state becomes visible;
  * a round is never resumed with stale snapshots after a coordinator restart.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from .config import CoordConfig
from .km import km_curve
from .models import FINAL_STATES, Round, RoundSnapshot
from .sites import SITE_IDS, SiteClient

logger = logging.getLogger("coordinator.worker")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_round_id() -> str:
    return str(uuid.uuid4())


class RoundManager:
    def __init__(self, cfg: CoordConfig, session_factory: async_sessionmaker):
        self.cfg = cfg
        self.Session = session_factory
        self.sites = SiteClient(cfg)
        # round_id -> asyncio.Task running the round
        self.tasks: dict[str, asyncio.Task] = {}
        # round_id -> {site: snapshot_id}; in-memory back-up of the DB
        # bookkeeping so cleanup still knows what to release even if a DB
        # insert failed mid-round.
        self.created: dict[str, dict[str, str]] = {}

    def start_round(self, round_id: str) -> None:
        task = asyncio.create_task(self.run_round(round_id), name=f"round-{round_id}")
        self.tasks[round_id] = task
        task.add_done_callback(lambda _t: self.tasks.pop(round_id, None))

    # ------------------------------------------------------------------ #
    # orchestration
    # ------------------------------------------------------------------ #
    async def run_round(self, round_id: str) -> None:
        status, error, result = "succeeded", None, None
        try:
            result = await self.compute_round(round_id)
        except asyncio.CancelledError:
            status = "cancelled"
            logger.info("round %s cancelled", round_id)
        except Exception as exc:  # any site failure stops the whole round
            status, error = "failed", f"{type(exc).__name__}: {exc}"
            logger.warning("round %s failed: %s", round_id, error)

        try:
            await self.cleanup_round(round_id)
        except BaseException:
            logger.exception("round %s cleanup failed", round_id)
        self.created.pop(round_id, None)

        # Terminal state is published only after the release attempts above.
        await self.finalize_round_row(round_id, status, error, result)

    async def compute_round(self, round_id: str) -> dict:
        created = self.created.setdefault(round_id, {})

        async def create(site: str) -> None:
            snapshot_id = await self.sites.create_snapshot(site, round_id)
            created[site] = snapshot_id
            try:
                await self.record_snapshot(round_id, site, snapshot_id)
            except Exception:
                logger.exception("round %s: could not persist snapshot record for site %s", round_id, site)

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(create("a"))
                tg.create_task(create("b"))

            days: dict[str, list[int]] = {}

            async def fetch_days(site: str) -> None:
                days[site] = await self.sites.death_days(site, created[site])

            async with asyncio.TaskGroup() as tg:
                tg.create_task(fetch_days("a"))
                tg.create_task(fetch_days("b"))

            points = sorted(set(days["a"]) | set(days["b"]))

            aggs: dict[str, list[dict]] = {}

            async def fetch_agg(site: str) -> None:
                aggs[site] = await self.sites.aggregate(site, created[site], points)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(fetch_agg("a"))
                tg.create_task(fetch_agg("b"))
        except BaseExceptionGroup as group:
            raise group.exceptions[0]

        curve = km_curve(points, [aggs["a"], aggs["b"]])
        return {
            "curve": curve,
            "n_points": len(curve),
            "sites": ["a", "b"],
            "snapshots": {"a": created["a"], "b": created["b"]},
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    async def cleanup_round(self, round_id: str) -> None:
        """Best-effort release of both sites' snapshots + finalize the round."""
        to_release: dict[str, str] = dict(self.created.get(round_id, {}))
        async with self.Session() as s:
            rows = (
                await s.scalars(
                    select(RoundSnapshot).where(
                        RoundSnapshot.round_id == round_id, RoundSnapshot.released.is_(False)
                    )
                )
            ).all()
            for row in rows:
                to_release.setdefault(row.site, row.snapshot_id)

        async def release(site: str, snapshot_id: str) -> None:
            try:
                await self.sites.delete_snapshot(site, snapshot_id)
            except BaseException as exc:
                logger.warning(
                    "round %s: release of snapshot %s on site %s failed: %s",
                    round_id, snapshot_id, site, exc,
                )

        await asyncio.gather(*(release(site, sid) for site, sid in to_release.items()))

        async with self.Session() as s:
            await s.execute(update(RoundSnapshot).where(RoundSnapshot.round_id == round_id).values(released=True))
            await s.commit()

        async def finalize(site: str) -> None:
            try:
                await self.sites.finalize_round(site, round_id)
            except BaseException as exc:
                logger.warning("round %s: finalize on site %s failed: %s", round_id, site, exc)

        # Finalize BOTH sites, even where no snapshot was recorded: a late
        # create for this round must never occupy a slot again.
        await asyncio.gather(*(finalize(site) for site in SITE_IDS))

    # ------------------------------------------------------------------ #
    # DB helpers
    # ------------------------------------------------------------------ #
    async def record_snapshot(self, round_id: str, site: str, snapshot_id: str) -> None:
        async with self.Session() as s:
            s.add(RoundSnapshot(round_id=round_id, site=site, snapshot_id=snapshot_id, created_at=utcnow()))
            await s.commit()

    async def finalize_round_row(self, round_id: str, status: str, error: str | None,
                                 result: dict | None) -> None:
        """Atomically publish the terminal state; a requested cancel always wins.

        Two single-statement conditional UPDATEs keep this race-free against
        the cancel endpoint on both PostgreSQL and SQLite.
        """
        now = utcnow()
        async with self.Session() as s:
            res = await s.execute(
                update(Round)
                .where(
                    Round.id == round_id,
                    Round.status.notin_(FINAL_STATES),
                    Round.cancel_requested.is_(False),
                )
                .values(status=status, error=error, result=result, updated_at=now)
            )
            if res.rowcount == 0:
                # Either already terminal (nothing to do) or a cancel was
                # requested: the cancel wins and the result is discarded.
                await s.execute(
                    update(Round)
                    .where(Round.id == round_id, Round.status.notin_(FINAL_STATES))
                    .values(status="cancelled", error=None, result=None, updated_at=now)
                )
            await s.commit()

    async def recover_interrupted_rounds(self) -> None:
        """On startup: never resume with stale snapshots; release and fail them."""
        async with self.Session() as s:
            ids = (await s.scalars(select(Round.id).where(Round.status.in_(("running", "cancelling"))))).all()
        for round_id in ids:
            logger.warning("recovering interrupted round %s", round_id)
            try:
                await self.cleanup_round(round_id)
            except Exception:
                logger.exception("recovery cleanup failed for round %s", round_id)
            await self.finalize_round_row(
                round_id, "failed", "coordinator restarted before the round finished; snapshots discarded", None
            )

    async def shutdown(self) -> None:
        """Cancel running rounds on shutdown; their cleanup releases the sites."""
        pending = list(self.tasks.values())
        for task in pending:
            task.cancel()
        if pending:
            _done, still = await asyncio.wait(pending, timeout=self.cfg.shutdown_timeout)
            for task in still:
                logger.warning("worker %s did not finish cleanup during shutdown", task.get_name())
