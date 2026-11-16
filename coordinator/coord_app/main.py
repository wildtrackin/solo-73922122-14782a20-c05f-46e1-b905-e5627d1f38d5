"""Federated Kaplan-Meier coordinator.

Runs computation rounds across two hospital site nodes. Case-level data never
leaves the sites: the coordinator only sees snapshot ids, death days, and
aggregate at-risk/death counts.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from sqlalchemy import select, text, update

from .config import CoordConfig
from .db import init_db, make_engine, make_sessionmaker
from .models import FINAL_STATES, Round
from .worker import RoundManager, new_round_id, utcnow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("coordinator")


def round_out(r: Round) -> dict:
    out = {
        "round_id": r.id,
        "status": r.status,
        "cancel_requested": r.cancel_requested,
        "error": r.error,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }
    # Results are published only once both sites contributed and the round
    # completed successfully.
    if r.status == "succeeded":
        out["result"] = r.result
    return out


def create_app(cfg: CoordConfig) -> FastAPI:
    engine = make_engine(cfg.database_url)
    Session = make_sessionmaker(engine)
    manager = RoundManager(cfg, Session)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_db(engine)
        await manager.recover_interrupted_rounds()
        yield
        await manager.shutdown()
        await engine.dispose()

    app = FastAPI(
        title="Federated Kaplan-Meier coordinator",
        version="1.0.0",
        description=(
            "Computes a pooled Kaplan-Meier survival curve across two hospital "
            "site nodes without any case-level data leaving the hospitals."
        ),
        lifespan=lifespan,
    )

    @app.post("/rounds", status_code=202)
    async def create_round():
        """Start a computation round (asynchronous)."""
        round_id = new_round_id()
        now = utcnow()
        async with Session() as s:
            s.add(Round(id=round_id, status="running", created_at=now, updated_at=now))
            await s.commit()
        manager.start_round(round_id)
        return {
            "round_id": round_id,
            "status": "running",
            "links": {"self": f"/rounds/{round_id}", "cancel": f"/rounds/{round_id}/cancel"},
        }

    @app.get("/rounds")
    async def list_rounds(limit: int = 50):
        async with Session() as s:
            rows = (await s.scalars(select(Round).order_by(Round.created_at.desc()).limit(min(limit, 500)))).all()
            return {"rounds": [round_out(r) for r in rows]}

    @app.get("/rounds/{round_id}")
    async def get_round(round_id: str):
        """Poll round status. Always fast: reads only the coordinator DB,
        never waits on a (possibly slow) site."""
        async with Session() as s:
            r = await s.get(Round, round_id)
            if r is None:
                raise HTTPException(status_code=404, detail="round not found")
            return round_out(r)

    @app.post("/rounds/{round_id}/cancel")
    async def cancel_round(round_id: str):
        """Request cancellation. Returns immediately; the round transitions to
        `cancelling` and reaches `cancelled` once both sites are released."""
        async with Session() as s:
            res = await s.execute(
                update(Round)
                .where(Round.id == round_id, Round.status.notin_(FINAL_STATES))
                .values(cancel_requested=True, status="cancelling", updated_at=utcnow())
            )
            if res.rowcount == 0:
                row = await s.get(Round, round_id)
                if row is None:
                    raise HTTPException(status_code=404, detail="round not found")
                raise HTTPException(status_code=409, detail=f"round already terminal: {row.status}")
            await s.commit()
        task = manager.tasks.get(round_id)
        if task is not None and not task.done():
            task.cancel()
        return {"round_id": round_id, "status": "cancelling"}

    @app.get("/health")
    async def health():
        async with Session() as s:
            await s.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app


# Module-level app used by `uvicorn coord_app.main:app` (Docker, CLI).
app = create_app(CoordConfig.from_env())
