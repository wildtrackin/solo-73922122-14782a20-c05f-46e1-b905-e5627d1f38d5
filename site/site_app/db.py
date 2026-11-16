from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from .models import Base

# Enforce the "at most 200 cases per site" rule at the database level, so it
# also holds for direct SQL corrections (PostgreSQL deployments).
_PG_CAP_FUNCTION = """
CREATE OR REPLACE FUNCTION cases_cap_200() RETURNS trigger AS $$
BEGIN
    IF (SELECT count(*) FROM cases) >= 200 THEN
        RAISE EXCEPTION 'cases table is capped at 200 rows';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"""
_PG_CAP_TRIGGER = """
CREATE TRIGGER cases_cap_200 BEFORE INSERT ON cases
FOR EACH ROW EXECUTE FUNCTION cases_cap_200()
"""


def make_engine(database_url: str) -> AsyncEngine:
    connect_args = {"timeout": 30} if database_url.startswith("sqlite") else {}
    return create_async_engine(database_url, pool_pre_ping=True, connect_args=connect_args)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if engine.sync_engine.dialect.name == "postgresql":
            await conn.execute(text(_PG_CAP_FUNCTION))
            await conn.execute(text("DROP TRIGGER IF EXISTS cases_cap_200 ON cases"))
            await conn.execute(text(_PG_CAP_TRIGGER))
