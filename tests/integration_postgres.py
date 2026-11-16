#!/usr/bin/env python3
"""PostgreSQL-backed end-to-end test using an embedded PostgreSQL (pgserver).

Validates the production database path (asyncpg + advisory-lock serialized
snapshot creation) that the SQLite-based integration_local.py cannot cover:

  P1. services healthy on PostgreSQL, demo cases seeded
  P2. 10 concurrent snapshot creates -> exactly 2 win the slots (advisory lock)
  P3. full round succeeds; curve matches an independent SQL computation
  P4. cancel mid-round -> cancelled, both sites released
  P5. idempotent create per round

Run:  python tests/integration_postgres.py
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "site"))
sys.path.insert(0, str(ROOT / "coordinator"))
sys.path.insert(0, str(ROOT / "tests"))

import asyncpg  # noqa: E402
import pgserver  # noqa: E402

from integration_local import (  # noqa: E402
    active_snapshots, check, failures, free_port, http, run_server, wait_final, wait_health,
)
from coord_app.config import CoordConfig  # noqa: E402
from coord_app.main import create_app as create_coord_app  # noqa: E402
from site_app.config import SiteConfig  # noqa: E402
from site_app.main import create_app as create_site_app  # noqa: E402

PG_DIR = tempfile.mkdtemp(prefix="km-pg-")


def pg_url(db: str) -> str:
    # asyncpg treats a host starting with '/' as a unix-socket directory
    return f"postgresql+asyncpg://postgres@/{db}?host={PG_DIR}"


async def pg_cases(db: str) -> list[tuple[int, str]]:
    con = await asyncpg.connect(user="postgres", database=db, host=PG_DIR)
    try:
        rows = await con.fetch("SELECT days, event FROM cases")
        return [(r["days"], r["event"]) for r in rows]
    finally:
        await con.close()


def expected_curve_pg() -> list[tuple[int, int, int, float]]:
    cases = asyncio.run(pg_cases("site_a")) + asyncio.run(pg_cases("site_b"))
    death_days = sorted({d for d, ev in cases if ev == "death"})
    s = 1.0
    out = []
    for t in death_days:
        n = sum(1 for d, _ in cases if d >= t)
        dd = sum(1 for d, ev in cases if d == t and ev == "death")
        s *= 1 - dd / n
        out.append((t, n, dd, s))
    return out


def main() -> int:
    srv = pgserver.get_server(PG_DIR)
    print("embedded postgres:", srv.get_uri())

    async def setup_dbs():
        con = await asyncpg.connect(user="postgres", database="postgres", host=PG_DIR)
        try:
            for db in ("coordinator", "site_a", "site_b"):
                await con.execute(f'CREATE DATABASE "{db}"')
        finally:
            await con.close()

    asyncio.run(setup_dbs())

    port_a, port_b, port_c = free_port(), free_port(), free_port()
    A, B, COORD = (f"http://127.0.0.1:{p}" for p in (port_a, port_b, port_c))

    run_server(create_site_app(SiteConfig(
        site_id="A", database_url=pg_url("site_a"), seed="A", seed_cases=160,
        snapshot_ttl_seconds=120)), port_a, "site-a-pg")
    run_server(create_site_app(SiteConfig(
        site_id="B", database_url=pg_url("site_b"), seed="B", seed_cases=140,
        response_delay_seconds=2.0, snapshot_ttl_seconds=120)), port_b, "site-b-pg")
    run_server(create_coord_app(CoordConfig(
        database_url=pg_url("coordinator"), site_a_url=A, site_b_url=B)), port_c, "coord-pg")
    wait_health(A, "site-a-pg")
    wait_health(B, "site-b-pg")
    wait_health(COORD, "coord-pg")

    st, ha = http("GET", f"{A}/health")
    st2, hb = http("GET", f"{B}/health")
    check("P1 services healthy on PostgreSQL with seeded cases",
          st == 200 and st2 == 200 and ha["cases"] == 160 and hb["cases"] == 140, f"{ha} {hb}")

    # ---- P2: concurrent creates race for the two slots ---------------------
    import concurrent.futures as cf

    def try_create(i):
        return http("POST", f"{A}/snapshots", {"round_id": f"race-{i}"}, timeout=30)[0]

    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        codes = list(ex.map(try_create, range(10)))
    n_ok = sum(1 for c in codes if c == 201)
    n_full = sum(1 for c in codes if c == 409)
    check("P2 exactly 2 of 10 concurrent creates win slots", n_ok == 2 and n_full == 8,
          f"codes={codes}")
    check("P2b site reports exactly 2 active", active_snapshots(A) == 2)
    # free the slots again
    st, snaps = http("GET", f"{A}/snapshots")
    for snap in snaps["active"]:
        http("DELETE", f"{A}/snapshots/{snap['snapshot_id']}")
    st, snaps = http("GET", f"{A}/snapshots")
    check("P2c slots released again", snaps["active_count"] == 0)

    # ---- P3: full round on PostgreSQL --------------------------------------
    st, r = http("POST", f"{COORD}/rounds")
    rid = r["round_id"]
    final = wait_final(COORD, rid, timeout=60)
    check("P3 round succeeded on PostgreSQL", final["status"] == "succeeded", str(final.get("error")))
    curve = final.get("result", {}).get("curve", [])
    want = expected_curve_pg()
    ok = len(curve) == len(want) and all(
        row["t"] == t and row["n_risk"] == n and row["d"] == dd and abs(row["survival"] - s) < 1e-9
        for row, (t, n, dd, s) in zip(curve, want))
    check("P3b curve matches independent SQL computation", ok,
          f"got {len(curve)} points, want {len(want)}")
    check("P3c both sites released", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- P4: cancel on PostgreSQL ------------------------------------------
    st, r = http("POST", f"{COORD}/rounds")
    rid4 = r["round_id"]
    time.sleep(0.8)
    st, r = http("POST", f"{COORD}/rounds/{rid4}/cancel")
    final = wait_final(COORD, rid4, timeout=30)
    check("P4 cancel -> cancelled, sites released",
          final["status"] == "cancelled" and active_snapshots(A) == 0 and active_snapshots(B) == 0,
          str(final))

    # ---- P5: idempotent create ---------------------------------------------
    st, i1 = http("POST", f"{B}/snapshots", {"round_id": "pg-idem"})
    st2, i2 = http("POST", f"{B}/snapshots", {"round_id": "pg-idem"})
    check("P5 idempotent create on PostgreSQL",
          i1["snapshot_id"] == i2["snapshot_id"] and active_snapshots(B) == 1)
    http("DELETE", f"{B}/snapshots/{i1['snapshot_id']}")

    # ---- P6: 200-case cap enforced even for direct SQL ----------------------
    async def cap_check():
        con = await asyncpg.connect(user="postgres", database="site_a", host=PG_DIR)
        try:
            n = await con.fetchval("SELECT count(*) FROM cases")
            for i in range(200 - n):
                await con.execute("INSERT INTO cases VALUES ($1, 30, 'death')", f"cap-{i:03d}")
            raised = False
            try:
                await con.execute("INSERT INTO cases VALUES ('cap-overflow', 30, 'death')")
            except asyncpg.RaiseError:
                raised = True
            await con.execute("DELETE FROM cases WHERE case_no LIKE 'cap-%'")
            return raised
        finally:
            await con.close()

    check("P6 200-case cap enforced for direct SQL inserts", asyncio.run(cap_check()))

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        return 1
    print("ALL POSTGRES CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
