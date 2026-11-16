#!/usr/bin/env python3
"""End-to-end self-test: runs the coordinator and both site nodes as real
HTTP services (uvicorn servers in threads, SQLite backends) inside one
process, and exercises the federated KM workflow:

  A. happy path + curve verified against an independent computation
  B. slow site: status queries and cancel stay responsive; round stops
  C. snapshot slot limit (2) -> round fails; both sites released
  D. late snapshot create after terminal state is rejected
  E. idempotent snapshot create per round
  F. direct SQL correction during computation is not blocked
  G. coordinator crash -> TTL auto-release -> restart -> recovery -> new round
  H. graceful coordinator stop releases in-flight snapshots

A "crash" is simulated by freezing the coordinator's event loop (no cleanup
code runs), which is exactly what a SIGKILL would look like to the sites.

Run:  python tests/integration_local.py
"""
from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "site"))
sys.path.insert(0, str(ROOT / "coordinator"))

import uvicorn  # noqa: E402

from coord_app.config import CoordConfig  # noqa: E402
from coord_app.main import create_app as create_coord_app  # noqa: E402
from site_app.config import SiteConfig  # noqa: E402
from site_app.main import create_app as create_site_app  # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix="km-itest-"))
SNAPSHOT_TTL = 20.0   # short so the auto-release test is fast
B_DELAY = 4.0         # slow site B

failures: list[str] = []
servers: list[tuple[uvicorn.Server, asyncio.AbstractEventLoop, threading.Thread]] = []


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def check(name: str, cond: bool, detail: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_server(app, port: int, name: str):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    loop = asyncio.new_event_loop()

    def runner():
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(server.serve())
        except RuntimeError:
            pass  # frozen ("crashed") event loop

    t = threading.Thread(target=runner, name=name, daemon=True)
    t.start()
    servers.append((server, loop, t))
    return server, loop, t


def freeze(server_loop) -> None:
    """Simulate a coordinator crash: stop its event loop immediately, without
    running any cleanup (what a SIGKILL would look like to the sites)."""
    _server, loop, _t = server_loop
    loop.call_soon_threadsafe(loop.stop)


def http(method: str, url: str, body: dict | None = None, timeout: float = 35):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw)
        except Exception:
            payload = raw
        return e.code, payload


def wait_health(url: str, name: str, timeout: float = 60) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            st, _ = http("GET", url + "/health", timeout=3)
            if st == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"{name} did not become healthy")


def wait_final(coord: str, round_id: str, timeout: float = 90) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, body = http("GET", f"{coord}/rounds/{round_id}")
        assert st == 200, body
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        time.sleep(0.4)
    raise RuntimeError(f"round {round_id} did not reach a terminal state")


def active_snapshots(base: str) -> int:
    st, body = http("GET", base + "/snapshots")
    assert st == 200, body
    return body["active_count"]


def load_cases(db: Path) -> list[tuple[int, str]]:
    con = sqlite3.connect(db)
    try:
        return con.execute("SELECT days, event FROM cases").fetchall()
    finally:
        con.close()


def expected_curve(dbs: list[Path]) -> list[tuple[int, int, int, float]]:
    cases = [c for db in dbs for c in load_cases(db)]
    death_days = sorted({d for d, ev in cases if ev == "death"})
    s = 1.0
    out = []
    for t in death_days:
        n = sum(1 for d, _ in cases if d >= t)
        dd = sum(1 for d, ev in cases if d == t and ev == "death")
        s *= 1 - dd / n
        out.append((t, n, dd, s))
    return out


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    db_a, db_b, db_c = TMP / "site_a.db", TMP / "site_b.db", TMP / "coord.db"
    port_a, port_b = free_port(), free_port()
    A, B = f"http://127.0.0.1:{port_a}", f"http://127.0.0.1:{port_b}"

    run_server(create_site_app(SiteConfig(
        site_id="A", database_url=f"sqlite+aiosqlite:///{db_a}", seed="A", seed_cases=160,
        snapshot_ttl_seconds=SNAPSHOT_TTL)), port_a, "site-a")
    run_server(create_site_app(SiteConfig(
        site_id="B", database_url=f"sqlite+aiosqlite:///{db_b}", seed="B", seed_cases=140,
        response_delay_seconds=B_DELAY, snapshot_ttl_seconds=SNAPSHOT_TTL)), port_b, "site-b")

    def new_coordinator() -> tuple[str, tuple]:
        port = free_port()
        app = create_coord_app(CoordConfig(
            database_url=f"sqlite+aiosqlite:///{db_c}",
            site_a_url=A, site_b_url=B, site_request_timeout=30))
        handle = run_server(app, port, f"coordinator-{port}")
        return f"http://127.0.0.1:{port}", handle

    wait_health(A, "site-a")
    wait_health(B, "site-b")
    COORD, coord_handle = new_coordinator()
    wait_health(COORD, "coordinator")
    print("== all three services healthy ==\n")

    # ---- A. happy path -----------------------------------------------------
    st, r = http("POST", f"{COORD}/rounds")
    check("A0 create round -> 202", st == 202, f"got {st}: {r}")
    rid = r["round_id"]
    time.sleep(1.0)  # round is running; site B is slow
    t0 = time.time()
    st, body = http("GET", f"{COORD}/rounds/{rid}")
    poll_ms = (time.time() - t0) * 1000
    check("A1 status query fast while slow site computes", st == 200 and poll_ms < 1500,
          f"status={st} poll={poll_ms:.0f}ms")
    check("A2 no result published while running", st == 200 and "result" not in body, str(body))
    final = wait_final(COORD, rid)
    check("A3 round succeeded", final["status"] == "succeeded", str(final.get("error")))
    curve = final.get("result", {}).get("curve", [])
    want = expected_curve([db_a, db_b])
    ok = len(curve) == len(want) and all(
        row["t"] == t and row["n_risk"] == n and row["d"] == dd and abs(row["survival"] - s) < 1e-9
        for row, (t, n, dd, s) in zip(curve, want))
    check("A4 curve matches independent KM computation", ok,
          f"got {len(curve)} points, want {len(want)}")
    # no sleep: the moment the terminal state is visible, releases must be done
    check("A5 both sites released before terminal state", active_snapshots(A) == 0 and active_snapshots(B) == 0)
    st, doc = http("GET", f"{COORD}/openapi.json")
    check("A6 /docs openapi available", st == 200 and "paths" in doc)

    # ---- B. cancel a round while the slow site is mid-call -----------------
    st, r = http("POST", f"{COORD}/rounds")
    rid_b = r["round_id"]
    time.sleep(1.0)  # site B is inside its delay
    t0 = time.time()
    st, r = http("POST", f"{COORD}/rounds/{rid_b}/cancel")
    cancel_ms = (time.time() - t0) * 1000
    check("B1 cancel returns fast despite slow site", st == 200 and cancel_ms < 2000,
          f"status={st} {cancel_ms:.0f}ms")
    check("B2 status becomes cancelling", r.get("status") == "cancelling", str(r))
    final = wait_final(COORD, rid_b, timeout=30)
    check("B3 round reaches cancelled", final["status"] == "cancelled", str(final))
    check("B4 no result after cancel", "result" not in final)
    # no sleep: release happens strictly before the terminal state is published
    check("B5 both sites released before terminal state", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- C. slot limit: round fails, both sites released -------------------
    st, s1 = http("POST", f"{A}/snapshots", {"round_id": "manual-1"})
    st2, s2 = http("POST", f"{A}/snapshots", {"round_id": "manual-2"})
    check("C0 two manual snapshots occupy both slots",
          st == 201 and st2 == 201 and active_snapshots(A) == 2)
    st, r = http("POST", f"{A}/snapshots", {"round_id": "manual-3"})
    check("C1 third snapshot rejected (409)", st == 409, f"got {st}: {r}")
    st, r = http("POST", f"{COORD}/rounds")
    rid_c = r["round_id"]
    final = wait_final(COORD, rid_c, timeout=30)
    check("C2 round fails when a site has no free slot", final["status"] == "failed", str(final))
    check("C3 failure mentions slots", "slot" in (final.get("error") or "").lower(),
          str(final.get("error")))
    check("C4 site B snapshot released after failure", active_snapshots(B) == 0)
    http("DELETE", f"{A}/snapshots/{s1['snapshot_id']}")
    http("DELETE", f"{A}/snapshots/{s2['snapshot_id']}")
    check("C5 manual snapshots cleaned up", active_snapshots(A) == 0)

    # ---- D. late create after terminal state is rejected -------------------
    st, r = http("POST", f"{A}/snapshots", {"round_id": rid_c})
    check("D1 late create for failed round rejected", st == 409, f"got {st}: {r}")
    st, r = http("POST", f"{B}/snapshots", {"round_id": rid_b})
    check("D2 late create for cancelled round rejected", st == 409, f"got {st}: {r}")
    check("D3 no slot occupied by late creates", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- E. idempotent create per round ------------------------------------
    st, i1 = http("POST", f"{A}/snapshots", {"round_id": "idem-1"})
    st2, i2 = http("POST", f"{A}/snapshots", {"round_id": "idem-1"})
    check("E1 retry returns same snapshot", st == 201 and st2 in (200, 201)
          and i1["snapshot_id"] == i2["snapshot_id"], f"{i1} vs {i2}")
    check("E2 only one slot occupied", active_snapshots(A) == 1)
    http("DELETE", f"{A}/snapshots/{i1['snapshot_id']}")

    # ---- F. direct SQL correction does not wait for computation ------------
    st, r = http("POST", f"{COORD}/rounds")
    rid_f = r["round_id"]
    time.sleep(1.0)  # round in flight
    t0 = time.time()
    con = sqlite3.connect(db_a)
    con.execute("PRAGMA busy_timeout=5000")
    victim = con.execute("SELECT case_no FROM cases WHERE event='censored' LIMIT 1").fetchone()[0]
    con.execute("UPDATE cases SET days = days + 1 WHERE case_no = ?", (victim,))
    con.commit()
    con.close()
    fix_ms = (time.time() - t0) * 1000
    check("F1 direct SQL correction immediate during computation", fix_ms < 2000, f"{fix_ms:.0f}ms")
    final = wait_final(COORD, rid_f)
    check("F2 round still completes after correction", final["status"] == "succeeded",
          str(final.get("error")))

    # ---- G. coordinator crash -> TTL release -> restart -> recovery --------
    st, r = http("POST", f"{COORD}/rounds")
    rid_g = r["round_id"]
    time.sleep(6.0)  # snapshots created on both sites, site B still computing
    check("G0 snapshots active while round runs", active_snapshots(A) == 1 and active_snapshots(B) == 1)
    freeze(coord_handle)  # "SIGKILL": no cleanup runs any more
    time.sleep(0.5)
    check("G1 slots stay occupied right after crash",
          active_snapshots(A) == 1 and active_snapshots(B) == 1)
    t0 = time.time()
    while time.time() - t0 < SNAPSHOT_TTL + 20:
        if active_snapshots(A) == 0 and active_snapshots(B) == 0:
            break
        time.sleep(1.0)
    check("G2 TTL auto-releases both sites after coordinator death",
          active_snapshots(A) == 0 and active_snapshots(B) == 0)

    COORD2, coord2_handle = new_coordinator()  # same DB, new process/port
    wait_health(COORD2, "coordinator2")
    st, body = http("GET", f"{COORD2}/rounds/{rid_g}")
    check("G3 interrupted round marked failed after restart",
          st == 200 and body["status"] == "failed" and "restart" in (body.get("error") or "").lower(),
          str(body))
    st, r = http("POST", f"{COORD2}/rounds")
    rid_g2 = r["round_id"]
    final = wait_final(COORD2, rid_g2)
    check("G4 new round completes after restart (no stale snapshot reuse)",
          final["status"] == "succeeded", str(final.get("error")))
    check("G5 sites clean after recovery round", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- H. graceful coordinator stop releases in-flight snapshots ---------
    st, r = http("POST", f"{COORD2}/rounds")
    time.sleep(6.0)  # in flight, snapshots occupied on both sites
    check("H0 snapshots occupied mid-round", active_snapshots(A) == 1 and active_snapshots(B) == 1)
    coord2_handle[0].should_exit = True  # like `docker stop` (SIGTERM)
    coord2_handle[2].join(timeout=20)
    time.sleep(0.5)
    check("H1 graceful stop releases both sites", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        for server, _loop, _t in servers:
            server.should_exit = True
    sys.exit(rc)
