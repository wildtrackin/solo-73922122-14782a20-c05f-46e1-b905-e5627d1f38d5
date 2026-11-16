"""HTTP client for the two hospital site nodes."""
from __future__ import annotations

import httpx

from .config import CoordConfig

SITE_IDS = ("a", "b")


class SiteError(Exception):
    """Any failure while talking to a site node (network or HTTP error)."""


class SiteClient:
    def __init__(self, cfg: CoordConfig):
        self.cfg = cfg

    def _base(self, site: str) -> str:
        if site == "a":
            return self.cfg.site_a_url.rstrip("/")
        if site == "b":
            return self.cfg.site_b_url.rstrip("/")
        raise ValueError(f"unknown site {site!r}")

    async def _request(self, method: str, site: str, path: str, *, json: dict | None = None,
                       timeout: float | None = None):
        url = self._base(site) + path
        try:
            async with httpx.AsyncClient(timeout=timeout or self.cfg.site_request_timeout) as client:
                resp = await client.request(method, url, json=json)
        except httpx.HTTPError as exc:
            raise SiteError(f"site {site} {method} {path}: {exc}") from exc
        if resp.status_code >= 400:
            raise SiteError(f"site {site} {method} {path}: HTTP {resp.status_code}: {resp.text[:300]}")
        return resp

    async def create_snapshot(self, site: str, round_id: str) -> str:
        resp = await self._request("POST", site, "/snapshots", json={"round_id": round_id})
        return str(resp.json()["snapshot_id"])

    async def death_days(self, site: str, snapshot_id: str) -> list[int]:
        resp = await self._request("GET", site, f"/snapshots/{snapshot_id}/death-days")
        return [int(x) for x in resp.json()["death_days"]]

    async def aggregate(self, site: str, snapshot_id: str, points: list[int]) -> list[dict]:
        resp = await self._request("POST", site, f"/snapshots/{snapshot_id}/aggregate",
                                   json={"points": points})
        return list(resp.json()["rows"])

    async def delete_snapshot(self, site: str, snapshot_id: str) -> None:
        await self._request("DELETE", site, f"/snapshots/{snapshot_id}",
                            timeout=self.cfg.site_cleanup_timeout)

    async def finalize_round(self, site: str, round_id: str) -> None:
        await self._request("POST", site, f"/rounds/{round_id}/finalize",
                            timeout=self.cfg.site_cleanup_timeout)
