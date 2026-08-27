"""Delegated auth (spec §5.3–5.4): forward the caller's key to an external resolver,
cache (user_id, groups) 60s, fail closed. Muster stores no users, groups, or keys."""
import hashlib
import time
from dataclasses import dataclass

import httpx

from . import config as config_mod, metrics


@dataclass(frozen=True)
class Identity:
    user_id: str
    groups: tuple[str, ...]


class AuthError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def _dig(obj, path: str):
    for part in path.split("."):
        obj = obj[part]
    return obj


class Authenticator:
    def __init__(self, cfg: config_mod.Config, http: httpx.AsyncClient | None = None):
        self.cfg = cfg
        self.http = http or httpx.AsyncClient(timeout=5.0)
        self._cache: dict[str, tuple[Identity, float]] = {}  # sha256(key) -> (ident, fresh_until)

    async def resolve(self, key: str) -> Identity:
        if not key:
            raise AuthError(401, "unauthorized", "missing x-muster-api-key")
        if key in self.cfg.static_keys:  # local-dev mode (spec §16)
            entry = self.cfg.static_keys[key]
            return Identity(entry["user_id"], tuple(entry.get("groups", ())))
        kh = hashlib.sha256(key.encode()).hexdigest()  # never hold raw keys in memory maps/logs
        hit = self._cache.get(kh)
        if hit and hit[1] > time.monotonic():
            metrics.AUTH_CACHE.labels(result="hit").inc()
            return hit[0]
        metrics.AUTH_CACHE.labels(result="miss").inc()
        if not self.cfg.resolver_url:
            raise AuthError(401, "unauthorized", "no resolver configured and key not in static keys")
        try:
            with metrics.RESOLVER_LATENCY.time():
                resp = await self.http.get(self.cfg.resolver_url,
                                           headers={self.cfg.resolver_header: key})
        except httpx.HTTPError:
            raise AuthError(503, "resolver_unavailable", "identity resolver unreachable")  # fail closed
        if resp.status_code in (400, 401, 403):
            self._cache.pop(kh, None)  # "the resolver said no" is never served from cache
            raise AuthError(401, "unauthorized", "key refused by identity resolver")
        if resp.status_code >= 500:
            raise AuthError(503, "resolver_unavailable", f"resolver answered {resp.status_code}")
        data = resp.json()
        try:
            ident = Identity(str(_dig(data, self.cfg.user_field)),
                             tuple(_dig(data, self.cfg.groups_field) or ()))
        except (KeyError, TypeError):
            raise AuthError(502, "resolver_schema", "resolver response missing user/groups fields")
        self._cache[kh] = (ident, time.monotonic() + self.cfg.auth_cache_ttl)
        return ident
