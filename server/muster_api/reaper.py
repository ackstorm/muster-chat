"""GC for expired agent identities (spec §7): when the agent hash TTL has fired,
remove the orphaned inbox, cursor, presence, and directory entry. This — not
compliance machinery — is the v1 retention story."""
import asyncio
import logging

from . import store

log = logging.getLogger("muster.reaper")


async def reap_once(r) -> int:
    reaped = 0
    for a in await r.smembers(store.AGENTS):
        if not await r.exists(store.agent_key(a)):
            await r.delete(store.inbox_key(a), store.cursor_key(a), store.presence_key(a))
            await r.srem(store.AGENTS, a)
            reaped += 1
    if reaped:
        log.info("reaped %d expired agents", reaped)
    return reaped


async def run_forever(r, interval: int = 600) -> None:
    while True:
        try:
            await reap_once(r)
        except Exception:  # noqa: BLE001 — the reaper must never kill the app
            log.exception("reap cycle failed")
        await asyncio.sleep(interval)
