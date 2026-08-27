"""GET /v1/stream — the delivery leg (spec §5.2, §11). One long-lived SSE response per
connected agent; the serving pod subscribes to the agent's notify channel; cross-pod
fan-out rides Valkey pub/sub, never pod memory."""
import json
import time
import uuid

import anyio
from fastapi import Request
from fastapi.responses import StreamingResponse

from . import store
from .auth import AuthError


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def stream_endpoint(request: Request, ident, addr) -> StreamingResponse:
    cfg, r = request.app.state.cfg, request.app.state.redis
    a = str(addr)
    connection_id = uuid.uuid4().hex
    try:
        meta = json.loads(request.headers.get("x-muster-meta") or "{}")
    except ValueError:
        meta = {}
    key = request.headers.get("x-muster-api-key", "")

    async def gen():
        nonlocal ident  # rebound on ping re-auth below when groups change
        await store.register_agent(r, addr, ident.groups, meta, cfg.agent_retention)
        await store.touch_presence(r, a, connection_id, cfg.presence_ttl)
        pubsub = r.pubsub()
        await pubsub.subscribe(store.notify_channel(a))
        try:
            count, oldest_ts = await store.unread_count(r, a)
            if count:  # coalesced nudge: a weekend of messages is ONE event (spec §11)
                yield _sse("deliver", {"event": "deliver", "kind": "unread",
                                       "count": count, "oldest_ts": oldest_ts})
            last_tick = time.monotonic()
            while True:
                if await request.is_disconnected():  # known trap: finally may not fire otherwise
                    return
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg:
                    yield f"event: deliver\ndata: {msg['data']}\n\n"  # forward verbatim
                if time.monotonic() - last_tick >= cfg.ping_interval:
                    try:
                        ident2 = await request.app.state.auth.resolve(key)  # §5.4: cache-bounded re-auth
                    except AuthError as e:
                        yield _sse("error", {"code": e.code, "message": "stream closed: " + e.message})
                        return
                    if tuple(ident2.groups) != tuple(ident.groups):
                        # groups changed since connect (or last ping) — refresh the registry
                        # entry so ACL/roster/search reflect it for the rest of the stream life.
                        await store.register_agent(r, addr, ident2.groups, meta, cfg.agent_retention)
                        ident = ident2
                    await store.touch_presence(r, a, connection_id, cfg.presence_ttl)
                    yield ": ping\n\n"
                    last_tick = time.monotonic()
        finally:
            # known trap: cleanup awaits inherit the same (already-cancelled) anyio cancel
            # scope on client disconnect and would themselves be cancelled before completing
            # — shield so presence cleanup always lands. Deadline bounds a blackholed Valkey
            # so shutdown can't stall forever.
            with anyio.move_on_after(5, shield=True):
                await pubsub.aclose()
                await store.clear_presence(r, a, connection_id)  # no-op if a successor took over

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
