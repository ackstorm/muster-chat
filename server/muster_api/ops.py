"""RPC handlers. Every rule the server enforces lives here or in identity.visible —
reference resolution, ACL, size cap, self-send, rate limits, server-stamped from."""
from . import identity, store
from .auth import Identity
from .identity import Address


class OpError(Exception):
    def __init__(self, status: int, payload: dict):
        super().__init__(payload.get("message", payload["code"]))
        self.status, self.payload = status, payload


async def _visible_agents(r, ident: Identity) -> list[dict]:
    return [a for a in await store.list_agents(r)
            if identity.visible(ident.user_id, ident.groups, a["user"], a["groups"])]


def _resolve_reference(ref: str, agents: list[dict]) -> dict:
    hits = [a for a in agents if identity.matches(ref, a["addr"])]
    if not hits:
        raise OpError(404, {"code": "agent_not_found",
                            "message": f"no visible agent matches {ref!r}",
                            "visible": [a["addr"] for a in agents]})
    if len(hits) > 1:
        raise OpError(409, {"code": "ambiguous_reference",
                            "message": f"{ref!r} matches {len(hits)} agents; use a longer reference",
                            "candidates": hits})
    return hits[0]


async def _check_rate(r, kind: str, sender: str, limit: int, window: int):
    ok, retry = await store.rate_check(r, kind, sender, limit, window)
    if not ok:
        raise OpError(429, {"code": "message_rate_exceeded", "retry_after": retry,
                            "limit": limit, "window": window,
                            "message": "Unusually high agent messaging rate; possible message "
                                       "loop. Wait until the retry window expires before continuing."})


async def op_chat(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    body = args.get("body") or ""
    subject, important = args.get("subject"), bool(args.get("important"))
    if len(body.encode()) > cfg.body_max:
        raise OpError(413, {"code": "message_too_large",
                            "message": f"body is {len(body.encode())} bytes; cap is {cfg.body_max}"})
    target = _resolve_reference(args.get("to") or "", await _visible_agents(r, ident))
    if target["addr"] == str(sender):
        raise OpError(400, {"code": "self_send", "message": "target resolves to the sending agent"})
    await _check_rate(r, "chat", str(sender), cfg.chat_rate, cfg.rate_window)
    env = store.envelope(str(sender), body, subject, important)
    fields = {"from": str(sender), "kind": "chat", "body": body, "summary": env,
              "important": "1" if important else "0"}
    if subject:
        fields["subject"] = subject
    msg_id = await store.append_message(r, target["addr"], fields, cfg.inbox_maxlen, cfg.message_ttl)
    await store.publish_deliver(r, target["addr"], {
        "event": "deliver", "kind": "chat", "msg_id": msg_id,
        "from": str(sender), "envelope": env, "important": important})
    return {"ok": True, "msg_id": msg_id, "to": target["addr"], "status": target["status"]}


async def op_fetch(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    limit = min(int(args.get("limit") or 20), 100)
    msgs = await store.fetch_unread(r, str(sender), limit)
    return {"ok": True, "messages": [
        {"msg_id": m["msg_id"], "from": m.get("from", ""), "kind": m.get("kind", ""),
         "subject": m.get("subject", ""), "body": m.get("body", ""), "ts": m.get("ts", ""),
         "important": m.get("important") == "1"} for m in msgs]}


def _public(a: dict) -> dict:
    """Roster row: groups stripped — membership is resolver business, not roster content."""
    return {k: a[k] for k in ("addr", "user", "host", "runtime", "project", "session", "status", "meta")}


async def op_roster(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    return {"ok": True, "agents": [_public(a) for a in await _visible_agents(r, ident)]}


async def op_search(r, cfg, ident: Identity, sender: Address, args: dict) -> dict:
    agents = await _visible_agents(r, ident)
    if args.get("user"):
        agents = [a for a in agents if a["user"] == args["user"]]
    if args.get("project"):
        agents = [a for a in agents if a["project"] == args["project"]]
    if args.get("runtime"):
        agents = [a for a in agents if a["runtime"] == args["runtime"]]
    if args.get("group"):
        agents = [a for a in agents if args["group"] in a["groups"]]
    if args.get("live"):
        agents = [a for a in agents if a["status"] == "online"]
    return {"ok": True, "agents": [_public(a) for a in agents]}


_OPS = {"chat": op_chat, "fetch": op_fetch, "roster": op_roster, "search": op_search}


async def dispatch(r, cfg, ident: Identity, sender: Address, op: str, args: dict) -> dict:
    handler = _OPS.get(op)
    if handler is None:
        raise OpError(400, {"code": "unknown_op", "message": f"unknown op {op!r}",
                            "ops": sorted(_OPS)})
    return await handler(r, cfg, ident, sender, args)
