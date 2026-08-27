# Telegram Gateway ("beer mode") Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone bus-client service that lets a human message their agents from Telegram — inbound "how's the feature going?" reaches an agent as an ordinary bus chat; agent replies come back as Telegram messages (spec v2 §12.3).

**Architecture:** One small Python service (~250 LOC, single file + store). It long-polls the Telegram Bot API (`getUpdates`) and, per paired Telegram chat, holds one SSE stream on muster-api as endpoint `{user}/cloud/telegram/-/bot`. Zero server changes: the gateway is just another client; the ACL (same user) already covers it. Pairing maps `chat_id → bus API key`; the key is a per-purpose bus-scoped key the user supplies — the gateway MUST NOT hold primary LiteLLM inference keys.

**Tech Stack:** Python 3.12, httpx only (no Telegram framework), pytest. Docker multi-stage image + compose profile.

**Spec:** `docs/superpowers/specs/2026-08-27-muster-v1-central-bus-spec-v2.md` §12.3, §15. Wire contract = live code in `server/muster_api/ops.py` / `stream.py` (same as the shims; see also `plugins/muster/mcp/httpbus.py` after Plan 2 — the gateway deliberately duplicates the tiny rpc/SSE helpers instead of importing across packages: it is a separately deployed service and ~60 duplicated lines beat a shared package).

## Global Constraints

- The gateway is an untrusted-side CLIENT of muster-api: no Valkey access, no server code changes, no new server endpoints.
- **Credentials:** per-chat bus keys only. Never ask for, store, or log a LiteLLM inference key; docs and `/pair` copy must say "bus-scoped key". Keys at rest in the pairing file; the file is mode `0600`; log key **hashes** only.
- **Address:** `x-muster-agent: cloud/telegram/-/bot` (host=`cloud`, runtime=`telegram`, project=`-`, session=`bot`). One stream per paired chat, under that chat's key.
- **Env:** `TELEGRAM_BOT_TOKEN` (required), `MUSTER_URL` (default `http://localhost:8765`), `MUSTER_PAIRING_FILE` (default `./pairings.json`).
- Rate/size/resolution errors from the bus are relayed back to the Telegram chat verbatim-ish (human-readable line with `retry_after`/candidates) — never silently dropped.
- Fail-safe: Telegram API down ⇒ retry with backoff; muster-api down ⇒ streams reconnect with backoff (1s→60s); a bad pairing key ⇒ tell the chat, keep the pairing (revocation is the identity platform's job — key may come back).
- Test budget: pure tests for command parsing + pairing store; NO Telegram API mocking suites, NO live-bot tests.

## File Structure

```
gateway/telegram/
  gateway.py        CREATE  everything: pairing store, command parsing, bus client, telegram loop, stream relays
  Dockerfile        CREATE  multi-stage (uv/pip → slim runtime), explicit COPY paths
gateway/tests/
  test_gateway.py   CREATE  parse_command + PairingStore (pure, tmp_path)
docker-compose.yml  MODIFY  telegram-gateway service under profile "gateway"
README.md           MODIFY  short "Telegram gateway" section (Task 2)
```

---

### Task 1: Gateway core — pairing store, command parsing, bus client

**Files:**
- Create: `gateway/telegram/gateway.py`
- Create: `gateway/tests/test_gateway.py`
- Create: `gateway/tests/__init__.py` (empty), `gateway/telegram/__init__.py` (empty)

**Interfaces:**
- Produces (used by Task 2 inside the same file): `PairingStore(path)` with `get(chat_id) -> dict | None`, `set(chat_id, key)`, `remove(chat_id)`, `set_last(chat_id, addr)`; `parse_command(text) -> tuple`; `Bus(url, api_key)` with `async rpc(op, args=None)`, `async events()` (SSE async-generator), `BusError`.

- [ ] **Step 1: Write the failing tests**

```python
# gateway/tests/test_gateway.py
"""Pure logic: command parsing + pairing store. The network loops are exercised live, not here."""
import json

from gateway.telegram.gateway import PairingStore, parse_command


def test_parse_commands():
    assert parse_command("/pair sk-bus-abc") == ("pair", "sk-bus-abc")
    assert parse_command("/unpair") == ("unpair", None)
    assert parse_command("/roster") == ("roster", None)
    assert parse_command("@muster-chat how is the feature going?") == ("chat", ("muster-chat", "how is the feature going?"))
    assert parse_command("plain reply text") == ("reply", "plain reply text")
    assert parse_command("/pair") == ("help", None)          # missing arg → usage
    assert parse_command("/nonsense") == ("help", None)


def test_pairing_store_roundtrip(tmp_path):
    p = tmp_path / "pairings.json"
    s = PairingStore(str(p))
    assert s.get(11) is None
    s.set(11, "sk-bus-abc")
    s.set_last(11, "jc/laptop/claude/muster-chat/1")
    assert s.get(11)["key"] == "sk-bus-abc"
    assert s.get(11)["last"] == "jc/laptop/claude/muster-chat/1"
    # survives reload + file is private
    s2 = PairingStore(str(p))
    assert s2.get(11)["key"] == "sk-bus-abc"
    assert (p.stat().st_mode & 0o777) == 0o600
    s2.remove(11)
    assert PairingStore(str(p)).get(11) is None
    # raw file never holds anything but chat_id/key/last
    assert set(json.loads(p.read_text()).get("11", {"key": 0, "last": 0}).keys()) <= {"key", "last"}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run --with httpx --with pytest --no-project pytest gateway/tests -v
```

Expected: FAIL with `ModuleNotFoundError: gateway`

- [ ] **Step 3: Write `gateway/telegram/gateway.py`**

```python
#!/usr/bin/env python
"""telegram-gateway — a human on Telegram is just another endpoint on the Muster bus
(spec v2 §12.3). Standalone bus CLIENT: pairs a Telegram chat with a per-purpose
bus-scoped API key, registers as {user}/cloud/telegram/-/bot, relays both directions.

Run: TELEGRAM_BOT_TOKEN=... uv run --with httpx --no-project python gateway/telegram/gateway.py
"""
import asyncio
import hashlib
import json
import os
import sys

MUSTER_URL = os.environ.get("MUSTER_URL", "http://localhost:8765").rstrip("/")
PAIRING_FILE = os.environ.get("MUSTER_PAIRING_FILE", "./pairings.json")
AGENT = "cloud/telegram/-/bot"          # host/runtime/project/session; user is server-stamped
HELP = (
    "Muster gateway commands:\n"
    "/pair <bus-key> — link this chat to your Muster user (use a bus-scoped key, NEVER your inference key)\n"
    "/unpair — remove the link\n"
    "/roster — list your reachable agents\n"
    "@<agent-ref> <text> — message that agent\n"
    "plain text — reply to the agent that last wrote to you"
)


def log(msg):
    print(f"[telegram-gateway] {msg}", file=sys.stderr, flush=True)


def khash(key):
    return hashlib.sha256(key.encode()).hexdigest()[:12]


# ---------- pairing store (chat_id -> {key, last}) ----------
class PairingStore:
    """JSON file, chmod 0600. Tiny and synchronous — a handful of chats, not a database."""

    def __init__(self, path):
        self.path = path
        try:
            with open(path) as f:
                self._d = json.load(f)
        except (OSError, ValueError):
            self._d = {}

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self._d, f)
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    def get(self, chat_id):
        return self._d.get(str(chat_id))

    def set(self, chat_id, key):
        self._d[str(chat_id)] = {"key": key}
        self._save()

    def set_last(self, chat_id, addr):
        e = self._d.get(str(chat_id))
        if e is not None:
            e["last"] = addr
            self._save()

    def remove(self, chat_id):
        self._d.pop(str(chat_id), None)
        self._save()


# ---------- command parsing ----------
def parse_command(text):
    """→ ("pair", key) | ("unpair", None) | ("roster", None) | ("chat", (ref, body))
       | ("reply", body) | ("help", None)"""
    text = (text or "").strip()
    if text.startswith("/pair"):
        parts = text.split(maxsplit=1)
        return ("pair", parts[1].strip()) if len(parts) == 2 and parts[1].strip() else ("help", None)
    if text == "/unpair":
        return ("unpair", None)
    if text == "/roster":
        return ("roster", None)
    if text.startswith("/"):
        return ("help", None)
    if text.startswith("@"):
        parts = text[1:].split(maxsplit=1)
        if len(parts) == 2:
            return ("chat", (parts[0], parts[1]))
        return ("help", None)
    if text:
        return ("reply", text)
    return ("help", None)


# ---------- bus client (deliberate small duplicate of the shim's httpbus) ----------
class BusError(Exception):
    def __init__(self, status, payload):
        super().__init__(payload.get("message", payload.get("code", str(status))))
        self.status, self.payload = status, payload


class Bus:
    def __init__(self, url, api_key):
        import httpx
        self.url = url
        self.headers = {"x-muster-api-key": api_key, "x-muster-agent": AGENT}
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=10))

    async def rpc(self, op, args=None):
        resp = await self.http.post(f"{self.url}/v1/rpc",
                                    json={"op": op, "args": args or {}}, headers=self.headers)
        try:
            data = resp.json()
        except ValueError:
            data = {"code": "bad_response", "message": f"HTTP {resp.status_code}"}
        if resp.status_code >= 400:
            raise BusError(resp.status_code, data)
        return data

    async def events(self):
        """One SSE connection; yields {"_event": name, **payload}. Caller owns reconnect."""
        import httpx
        headers = dict(self.headers)
        headers["x-muster-meta"] = json.dumps({"cwd": "telegram"})
        async with self.http.stream("GET", f"{self.url}/v1/stream", headers=headers,
                                    timeout=httpx.Timeout(None, connect=10, read=45)) as resp:
            resp.raise_for_status()
            event, data = None, []
            async for line in resp.aiter_lines():
                if line == "":
                    if event and data:
                        try:
                            payload = json.loads("\n".join(data))
                        except ValueError:
                            payload = {}
                        yield {"_event": event, **payload}
                    event, data = None, []
                elif line.startswith("event:"):
                    event = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data.append(line[len("data:"):].strip())

    async def aclose(self):
        await self.http.aclose()


def fmt_bus_error(e):
    p = e.payload
    msg = p.get("message") or p.get("code") or "bus error"
    if p.get("visible"):
        msg += "\nVisible: " + ", ".join(p["visible"])
    if p.get("candidates"):
        msg += "\nCandidates: " + ", ".join(c["addr"] for c in p["candidates"])
    if "retry_after" in p:
        msg += f"\nRetry in {p['retry_after']}s."
    return msg


# ---------- telegram API (long poll; no framework) ----------
class Telegram:
    def __init__(self, token):
        import httpx
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(65, connect=10))

    async def updates(self, offset):
        resp = await self.http.get(f"{self.base}/getUpdates",
                                   params={"timeout": 50, "offset": offset})
        resp.raise_for_status()
        return resp.json().get("result", [])

    async def send(self, chat_id, text):
        # Telegram caps messages at 4096 chars — split long agent replies
        for i in range(0, max(len(text), 1), 4000):
            await self.http.post(f"{self.base}/sendMessage",
                                 json={"chat_id": chat_id, "text": text[i:i + 4000]})


# ---------- gateway wiring ----------
class Gateway:
    def __init__(self, tg, store):
        self.tg, self.store = tg, store
        self.streams = {}   # chat_id -> asyncio.Task holding that pairing's SSE relay

    # -- outbound leg: bus deliver events -> telegram messages --
    async def relay(self, chat_id, key):
        backoff = 1
        while True:
            bus = Bus(MUSTER_URL, key)
            try:
                log(f"stream open chat={chat_id} key#{khash(key)}")
                async for ev in bus.events():
                    backoff = 1
                    if ev["_event"] == "error":
                        await self.tg.send(chat_id, f"⚠️ Muster closed the stream: {ev.get('message', ev.get('code'))}")
                        break
                    if ev["_event"] != "deliver":
                        continue
                    if ev.get("kind") == "announce":
                        subj = f" [{ev['subject']}]" if ev.get("subject") else ""
                        await self.tg.send(chat_id, f"📢 {ev.get('from', '?')}{subj}: {ev.get('body', '')}")
                    else:   # chat envelope or coalesced unread → drain full bodies
                        await self.drain(chat_id, bus)
            except asyncio.CancelledError:
                await bus.aclose()
                raise
            except Exception as e:
                log(f"stream err chat={chat_id}: {e!r}; retry in {backoff}s")
            await bus.aclose()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def drain(self, chat_id, bus):
        msgs = (await bus.rpc("fetch", {"limit": 20}))["messages"]
        for m in msgs:
            self.store.set_last(chat_id, m["from"])   # bare text replies go here
            mark = "❗ " if m.get("important") else ""
            subj = f" [{m['subject']}]" if m.get("subject") else ""
            await self.tg.send(chat_id, f"{mark}✉ {m['from']}{subj}:\n{m['body']}")

    def start_stream(self, chat_id, key):
        self.stop_stream(chat_id)
        self.streams[chat_id] = asyncio.create_task(self.relay(chat_id, key))

    def stop_stream(self, chat_id):
        task = self.streams.pop(chat_id, None)
        if task:
            task.cancel()

    # -- inbound leg: telegram messages -> bus ops --
    async def handle(self, chat_id, text):
        cmd, arg = parse_command(text)
        pairing = self.store.get(chat_id)

        if cmd == "pair":
            probe = Bus(MUSTER_URL, arg)
            try:
                await probe.rpc("roster")             # validates the key against the bus
            except BusError as e:
                await self.tg.send(chat_id, "Pairing failed: " + fmt_bus_error(e))
                return
            except Exception:
                await self.tg.send(chat_id, f"Pairing failed: muster-api unreachable at {MUSTER_URL}.")
                return
            finally:
                await probe.aclose()
            self.store.set(chat_id, arg)
            self.start_stream(chat_id, arg)
            await self.tg.send(chat_id, "Paired ✅ — you are on the bus as your Telegram endpoint. "
                                        "/roster shows your agents; @<agent> <text> messages one.")
            return

        if pairing is None:
            await self.tg.send(chat_id, "Not paired. " + HELP)
            return
        bus = Bus(MUSTER_URL, pairing["key"])
        try:
            if cmd == "unpair":
                self.stop_stream(chat_id)
                self.store.remove(chat_id)
                await self.tg.send(chat_id, "Unpaired. The key was forgotten; revoke it at the identity platform too.")
            elif cmd == "roster":
                agents = (await bus.rpc("roster"))["agents"]
                peers = [a for a in agents if not a["addr"].endswith("/" + AGENT)]
                await self.tg.send(chat_id, "Your reachable agents:\n" + "\n".join(
                    f"- {a['addr']} — {a['status']}" for a in peers) if peers else "No agents visible.")
            elif cmd == "chat":
                ref, body = arg
                res = await bus.rpc("chat", {"to": ref, "body": body})
                self.store.set_last(chat_id, res["to"])
                await self.tg.send(chat_id, f"→ delivered to {res['to']} ({res['status']}).")
            elif cmd == "reply":
                last = pairing.get("last")
                if not last:
                    await self.tg.send(chat_id, "No conversation yet — address an agent: @<agent-ref> <text>. "
                                                "/roster lists them.")
                else:
                    res = await bus.rpc("chat", {"to": last, "body": arg})
                    await self.tg.send(chat_id, f"→ delivered to {res['to']} ({res['status']}).")
            else:
                await self.tg.send(chat_id, HELP)
        except BusError as e:
            await self.tg.send(chat_id, "⚠️ " + fmt_bus_error(e))
        except Exception as e:
            await self.tg.send(chat_id, f"⚠️ bus unreachable ({e.__class__.__name__}).")
        finally:
            await bus.aclose()

    async def run(self):
        # resume streams for existing pairings
        for cid, e in list(self.store._d.items()):
            self.start_stream(int(cid), e["key"])
        offset = 0
        while True:
            try:
                for u in await self.tg.updates(offset):
                    offset = u["update_id"] + 1
                    msg = u.get("message") or {}
                    chat_id, text = msg.get("chat", {}).get("id"), msg.get("text", "")
                    if chat_id is None or msg.get("chat", {}).get("type") != "private":
                        continue                     # DMs only — a group chat must never hold a key
                    await self.handle(chat_id, text)
            except Exception as e:
                log(f"telegram poll err {e!r}; retry in 5s")
                await asyncio.sleep(5)


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        log("TELEGRAM_BOT_TOKEN is required")
        sys.exit(1)
    gw = Gateway(Telegram(token), PairingStore(PAIRING_FILE))
    log(f"gateway up — bus {MUSTER_URL}, pairings {PAIRING_FILE}")
    asyncio.run(gw.run())


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests**

```bash
uv run --with httpx --with pytest --no-project pytest gateway/tests -v
```

Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway
git commit -m "feat(gateway): telegram gateway core — pairing store, commands, bus client"
```

---

### Task 2: Packaging + live smoke against local muster-api

**Files:**
- Create: `gateway/telegram/Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: `gateway/telegram/gateway.py` (Task 1), running muster-api from compose.

- [ ] **Step 1: Create `gateway/telegram/Dockerfile`**

```dockerfile
FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install --target=/app/deps httpx

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/deps /usr/local/lib/python3.12/site-packages
COPY gateway/telegram/gateway.py .
ENV MUSTER_PAIRING_FILE=/data/pairings.json
VOLUME /data
CMD ["python", "gateway.py"]
```

(Build context is the repo root — compose sets it below. Explicit COPY only; no `COPY . .`.)

- [ ] **Step 2: Add the compose service (profile `gateway`)**

Append to `docker-compose.yml` services (match the file's existing style/indentation):

```yaml
  telegram-gateway:
    build:
      context: .
      dockerfile: gateway/telegram/Dockerfile
    profiles: ["gateway"]
    environment:
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:?set TELEGRAM_BOT_TOKEN to enable the gateway}
      MUSTER_URL: http://muster-api:8765
    volumes:
      - telegram-pairings:/data
    depends_on:
      muster-api:
        condition: service_healthy
    restart: unless-stopped
```

And under top-level `volumes:` add `telegram-pairings:`. Check first how the muster-api service and existing volumes are declared and follow that shape; if muster-api has no healthcheck, use plain `depends_on: [muster-api]`.

- [ ] **Step 3: Build + config check**

```bash
docker compose --profile gateway build telegram-gateway
TELEGRAM_BOT_TOKEN=dummy docker compose --profile gateway config -q
```

Expected: image builds; config validates. (No live bot run — that needs a real token.)

- [ ] **Step 4: Wire smoke without Telegram**

The bus leg is testable without a bot token — drive `Gateway.handle` directly against local muster-api (compose up), with a fake `Telegram` that records `send` calls:

```bash
uv run --with httpx --no-project python - <<'EOF'
import asyncio, sys
sys.path.insert(0, "gateway/telegram")
import gateway as G

class FakeTG:
    def __init__(self): self.sent = []
    async def send(self, chat_id, text): self.sent.append(text); print(f"[to {chat_id}] {text[:120]}")

async def main():
    store = G.PairingStore("/tmp/muster-gw-smoke.json")
    gw = G.Gateway(FakeTG(), store)
    await gw.handle(1, "/pair dev-key")        # pairs against compose static key
    await gw.handle(1, "/roster")
    await gw.handle(1, "plain text with no last correspondent")
    gw.stop_stream(1)
    assert any("Paired" in t for t in gw.tg.sent), gw.tg.sent
    assert any("No conversation yet" in t for t in gw.tg.sent), gw.tg.sent
    print("SMOKE OK")

asyncio.run(main())
EOF
```

Expected: `Paired ✅ …`, a roster (or "No agents visible."), the no-correspondent hint, `SMOKE OK`.

- [ ] **Step 5: README section**

Add a short "Telegram gateway (beer mode)" section to `README.md`: what it is (message your agents from Telegram), how to run (`TELEGRAM_BOT_TOKEN=… docker compose --profile gateway up -d`), the pairing flow (`/pair <bus-scoped key>` in a DM with the bot — **never your inference key**), commands (`/roster`, `@agent text`, plain text = reply to last), and the revocation note (unpair forgets the key locally; revoke at the identity platform).

- [ ] **Step 6: Commit**

```bash
git add gateway docker-compose.yml README.md
git commit -m "feat(gateway): package telegram-gateway — Dockerfile, compose profile, README"
```

---

## Verification (whole plan)

1. `uv run --with httpx --with pytest --no-project pytest gateway/tests -v` — green.
2. Wire smoke (Task 2 Step 4) prints `SMOKE OK` against compose.
3. `docker compose --profile gateway build` succeeds; `docker compose config -q` validates.
4. Grep gate: `grep -ri "litellm" gateway/` returns nothing (no inference-key coupling); `/pair` copy says "bus-scoped key".
5. Manual (post-merge, needs a real bot token): create a bot with @BotFather, `TELEGRAM_BOT_TOKEN=… docker compose --profile gateway up -d`, DM `/pair dev-key`, `/roster`, `@<agent> hola` and watch the agent receive it; reply from the agent to `telegram` and see it in the chat.
