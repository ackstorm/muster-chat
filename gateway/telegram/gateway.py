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
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(65, connect=10))

    def scrub(self, s):
        """Never let the raw bot token (embedded in every request URL) reach a log line."""
        return s.replace(self.token, "bot***")

    async def updates(self, offset):
        resp = await self.http.get(f"{self.base}/getUpdates",
                                   params={"timeout": 50, "offset": offset})
        resp.raise_for_status()
        return resp.json().get("result", [])

    async def send(self, chat_id, text):
        # Telegram caps messages at 4096 chars — split long agent replies
        for i in range(0, max(len(text), 1), 4000):
            chunk = text[i:i + 4000]
            for attempt in range(3):
                resp = await self.http.post(f"{self.base}/sendMessage",
                                            json={"chat_id": chat_id, "text": chunk})
                if resp.status_code == 200:
                    break
                if resp.status_code == 429:
                    retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                    log(f"telegram 429 chat={chat_id}; retry_after={retry_after}s (attempt {attempt + 1}/3)")
                    await asyncio.sleep(retry_after)
                    continue
                log(f"telegram sendMessage failed chat={chat_id} status={resp.status_code}: {self.scrub(resp.text)}")
                raise RuntimeError(f"telegram sendMessage failed: HTTP {resp.status_code}")
            else:
                log(f"telegram sendMessage rate-limited chat={chat_id}; giving up after 3 attempts")
                raise RuntimeError("telegram sendMessage: retries exhausted (429)")


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
                log(f"stream err chat={chat_id}: {self.tg.scrub(repr(e))}; retry in {backoff}s")
            await bus.aclose()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def drain(self, chat_id, bus):
        # A coalesced unread nudge can stand for a backlog > one page — loop until empty.
        while True:
            msgs = (await bus.rpc("fetch", {"limit": 20}))["messages"]
            if not msgs:
                return
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
                text = ("Your reachable agents:\n" + "\n".join(
                    f"- {a['addr']} — {a['status']}" for a in peers)) if peers else "No agents visible."
                await self.tg.send(chat_id, text)
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
                log(f"telegram poll err {self.tg.scrub(repr(e))}; retry in 5s")
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
