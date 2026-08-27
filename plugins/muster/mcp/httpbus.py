# plugins/muster/mcp/httpbus.py
"""HTTP client for the central muster-api bus (spec v2 §5): POST /v1/rpc + SSE /v1/stream.
The only module that talks to the network; muster_channel renders, this transports."""
import json


class BusError(Exception):
    """Non-2xx rpc answer. payload is the server's machine-readable error body
    (code/message plus op-specific fields: visible, candidates, retry_after…)."""

    def __init__(self, status, payload):
        super().__init__(payload.get("message", payload.get("code", str(status))))
        self.status, self.payload = status, payload


async def parse_sse(lines):
    """Parse an async iterator of text lines into event dicts {"_event": name, **data}.
    Comment frames (': ping') and events without data are dropped. Malformed JSON data
    yields an empty payload rather than raising — one bad frame must not kill the relay."""
    event, data = None, []
    async for line in lines:
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
        # anything else (comments, unknown fields): ignored per SSE spec


class MusterClient:
    def __init__(self, url, api_key, agent, meta=None):
        self.url = url.rstrip("/")
        self.headers = {"x-muster-api-key": api_key, "x-muster-agent": agent}
        self.meta = meta or {}
        self._http = None  # lazy: import/construct on first use so startup never blocks

    async def _client(self):
        if self._http is None:
            import httpx
            self._http = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=10))
        return self._http

    async def rpc(self, op, args=None):
        c = await self._client()
        resp = await c.post(f"{self.url}/v1/rpc",
                            json={"op": op, "args": args or {}}, headers=self.headers)
        try:
            data = resp.json()
        except ValueError:
            data = {"code": "bad_response", "message": f"non-JSON answer ({resp.status_code})"}
        if resp.status_code >= 400:
            raise BusError(resp.status_code, data)
        return data

    async def stream(self):
        """One SSE connection; yields parsed events. Raises on disconnect/timeout —
        the caller owns reconnect + backoff. Read timeout 45s: server pings every 15s,
        so three missed pings = dead connection."""
        import httpx
        c = await self._client()
        headers = dict(self.headers)
        headers["x-muster-meta"] = json.dumps(self.meta)
        async with c.stream("GET", f"{self.url}/v1/stream", headers=headers,
                            timeout=httpx.Timeout(None, connect=10, read=45)) as resp:
            resp.raise_for_status()
            async for ev in parse_sse(resp.aiter_lines()):
                yield ev
