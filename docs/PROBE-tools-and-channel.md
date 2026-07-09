# PROBE: MCP tools + `claude/channel` push coexisting on one stdio session

**Question:** can a single low-level `mcp.server.lowlevel.Server` stdio session both
serve tool calls (`tools/list`, `tools/call`) AND proactively push a
`notifications/claude/channel` notification, sharing ONE `ServerSession`?

**Answer: yes.** De-risked and verified deterministically (no live Claude needed).

## Why `Server.run()` doesn't work as-is

Reading `mcp.server.lowlevel.Server.run` source directly (`uv run --with mcp
--no-project python -c "import inspect,mcp.server.lowlevel as m;
print(inspect.getsource(m.Server.run))"`) shows it builds its own `ServerSession`
internally with no parameter to inject a pre-built one:

```python
async with AsyncExitStack() as stack:
    lifespan_context = await stack.enter_async_context(self.lifespan(self))
    session = await stack.enter_async_context(
        ServerSession(read_stream, write_stream, initialization_options, stateless=stateless)
    )
    async with anyio.create_task_group() as tg:
        async for message in session.incoming_messages:
            tg.start_soon(self._handle_message, message, session, lifespan_context, raise_exceptions)
```

So a background pusher task started outside `srv.run()` has no handle to `session`.

## The working pattern (strategy b from the brief)

Don't call `srv.run()`. Build the `ServerSession` manually (same as the existing
`muster_channel.py` pusher-only server), then manually replicate `Server.run()`'s loop —
call `Server._handle_message` (the same private method `Server.run` calls internally)
for each incoming message. This gives the pusher task and the request-dispatch task
the same `session` object.

```python
# plugins/muster/mcp/_probe.py (working main())
async def main():
    init = srv.create_initialization_options(experimental_capabilities={"claude/channel": {}})
    async with stdio_server() as (read, write):
        async with ServerSession(read, write, init) as session:
            async with srv.lifespan(srv) as lifespan_context:
                async with anyio.create_task_group() as tg:
                    tg.start_soon(_push_later, session)
                    tg.start_soon(_dispatch_loop, session, lifespan_context, tg)

async def _dispatch_loop(session, lifespan_context, tg):
    async for message in session.incoming_messages:
        tg.start_soon(srv._handle_message, message, session, lifespan_context, True)

async def _push_later(session):
    await anyio.sleep(2)
    note = JSONRPCNotification(jsonrpc="2.0", method="notifications/claude/channel",
        params={"content": "probe channel push OK", "meta": {"kind": "probe"}})
    await session._write_stream.send(SessionMessage(message=JSONRPCMessage(note)))
```

`srv.list_tools()` / `srv.call_tool()` decorators still register handlers on `srv` as
normal — `Server._handle_message` dispatches to them exactly like `Server.run()` would.

## Client-side gotcha: `ClientSession` silently drops the notification

The brief's suggested `ClientSession(message_handler=...)` client did NOT see the
push. Root cause, found by reading `mcp.shared.session.BaseSession._receive_loop`:
every inbound notification is `model_validate`-d against the closed
`types.ServerNotification` discriminated union; `notifications/claude/channel` matches
none of the known variants, so the SDK **warn-logs and drops it** before it ever
reaches `_received_notification`/`message_handler`. The tool call worked fine
(`CLIENT: tool call OK`), but `got_push` never fired within the 6s window.

Fix: don't use `ClientSession`'s typed dispatch for this. Hand-roll the
`initialize`/`initialized` handshake and the `tools/call` request over the raw
`stdio_client` streams, and read the raw stream directly — see
`plugins/muster/mcp/_probe_client.py` (deleted; pattern recorded here).

## Verification run (evidence)

Command:
```
uv run --with mcp --no-project python plugins/muster/mcp/_probe_client.py
```

Output (run twice, identical both times):
```
CLIENT: tool call OK
[probe] pushed channel notification
CLIENT: got channel push
CLIENT: BOTH OK
```
Exit code: `0`.

## Scope note

This probe verifies the JSON-RPC mechanics only. Whether a live Claude Code session
actually *renders* the `<channel source="...">` markup from
`notifications/claude/channel` was already proven in a prior session for the
push-only path, and coexistence with tools is confirmed here at the protocol level.
Full live-Claude rendering with tools present is deferred to Task 8 acceptance.

## MVP acceptance (deterministic two-instance e2e)

**Question:** does a `send` from one Muster server instance actually get delivered by a
*different* instance's own channel relay — i.e. does the full loop (register → send-by-
name → the recipient's own `relay_inbox` tailing its own inbox → pushed as a channel
notification into the recipient's own client) work end-to-end, without a live Claude?

**Answer: yes.** Run twice, identical result both times.

### Setup

Two real `muster_channel.py` server subprocesses on the same bus, each with its own raw
stdio MCP client attached (the `ClientSession`-drops-unknown-notifications gotcha above
still applies, so both clients hand-roll the `initialize`/`initialized` handshake and
read the raw stream directly, same pattern as `_probe_client.py` above):

- **Server A**: env `HERDR_PANE_ID=w9:pA HERDR_WORKSPACE_ID=w9 MUSTER_WELCOME=0` →
  identity resolves to `bus=w9 name=w9:pA` (no real herdr pane matches, so
  `self_identity` falls back to the raw env ids — expected and fine for this probe).
- **Server B**: env `HERDR_PANE_ID=w9:pB HERDR_WORKSPACE_ID=w9 MUSTER_WELCOME=0` →
  `bus=w9 name=w9:pB`. B's client starts a background reader that watches for a
  `notifications/claude/channel` message on B's own stream.

Both servers self-register presence within ~3s (their `register_presence` loop writes
on its first iteration, before the first 30s sleep). Once both are up, A's client calls
`tools/call send {to: "w9:pB", body: "hello from A"}`.

### Result

```
SEND RESULT: {'content': [{'type': 'text', 'text': 'Delivered to w9:pB (msg 1783430298087-0).'}], 'isError': False}
B CAPTURED : {'content': 'w9:pA: hello from A', 'meta': {'ident': 'w9:w9:pB', 'msg_id': '1783430298087-0'}}
PASS
```

B's own client received a `notifications/claude/channel` push — pushed by **B's own
process**, not A's — whose `content` is exactly the summary `busops.send_message`
wrote: `f"{frm}: {body[:80]}"` = `"w9:pA: hello from A"`. That proves the full loop:
A's `send` tool wrote to `muster:inbox:w9:w9:pB`; B's `relay_inbox` background task (an
independent process, tailing its own inbox via `XREAD BLOCK 0`) picked it up and pushed
it into B's own session — exactly what a live two-pane Claude acceptance would exercise,
just without a live Claude driving either side.

Verified twice (`PASS`, exit code `0` both times), then all `muster:*:w9:*` keys were
deleted and the throwaway client script was not committed.

## Live 2-pane acceptance (real Claude sessions)

The deterministic e2e above was confirmed with two REAL Claude Code sessions on
2026-07-07. Two throwaway git repos (`agent-alpha`, `agent-bravo`) were opened as panes in
one herdr workspace (`w6`) and launched with
`claude --mcp-config <muster server> --dangerously-load-development-channels server:muster`.

Observed, end to end, with no keystroke injection between the agents:

- **Identity from git + shared bus:** both sessions self-registered presence —
  `muster:presence:w6:agent-alpha` and `muster:presence:w6:agent-bravo` (name = the git repo, bus
  = the herdr workspace `w6`, `status=online`, TTL ~90). Names are the repos, not pane ids.
- **Welcome:** each session rendered `← muster: Muster bus online — you are <name> on bus w6.
  Tools: roster, send, fetch…` natively.
- **roster:** agent-alpha called the `roster` tool and reported *"Live on bus w6:
  agent-bravo"* — it discovered its peer BY NAME.
- **send + native delivery:** agent-alpha called `send(to="agent-bravo",
  body="LIVE-TEST-PING-42")`; the message landed in `muster:inbox:w6:agent-bravo` and
  agent-bravo's own relay pushed `← muster: agent-bravo …` — actually
  `← muster: agent-alpha: LIVE-TEST-PING-42` — into agent-bravo's session.
- **Bidirectional (emergent):** agent-bravo, on receiving the channel event, autonomously
  called `send` back to agent-alpha (`LIVE-TEST-PONG-42 — … bus w6 ack.`); agent-alpha
  received it as a channel push and concluded *"Round-trip works."*

This is the full goal proven live: two Claude agents sharing a herdr workspace discover each
other by name and hold a two-way conversation over native channels, no keystrokes. The
throwaway repos/panes and the `w6:agent-*` Valkey keys were cleaned up afterward.
