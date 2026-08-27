# Channel notification deferral (spec v2 §13) — decision: no hold logic in v1

Spec §13 allows the runtime adapter to defer surfacing while the runtime is busy,
"if empirical validation shows the runtime does not already handle this".

## Observation (Claude Code, v0 muster in daily use, 2026-08)

- `notifications/claude/channel` events pushed by the stdio MCP server while the agent
  is MID-TURN are not lost and do not corrupt the turn: Claude Code queues them and
  surfaces the `<channel>` block at a safe point (observed: start of the next model
  turn / between tool batches), matching the documented behavior of native
  cross-session messaging ("messages surface between tool calls").
- Weeks of v0 operation (presence notices + chat envelopes arriving during active work)
  produced zero mid-tool-call injections and zero lost handshakes.

## Decision

Per §13: the runtime already defers to safe points ⇒ **v1 ships no hold logic anywhere**
(neither server- nor shim-side). `important: true` therefore has no deferral to bypass in
the Claude shim; its only effect is the ❗ mark on the envelope.

Re-verify if: Claude Code changes channel semantics, or a runtime is added whose
injection is truly immediate (OpenCode's `noReply:false` wake starts a NEW turn — that is
wake semantics, not mid-turn injection, and is the adapter's deliberate choice).
