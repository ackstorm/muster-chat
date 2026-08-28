# muster-api

Central Muster coordination bus. Spec: `../docs/superpowers/specs/2026-08-27-muster-v1-central-bus-spec-v2.md`.

The target Valkey MUST run `--appendonly yes --appendfsync everysec` (spec §16) — without AOF, durable unicast is void.

## Run locally

    docker compose up -d          # from the repo root: Valkey + muster-api on 127.0.0.1:8765
    # dev auth: MUSTER_STATIC_KEYS maps "dev-key" -> user "dev", group "local"

## Tests

    docker compose up -d valkey
    cd server && uv run pytest -v      # uses Valkey DB 3 and flushes it

## API in 30 seconds

    POST /v1/rpc     {"op": "roster"|"chat"|"fetch"|"announce", "args": {...}}
    GET  /v1/stream  SSE: deliver events (chat envelopes, announces, unread nudge) + pings
    Headers on everything: x-muster-api-key, x-muster-agent: host/runtime/project/session
    (x-muster-meta JSON on the stream connect only)
