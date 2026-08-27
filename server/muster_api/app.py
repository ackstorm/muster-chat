"""FastAPI wiring. Session-less: every request re-derives (identity, address) from
headers (spec §5). The stream route is added in Task 9, the reaper in Task 10."""
import os

import redis.asyncio as redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import config as config_mod, ops
from .auth import AuthError, Authenticator
from .identity import Address, AddressError, parse_agent_header


async def caller(request: Request) -> tuple:
    """(Identity, Address) for this request. user is stamped from the resolved key."""
    ident = await request.app.state.auth.resolve(request.headers.get("x-muster-api-key", ""))
    addr = parse_agent_header(ident.user_id, request.headers.get("x-muster-agent", ""))
    return ident, addr


def create_app(cfg: config_mod.Config | None = None) -> FastAPI:
    cfg = cfg or config_mod.load(os.environ)

    import asyncio
    from contextlib import asynccontextmanager

    from . import reaper

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(reaper.run_forever(app.state.redis))
        yield
        task.cancel()

    app = FastAPI(title="muster-api", lifespan=lifespan)
    app.state.cfg = cfg
    app.state.redis = redis.from_url(cfg.valkey_url, decode_responses=True)
    app.state.auth = Authenticator(cfg)

    @app.exception_handler(AuthError)
    async def _auth_err(request, exc: AuthError):
        return JSONResponse({"code": exc.code, "message": exc.message}, status_code=exc.status)

    @app.exception_handler(AddressError)
    async def _addr_err(request, exc: AddressError):
        return JSONResponse({"code": "bad_agent_header", "message": str(exc)}, status_code=400)

    @app.exception_handler(ops.OpError)
    async def _op_err(request, exc: ops.OpError):
        return JSONResponse(exc.payload, status_code=exc.status)

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.post("/v1/rpc")
    async def rpc(request: Request):
        ident, addr = await caller(request)
        payload = await request.json()
        return await ops.dispatch(request.app.state.redis, cfg, ident, addr,
                                  payload.get("op", ""), payload.get("args") or {})

    from . import stream as stream_mod

    @app.get("/v1/stream")
    async def stream_route(request: Request):
        ident, addr = await caller(request)
        return await stream_mod.stream_endpoint(request, ident, addr)

    return app
