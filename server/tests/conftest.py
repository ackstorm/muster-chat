"""Shared fixtures. Requires the repo-root Valkey: `docker compose up -d`.
Tests use DB 3 and flush it — production uses DB 1."""
import pytest
import redis.asyncio as redis

TEST_VALKEY = "redis://localhost:6379/3"


@pytest.fixture
async def r():
    client = redis.from_url(TEST_VALKEY, decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()
