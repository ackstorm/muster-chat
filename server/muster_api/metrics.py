"""Prometheus metrics, instrumented at the choke points: delivery publish,
rate-limit rejection, SSE stream lifecycle, resolver calls. One uvicorn process
per pod (no --workers), so the default registry is correct as-is."""
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

SSE_CONNECTIONS = Gauge("muster_sse_connections", "SSE streams currently open on this pod")
DELIVERED = Counter("muster_messages_delivered_total", "Deliver events published", ["kind"])
RATE_LIMITED = Counter("muster_rate_limited_total", "Requests refused with 429", ["kind"])
RESOLVER_LATENCY = Histogram("muster_resolver_latency_seconds", "Identity resolver call latency")
AUTH_CACHE = Counter("muster_auth_cache_total", "Auth cache lookups", ["result"])  # hit|miss
# Settles the stale-on-transport-error question with data instead of intuition
# (spec §5.3): does resolver unavailability actually happen, and for how long?
RESOLVER_ERRORS = Counter("muster_resolver_errors_total", "Failed resolver calls",
                          ["reason"])  # timeout|connect|transport|http_5xx|http_4xx


def render() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
