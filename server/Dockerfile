# Digest-pinned: this image is published publicly (ghcr.io/ackstorm/muster-chat),
# so builds must stay reproducible — a moving :3.12-slim tag is not.
FROM python:3.12-slim@sha256:7a8b475003c4fe15a2cd4e55e5cfc2f3560bdc9333d624f24cdd6d4340fd7a17 AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
# Deps come from the lockfile — no hand-maintained requirements list to drift.
RUN uv export --frozen --no-dev --no-emit-project -o requirements.txt \
 && pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim@sha256:7a8b475003c4fe15a2cd4e55e5cfc2f3560bdc9333d624f24cdd6d4340fd7a17
ARG GIT_SHA=unknown
LABEL org.opencontainers.image.title="muster-api" \
      org.opencontainers.image.description="Muster central agent coordination bus" \
      org.opencontainers.image.source="https://github.com/ackstorm/muster-chat" \
      org.opencontainers.image.revision="${GIT_SHA}"
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY --from=builder /app/deps /usr/local/lib/python3.12/site-packages
COPY muster_api/ ./muster_api/
# Numeric non-root uid — must match the Helm chart's runAsUser (runAsNonRoot needs
# a numeric id to verify).
RUN useradd --system --uid 10001 --no-create-home muster
USER 10001
EXPOSE 8765
# ponytail: pip install --target doesn't uvicorn console script, so invoke
# module instead relying on $PATH.
CMD ["python", "-m", "uvicorn", "muster_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8765"]
