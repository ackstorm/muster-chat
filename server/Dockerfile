FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project -o requirements.txt \
 && pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY --from=builder /app/deps /usr/local/lib/python3.12/site-packages
COPY muster_api/ ./muster_api/
EXPOSE 8765
# ponytail: pip install --target doesn't generate the uvicorn console script, so invoke
# it as a module instead of relying on $PATH.
CMD ["python", "-m", "uvicorn", "muster_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8765"]
