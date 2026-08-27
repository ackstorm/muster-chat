FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install --target=/app/deps httpx

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/deps /usr/local/lib/python3.12/site-packages
COPY gateway/telegram/gateway.py .
ENV MUSTER_PAIRING_FILE=/data/pairings.json
VOLUME /data
CMD ["python", "gateway.py"]
