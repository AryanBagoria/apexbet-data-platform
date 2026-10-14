# Multi-purpose image for the ApexBet platform (API, consumers, CLI, jobs).
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    APEXBET_ENV=local

WORKDIR /app

# System deps kept minimal; librdkafka is needed for confluent-kafka.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates build-essential librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first for better layer caching.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install -e ".[spark]" \
    && apt-get purge -y build-essential && apt-get autoremove -y || true

# Non-root user.
RUN useradd --create-home --uid 10001 apexbet && chown -R apexbet:apexbet /app
USER apexbet

ENV PATH="/home/apexbet/.local/bin:${PATH}" \
    APEXBET_LAKE_ROOT=/data/lake \
    APEXBET_DUCKDB_PATH=/data/lake/analytics.duckdb

EXPOSE 8000 9101
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "apexbet.ingestion.app:app", "--host", "0.0.0.0", "--port", "8000"]
