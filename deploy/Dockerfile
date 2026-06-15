# One image for every Python service: ingestion, dashboard, and the dbt batch.
# (Spark gets its own image because it needs the JVM.) Build context is the repo
# root: docker compose -f deploy/docker-compose.vm.yml build
FROM python:3.11-slim

WORKDIR /app

# System deps kept minimal; curl only for container healthchecks.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Pin the runtime deps explicitly so we DON'T pull pyspark into this image.
RUN pip install --no-cache-dir \
    "kafka-python>=2.0.2" \
    "websockets>=12.0" \
    "requests>=2.31" \
    "duckdb>=1.0.0" \
    "pandas>=2.0" \
    "pyarrow>=15.0" \
    "dbt-core>=1.8" \
    "dbt-duckdb>=1.8" \
    "streamlit>=1.36"

COPY . /app

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
