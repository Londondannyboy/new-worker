# Content Worker Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    temporalio \
    httpx \
    pydantic \
    python-dotenv \
    structlog \
    psycopg2-binary

# Copy packages from repo root
COPY ../../packages/ /app/packages/
COPY ../../apps/ /app/apps/

# Set Python path
ENV PYTHONPATH=/app

# Run content worker
CMD ["python", "/app/apps/content-worker/worker.py"]
