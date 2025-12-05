# Gateway Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    httpx \
    pydantic \
    python-dotenv \
    temporalio \
    structlog

# Copy application code
COPY packages/ /app/packages/
COPY gateway/ /app/gateway/

# Set Python path
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Run gateway
CMD ["python", "-m", "uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
