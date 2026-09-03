# The serving layer. Deliberately the smallest image in the stack: it holds no Spark,
# no Kafka client and no simulation code, because it is the only container an outside
# client talks to and everything it does not need is attack surface and RAM.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --no-cache-dir \
      "fastapi>=0.111" "uvicorn[standard]>=0.30" \
      "redis>=5.0" "psycopg[binary]>=3.1" \
      "pydantic>=2.7" "pydantic-settings>=2.3" \
      "structlog>=24.1" "prometheus-client>=0.20" \
      "prometheus-fastapi-instrumentator>=7.0"

COPY src/ /app/src/

# curl for the compose healthcheck; the slim image has none.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

EXPOSE 8000

# One worker on purpose. Several would each keep their own Redis and Postgres pools
# on a 15.6 GB host for no benefit at this request rate, and the /metrics endpoint
# would then report one worker's view of the world as if it were the whole API.
CMD ["uvicorn", "fleet.serving.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
