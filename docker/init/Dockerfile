# One-shot bootstrap container: guards against a stale run, anchors the simulated
# clock, creates topics, registers schemas. Exits 0 and everything else starts.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src:/app/scripts

WORKDIR /app

# librdkafka needs a compiler toolchain only if a wheel is unavailable; the slim
# image plus the manylinux wheel avoids that, so we install nothing extra.
RUN pip install --no-cache-dir \
      "confluent-kafka[avro,schemaregistry]>=2.4" \
      "pydantic>=2.7" "pydantic-settings>=2.3" \
      "structlog>=24.1" "fastavro>=1.9" \
      "boto3>=1.34" "psycopg[binary]>=3.1" "redis>=5.0"

COPY src/ /app/src/
COPY scripts/ /app/scripts/

CMD ["python", "/app/scripts/init_stack.py"]
