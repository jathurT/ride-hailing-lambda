# Airflow, for orchestration only.
#
# Note what is NOT installed: pyspark. The batch jobs need a Spark driver, and
# putting one in this image would mean a ~300 MB layer plus a second 1.5 GB driver
# process on a host that has 11 GB for all of Docker.
#
# Instead the BashOperators `docker exec` into the existing spark-app container,
# which already has pyspark and is where those jobs were designed to run. That needs
# the docker socket mounted into the scheduler - acceptable for a local demo stack
# and called out here rather than hidden in the compose file. In a real deployment
# this would be a KubernetesPodOperator or a Spark cluster submit endpoint, and the
# DAG would not change shape.
FROM apache/airflow:2.10.5-python3.11

USER root

# The docker CLI only - not the daemon. It talks to the host's socket.
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io curl \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Only what the DAG's own tasks import. The heavy lifting happens in other containers.
RUN pip install --no-cache-dir \
      "psycopg[binary]>=3.1" "redis>=5.0" "boto3>=1.34" \
      "pydantic>=2.7" "pydantic-settings>=2.3" \
      "structlog>=24.1" "prometheus-client>=0.20"

ENV PYTHONPATH=/opt/airflow/src
