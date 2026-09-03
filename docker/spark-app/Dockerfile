# Spark application image.
#
# Built FROM python:3.11-slim rather than from a Spark base image, because both
# candidate bases were unusable: bitnami/spark:3.5.1 has been withdrawn from Docker
# Hub, and apache/spark:3.5.1-python3 ships Python 3.8 - too old for this codebase,
# which uses StrEnum, datetime.UTC, match statements and dataclass slots.
#
# pip-installed pyspark bundles the full Spark distribution, so this gives an
# identical engine with a Python version we control. It also guarantees the driver
# and worker Python versions match, which is its own class of confusing failure
# ("Python in worker has different version than that in driver").
#
# Spark runs in local[4] mode by default (ADR-006).
#
# The jar versions are the reason this file exists. Spark 3.5.1 ships Hadoop 3.3.4
# client libraries, so hadoop-aws MUST be 3.3.4 and aws-java-sdk-bundle MUST be the
# 1.12.262 it was built against. A mismatch surfaces as ClassNotFoundException or
# NoSuchMethodError at the first s3a:// write - the failure the plan budgets 1-2
# hours for. Pinning them here moves that failure to build time, once.
# Pinned to bookworm (Debian 12), NOT the floating -slim tag: trixie (Debian 13)
# ships only openjdk-21 and openjdk-25, and Spark 3.5 supports Java 8/11/17 only.
# An unpinned base would have started failing the day Docker Hub moved the tag.
FROM python:3.11-slim-bookworm

ENV PYSPARK_VERSION=3.5.1 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark 3.5 supports Java 8/11/17. Not 21 - the host's JVM is irrelevant here,
# which is precisely why Spark runs in a container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

# Drop the S3A jars into pyspark's own jars directory so no --packages resolution
# (and no network round-trip) is needed at submit time.
RUN SPARK_JARS="$(python -c 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
 && curl -fsSL -o "${SPARK_JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSL -o "${SPARK_JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
 && ls -1 "${SPARK_JARS}" | grep -E 'hadoop-aws|aws-java-sdk'

# Kafka source/sink and Avro codec. Downloaded explicitly rather than resolved with
# --packages at submit time: --packages needs network on every job start, adds ~30s
# of Ivy resolution, and turns a transient network blip into a failed streaming job.
#
# Versions are not free choices. spark-sql-kafka and spark-avro must match Spark
# exactly (3.5.1) and the Scala binary version of the bundled jars (2.12).
# kafka-clients must be the version Spark 3.5.1 was built against.
RUN SPARK_JARS="$(python -c 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
 && M2=https://repo1.maven.org/maven2 \
 && for artifact in \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
      "org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar" \
      "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do curl -fsSL -o "${SPARK_JARS}/$(basename $artifact)" "${M2}/${artifact}"; done \
 && ls -1 "${SPARK_JARS}" | grep -E 'kafka|avro|commons-pool'

# pandas + pyarrow are required by applyInPandasWithState, which is PySpark's ONLY
# stateful streaming API (flatMapGroupsWithState is Scala/Java only). pyarrow is
# also what makes any Python<->JVM column transfer fast, so it earns its ~120 MB.
RUN pip install --no-cache-dir "pandas>=2.2" "pyarrow>=16.0"

RUN pip install --no-cache-dir \
      "pydantic>=2.7" "pydantic-settings>=2.3" "structlog>=24.1" \
      "prometheus-client>=0.20" "redis>=5.0" "psycopg[binary]>=3.1" \
      "confluent-kafka[avro,schemaregistry]>=2.4"

WORKDIR /app
COPY src/ /app/src/

CMD ["python", "-c", "import pyspark; print(f'pyspark {pyspark.__version__} ready')"]
