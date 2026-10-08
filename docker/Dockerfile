# syntax=docker/dockerfile:1.7
# One image for every role (batch, stream, producer, dashboard); the command picks the role.
FROM python:3.12-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

# Spark 4.1 needs a JRE (17 or 21). tini reaps the JVM and forwards SIGTERM for clean shutdowns.
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless tini procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv

WORKDIR /app
# Dependencies first so code changes do not invalidate the (large) pyspark layer.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra dashboard
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra dashboard

# Bake Delta + Kafka connector jars (and their transitive deps) into the image so containers
# never resolve from Maven Central at runtime.
RUN python -m ipl_lakehouse.spark fetch-jars /opt/spark-jars && ls /opt/spark-jars | wc -l
ENV IPL_SPARK_JARS_DIR=/opt/spark-jars \
    IPL_DATA_DIR=/data \
    IPL_CHECKPOINT_DIR=/data/_checkpoints

RUN useradd --create-home --uid 10001 ipl && mkdir -p /data && chown ipl:ipl /data
USER ipl
WORKDIR /home/ipl
VOLUME ["/data"]

ENTRYPOINT ["tini", "--", "ipl"]
CMD ["--help"]
