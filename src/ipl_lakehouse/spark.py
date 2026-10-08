"""SparkSession factory with Delta Lake (and optionally the Kafka connector) configured.

Jars come from Maven Central via ``spark.jars.packages`` by default. Container images bake them
in with ``python -m ipl_lakehouse.spark fetch-jars <dir>`` and set ``IPL_SPARK_JARS_DIR`` so
runtime needs no network access.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from importlib import metadata, resources
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from ipl_lakehouse.config import Settings

SCALA_BINARY = "2.13"


def spark_minor_version() -> str:
    return ".".join(pyspark.__version__.split(".")[:2])


def maven_packages(kafka: bool = True) -> list[str]:
    # delta-spark >= 4.x publishes one artifact per Spark minor (delta-spark_4.1_2.13, ...).
    delta = metadata.version("delta-spark")
    packages = [f"io.delta:delta-spark_{spark_minor_version()}_{SCALA_BINARY}:{delta}"]
    if kafka:
        packages.append(
            f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{pyspark.__version__}"
        )
    return packages


def _log4j_config() -> str:
    return str(resources.files("ipl_lakehouse").joinpath("log4j2.properties"))


def spark_conf(settings: Settings, *, kafka: bool) -> dict[str, str]:
    conf = {
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.shuffle.partitions": str(settings.spark_shuffle_partitions),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.adaptive.enabled": "true",
        "spark.ui.enabled": str(settings.spark_ui_enabled).lower(),
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.streaming.stateStore.providerClass": (
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
        ),
        "spark.driver.extraJavaOptions": f"-Dlog4j2.configurationFile=file:{_log4j_config()}",
        "spark.sql.streaming.metricsEnabled": "true",
        # Delta replays its transaction log with 50 tasks by default; these tables are small, so
        # that fixed overhead dominated every read, MERGE and commit.
        "spark.databricks.delta.snapshotPartitions": str(settings.spark_shuffle_partitions),
    }
    if settings.spark_jars_dir:
        jars = sorted(str(p) for p in Path(settings.spark_jars_dir).glob("*.jar"))
        if not jars:
            raise RuntimeError(f"IPL_SPARK_JARS_DIR={settings.spark_jars_dir} has no jars")
        conf["spark.jars"] = ",".join(jars)
    else:
        conf["spark.jars.packages"] = ",".join(maven_packages(kafka=kafka))
    return conf


def build_spark(settings: Settings, app_name: str, *, kafka: bool = False) -> SparkSession:
    # Workers must run the interpreter that owns this environment (venvs, containers).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = SparkSession.builder.appName(app_name).master(settings.spark_master)
    for key, value in spark_conf(settings, kafka=kafka).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    return spark


def fetch_jars(destination: Path) -> list[Path]:
    """Resolve Delta + Kafka jars (with transitive deps) once and copy them to ``destination``."""
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as ivy:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("fetch-jars")
            .config("spark.jars.packages", ",".join(maven_packages(kafka=True)))
            .config("spark.jars.ivy", ivy)
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.stop()
        copied = []
        for jar in sorted(Path(ivy, "jars").glob("*.jar")):
            copied.append(Path(shutil.copy2(jar, destination / jar.name)))
    return copied


if __name__ == "__main__":  # pragma: no cover - used by the Dockerfile
    if len(sys.argv) != 3 or sys.argv[1] != "fetch-jars":
        raise SystemExit("usage: python -m ipl_lakehouse.spark fetch-jars <dir>")
    for path in fetch_jars(Path(sys.argv[2])):
        print(path)
