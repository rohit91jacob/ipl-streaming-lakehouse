"""A lakehouse built once from the real fixture matches, shared by read-only Spark tests."""

from __future__ import annotations

import pytest

from ipl_lakehouse.batch.gold import build_gold
from ipl_lakehouse.batch.silver import build_silver
from ipl_lakehouse.config import Settings
from ipl_lakehouse.ingest.cricsheet import run_ingest
from support import build_archive, make_settings


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory) -> Settings:
    """ingest -> silver -> gold over the six fixture matches; shared by read-only tests."""
    root = tmp_path_factory.mktemp("lake")
    settings = make_settings(root)
    run_ingest(settings, archive_path=build_archive(root / "ipl_json.zip"), include_register=False)
    build_silver(spark, settings)
    build_gold(spark, settings)
    return settings


@pytest.fixture(scope="session")
def silver(spark, lake):
    def read(table: str):
        return spark.read.format("delta").load(str(lake.lake.silver(table)))

    return read


@pytest.fixture(scope="session")
def gold(spark, lake):
    def read(table: str):
        return spark.read.format("delta").load(str(lake.lake.gold(table)))

    return read
