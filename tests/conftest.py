from __future__ import annotations

from pathlib import Path

import pytest

from ipl_lakehouse.config import Settings
from ipl_lakehouse.spark import build_spark
from support import build_archive, make_settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    return build_archive(tmp_path / "ipl_json.zip")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One local Spark (Delta + Kafka connector) for the whole run; started lazily."""
    settings = make_settings(tmp_path_factory.mktemp("spark-session"))
    session = build_spark(settings, "ipl-tests", kafka=True)
    yield session
    session.stop()
