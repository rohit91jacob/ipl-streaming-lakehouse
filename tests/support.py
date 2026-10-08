"""Shared test helpers and the catalogue of real Cricsheet fixture matches."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from ipl_lakehouse.config import Settings

FIXTURES = Path(__file__).parent / "fixtures" / "cricsheet"

# Real Cricsheet matches chosen for their edge cases.
FINAL_2025 = "1473511"  # RCB 190/9 beat PBKS 184/7 by 6 runs (impact players)
DOUBLE_SUPER_OVER = "1216517"  # MI v KXIP 2020: tie, two super overs, KXIP win
DLS_FINAL_2023 = "1370353"  # CSK chase a revised 171 in 15 overs (D/L)
NO_RESULT_2019 = "1178424"  # RCB v RR, 5-over game washed out
ABSENT_HURT_2019 = "1175358"  # MI 176 "all out" for 9: Bumrah absent hurt
VOID_2025 = "1473495"  # PBKS v DC stopped at 10.1 overs and voided (replayed later)

ALL_FIXTURES = (
    FINAL_2025,
    DOUBLE_SUPER_OVER,
    DLS_FINAL_2023,
    NO_RESULT_2019,
    ABSENT_HURT_2019,
    VOID_2025,
)


def load_match(match_id: str) -> dict:
    return json.loads((FIXTURES / f"{match_id}.json").read_text(encoding="utf-8"))


def make_settings(root: Path, **overrides) -> Settings:
    base = {
        "data_dir": root / "data",
        "checkpoint_dir": root / "data" / "_checkpoints",
        "spark_master": "local[2]",
        "spark_shuffle_partitions": 2,
        "spark_driver_memory": "1g",
        "log_format": "text",
    }
    base.update(overrides)
    return Settings(**base)


def build_archive(
    path: Path, match_ids=ALL_FIXTURES, extra: dict[str, bytes] | None = None
) -> Path:
    """A miniature ipl_json.zip built from the fixtures (same layout as Cricsheet's)."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.txt", "fixture archive\n")
        for match_id in match_ids:
            zf.write(FIXTURES / f"{match_id}.json", arcname=f"{match_id}.json")
        for name, data in (extra or {}).items():
            zf.writestr(name, data)
    return path
