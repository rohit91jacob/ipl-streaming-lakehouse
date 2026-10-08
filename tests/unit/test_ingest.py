import hashlib
import json
from pathlib import Path

import pytest

from ipl_lakehouse.ingest import manifest
from ipl_lakehouse.ingest.cricsheet import IngestError, download, land_archive, run_ingest
from support import ALL_FIXTURES, FINAL_2025, FIXTURES, build_archive


def _ingest(settings, archive, **kw):
    return run_ingest(settings, archive_path=archive, include_register=False, **kw)


def test_first_run_lands_every_match(settings, archive):
    result = _ingest(settings, archive)
    assert result["archive_status"] == "landed"
    assert (result["new"], result["changed"], result["unchanged"]) == (len(ALL_FIXTURES), 0, 0)
    latest = manifest.latest_versions(settings.lake.manifest)
    assert set(latest) == set(ALL_FIXTURES)
    row = latest[FINAL_2025]
    raw = (FIXTURES / f"{FINAL_2025}.json").read_bytes()
    assert row["sha256"] == hashlib.sha256(raw).hexdigest()
    assert (
        row["bronze_path"] == f"bronze/cricsheet/matches/match_id={FINAL_2025}/{row['sha256']}.json"
    )
    assert (settings.data_dir / row["bronze_path"]).read_bytes() == raw
    assert row["season"] == 2025 and row["match_date"] == "2025-06-03"


def test_rerun_with_same_archive_is_a_no_op(settings, archive):
    _ingest(settings, archive)
    again = _ingest(settings, archive)
    assert again["archive_status"] == "unchanged"
    assert manifest.read_all(settings.lake.manifest).num_rows == len(ALL_FIXTURES)


def test_forced_rerun_only_reports_unchanged(settings, archive):
    _ingest(settings, archive)
    forced = _ingest(settings, archive, force=True)
    assert (forced["new"], forced["changed"], forced["unchanged"]) == (0, 0, len(ALL_FIXTURES))


def test_corrected_match_becomes_a_new_immutable_version(settings, archive, tmp_path: Path):
    _ingest(settings, archive)
    old = manifest.latest_versions(settings.lake.manifest)[FINAL_2025]
    corrected = json.loads((FIXTURES / f"{FINAL_2025}.json").read_text())
    corrected["meta"]["revision"] = 2
    archive2 = build_archive(
        tmp_path / "v2.zip",
        [m for m in ALL_FIXTURES if m != FINAL_2025],
        extra={f"{FINAL_2025}.json": json.dumps(corrected).encode()},
    )
    result = _ingest(settings, archive2)
    assert (result["new"], result["changed"], result["unchanged"]) == (0, 1, len(ALL_FIXTURES) - 1)
    new = manifest.latest_versions(settings.lake.manifest)[FINAL_2025]
    assert new["sha256"] != old["sha256"] and new["change_type"] == "changed"
    assert (settings.data_dir / old["bronze_path"]).exists(), "old version must be kept"
    assert (settings.data_dir / new["bronze_path"]).exists()


def test_invalid_files_are_quarantined(settings, tmp_path: Path):
    bad = build_archive(
        tmp_path / "bad.zip", [FINAL_2025], extra={"999.json": b"{not json", "998.json": b"[]"}
    )
    result = _ingest(settings, bad)
    assert sorted(result["quarantined"]) == ["998", "999"]
    assert result["new"] == 1
    quarantined = list((settings.lake.bronze_quarantine_dir / result["run_id"]).iterdir())
    assert sorted(p.name for p in quarantined) == ["998.json", "999.json"]


def test_land_archive_rejects_non_zip(settings, tmp_path: Path):
    junk = tmp_path / "junk.zip"
    junk.write_bytes(b"definitely not a zip")
    with pytest.raises(IngestError):
        land_archive(
            junk,
            settings,
            run_id="r",
            source_url=None,
            source_last_modified=None,
            archive_sha256=None,
        )


class _Response:
    def __init__(self, status: int, body: bytes = b"", headers: dict | None = None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]


class _Session:
    def __init__(self, response: _Response):
        self.response, self.calls = response, []

    def get(self, url, headers=None, stream=None, timeout=None):
        self.calls.append(headers or {})
        return self.response


def test_download_honours_not_modified(tmp_path: Path):
    session = _Session(_Response(304))
    result = download(
        session, "https://x/ipl_json.zip", tmp_path, timeout=5, if_modified_since="Mon"
    )
    assert result.status == "not_modified"
    assert session.calls == [{"If-Modified-Since": "Mon"}]


def test_download_streams_to_a_content_addressed_file(tmp_path: Path):
    body = b"zip-bytes" * 1000
    session = _Session(
        _Response(200, body, {"Content-Length": str(len(body)), "Last-Modified": "Tue"})
    )
    result = download(session, "https://x/ipl_json.zip", tmp_path, timeout=5)
    assert result.status == "downloaded" and result.last_modified == "Tue"
    assert result.path.read_bytes() == body
    assert result.sha256 == hashlib.sha256(body).hexdigest()
    assert result.path.name.endswith(f"{result.sha256[:12]}-ipl_json.zip")


def test_download_detects_truncation_and_http_errors(tmp_path: Path):
    with pytest.raises(IngestError, match="truncated"):
        download(
            _Session(_Response(200, b"abc", {"Content-Length": "10"})),
            "https://x/a.zip",
            tmp_path,
            timeout=5,
        )
    with pytest.raises(IngestError, match="HTTP 503"):
        download(_Session(_Response(503)), "https://x/a.zip", tmp_path, timeout=5)
    assert not list(tmp_path.glob("*.part")), "partial downloads must be cleaned up"
