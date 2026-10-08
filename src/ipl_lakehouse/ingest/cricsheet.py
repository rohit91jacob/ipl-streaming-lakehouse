"""Download the Cricsheet IPL archive and land each match file in the immutable bronze zone.

Idempotency and incrementality:

* the archive is fetched with ``If-Modified-Since``; a 304 or an archive whose sha256 equals
  the last one ends the run without touching bronze;
* every match file is stored content-addressed (``match_id=<id>/<sha256>.json``) and never
  overwritten, so a corrected file from Cricsheet becomes a *new version* next to the old one;
* the manifest gets one row per new/changed file in a single Delta commit, written after the
  files, so a crash mid-run is repaired by simply running again;
* files that are not valid match JSON are copied to ``_quarantine/<run_id>/`` and skipped.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ipl_lakehouse.config import Settings
from ipl_lakehouse.cricsheet import match_id_from_filename, season_of, validate_match
from ipl_lakehouse.ingest import manifest
from ipl_lakehouse.logs import get_logger

log = get_logger(__name__)

LANDING_RETENTION = 3


class IngestError(RuntimeError):
    pass


@dataclass
class Download:
    status: str  # "downloaded" | "not_modified"
    path: Path | None = None
    sha256: str | None = None
    size_bytes: int = 0
    last_modified: str | None = None
    etag: str | None = None


@dataclass
class LandingSummary:
    run_id: str
    archive_entries: int = 0
    new: int = 0
    changed: int = 0
    unchanged: int = 0
    quarantined: list[str] = field(default_factory=list)


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def http_session(settings: Settings) -> requests.Session:
    retry = Retry(
        total=settings.http_retries,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = settings.user_agent
    return session


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def download(
    session: requests.Session,
    url: str,
    dest_dir: Path,
    *,
    timeout: float,
    if_modified_since: str | None = None,
) -> Download:
    headers = {"If-Modified-Since": if_modified_since} if if_modified_since else {}
    dest_dir.mkdir(parents=True, exist_ok=True)
    with session.get(url, headers=headers, stream=True, timeout=timeout) as resp:
        if resp.status_code == 304:
            return Download("not_modified", last_modified=if_modified_since)
        if resp.status_code != 200:
            raise IngestError(f"GET {url} returned HTTP {resp.status_code}")
        digest, size = hashlib.sha256(), 0
        fd, tmp = tempfile.mkstemp(dir=dest_dir, suffix=".part")
        try:
            with os.fdopen(fd, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            expected = resp.headers.get("Content-Length")
            if expected and not resp.headers.get("Content-Encoding") and int(expected) != size:
                raise IngestError(f"truncated download from {url}: {size} of {expected} bytes")
            sha = digest.hexdigest()
            name = Path(urlparse(url).path).name or "download"
            final = dest_dir / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{sha[:12]}-{name}"
            Path(tmp).replace(final)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
    return Download(
        "downloaded",
        path=final,
        sha256=sha,
        size_bytes=size,
        last_modified=resp.headers.get("Last-Modified"),
        etag=resp.headers.get("ETag"),
    )


def load_state(settings: Settings) -> dict:
    path = settings.lake.ingest_state_file
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def save_state(settings: Settings, state: dict) -> None:
    atomic_write_bytes(
        settings.lake.ingest_state_file,
        json.dumps(state, indent=2, sort_keys=True, default=str).encode("utf-8"),
    )


def _relative(settings: Settings, path: Path) -> str:
    return path.relative_to(settings.data_dir).as_posix()


def land_archive(
    zip_path: Path,
    settings: Settings,
    *,
    run_id: str,
    source_url: str | None,
    source_last_modified: str | None,
    archive_sha256: str | None,
) -> LandingSummary:
    lake = settings.lake
    summary = LandingSummary(run_id=run_id)
    latest = manifest.latest_versions(lake.manifest)
    records: list[manifest.ManifestRecord] = []
    now = datetime.now(UTC)
    try:
        archive = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as exc:
        raise IngestError(f"{zip_path} is not a valid zip archive") from exc
    with archive:
        corrupt = archive.testzip()
        if corrupt is not None:
            raise IngestError(f"corrupt member {corrupt!r} in {zip_path}")
        for member in archive.infolist():
            match_id = match_id_from_filename(member.filename)
            if match_id is None:
                continue  # README.txt, LICENSE.txt
            summary.archive_entries += 1
            raw = archive.read(member)
            sha = hashlib.sha256(raw).hexdigest()
            previous = latest.get(match_id)
            if previous is not None and previous["sha256"] == sha:
                summary.unchanged += 1
                continue
            try:
                doc = json.loads(raw)
                problems = validate_match(doc)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                doc, problems = None, [f"invalid JSON: {exc}"]
            if problems:
                atomic_write_bytes(
                    lake.bronze_quarantine_dir / run_id / Path(member.filename).name, raw
                )
                summary.quarantined.append(match_id)
                log.warning(
                    "quarantined match file",
                    extra={"match_id": match_id, "problems": problems, "run_id": run_id},
                )
                continue
            target = lake.bronze_matches_dir / f"match_id={match_id}" / f"{sha}.json"
            if not target.exists():
                atomic_write_bytes(target, raw)
            change = "changed" if previous is not None else "new"
            if change == "new":
                summary.new += 1
            else:
                summary.changed += 1
            records.append(
                manifest.ManifestRecord(
                    match_id=match_id,
                    sha256=sha,
                    size_bytes=len(raw),
                    bronze_path=_relative(settings, target),
                    data_version=str(doc.get("meta", {}).get("data_version") or "") or None,
                    season=season_of(doc),
                    match_date=str(doc["info"]["dates"][0]),
                    change_type=change,
                    source_url=source_url,
                    source_archive_sha256=archive_sha256,
                    source_last_modified=source_last_modified,
                    ingest_run_id=run_id,
                    ingested_at=now,
                )
            )
    manifest.append_records(lake.manifest, records)
    return summary


def _prune_landing(directory: Path, keep: int = LANDING_RETENTION) -> None:
    archives = sorted(directory.glob("*.zip"))
    for stale in archives[:-keep] if len(archives) > keep else []:
        stale.unlink(missing_ok=True)


def ingest_register(
    settings: Settings, session: requests.Session, state: dict, force: bool
) -> dict:
    """Land Cricsheet's people register (player identifiers) as a content-addressed CSV."""
    lake = settings.lake
    previous = state.get("register", {})
    result = download(
        session,
        settings.cricsheet_register_url,
        lake.landing_dir / "register",
        timeout=settings.http_timeout_s,
        if_modified_since=None if force else previous.get("last_modified"),
    )
    if result.status == "not_modified" or result.sha256 == previous.get("sha256"):
        if result.path:
            result.path.unlink(missing_ok=True)
        return previous
    target = lake.bronze_register_dir / f"{result.sha256}.csv"
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        result.path.replace(target)
    else:
        result.path.unlink(missing_ok=True)
    return {
        "url": settings.cricsheet_register_url,
        "sha256": result.sha256,
        "last_modified": result.last_modified,
        "bronze_path": _relative(settings, target),
        "fetched_at": datetime.now(UTC).isoformat(),
    }


def run_ingest(
    settings: Settings,
    *,
    force: bool = False,
    archive_path: Path | None = None,
    include_register: bool = True,
) -> dict:
    """Fetch (or take ``archive_path``), land new/changed matches, record the run in state."""
    run_id = new_run_id()
    lake = settings.lake
    state = load_state(settings)
    previous = state.get("archive", {})
    log.info("ingest started", extra={"run_id": run_id, "force": force})

    session = http_session(settings)
    if archive_path is not None:
        sha = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        fetched = Download("downloaded", archive_path, sha, archive_path.stat().st_size)
        source_url = archive_path.resolve().as_uri()
    else:
        # Without a manifest there is nothing to be incremental against: always download.
        conditional = not force and manifest.manifest_exists(lake.manifest)
        fetched = download(
            session,
            settings.cricsheet_url,
            lake.landing_dir / "ipl_json",
            timeout=settings.http_timeout_s,
            if_modified_since=previous.get("last_modified") if conditional else None,
        )
        source_url = settings.cricsheet_url

    summary: LandingSummary | None = None
    if fetched.status == "not_modified":
        log.info("archive not modified since last run", extra={"run_id": run_id})
    elif (
        not force
        and fetched.sha256 == previous.get("sha256")
        and manifest.manifest_exists(lake.manifest)
    ):
        log.info(
            "archive unchanged (same sha256)", extra={"run_id": run_id, "sha256": fetched.sha256}
        )
    else:
        summary = land_archive(
            fetched.path,
            settings,
            run_id=run_id,
            source_url=source_url,
            source_last_modified=fetched.last_modified,
            archive_sha256=fetched.sha256,
        )
        state["archive"] = {
            "url": source_url,
            "sha256": fetched.sha256,
            "size_bytes": fetched.size_bytes,
            "last_modified": fetched.last_modified,
            "etag": fetched.etag,
            "fetched_at": datetime.now(UTC).isoformat(),
        }
    if archive_path is None:
        _prune_landing(lake.landing_dir / "ipl_json")

    if include_register:
        state["register"] = ingest_register(settings, session, state, force)

    if summary is not None:
        status = "landed"
    else:
        status = "not_modified" if fetched.status == "not_modified" else "unchanged"
    result = {
        "run_id": run_id,
        "archive_status": status,
        **({k: v for k, v in asdict(summary).items() if k != "run_id"} if summary else {}),
    }
    state["last_run"] = {**result, "finished_at": datetime.now(UTC).isoformat()}
    save_state(settings, state)
    log.info("ingest finished", extra=result)
    return result
