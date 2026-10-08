import pytest

from ipl_lakehouse import __version__
from ipl_lakehouse.cli import EXIT_OK, EXIT_USAGE, build_parser, main


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_every_command_parses():
    parser = build_parser()
    for argv in (
        ["ingest", "--force"],
        ["silver", "--full-refresh"],
        ["gold"],
        ["dq", "--layer", "gold"],
        ["batch", "--skip-ingest"],
        ["topics"],
        ["produce", "--season", "2025", "--speedup", "120"],
        ["stream", "--available-now", "--queries", "ingest,scorecard"],
        ["verify-stream", "--match-id", "1473511", "--against", "gold"],
        ["sql", "select 1"],
        ["stream-reset", "--yes"],
    ):
        assert parser.parse_args(argv).command == argv[0]


def test_queries_are_split():
    args = build_parser().parse_args(["stream", "--queries", "ingest, scorecard"])
    assert args.queries == ["ingest", "scorecard"]


def test_config_errors_exit_with_usage_code(monkeypatch, capsys):
    monkeypatch.setenv("IPL_SPARK_SHUFFLE_PARTITIONS", "lots")
    assert main(["sql", "--list"]) == EXIT_USAGE
    assert "IPL_SPARK_SHUFFLE_PARTITIONS" in capsys.readouterr().err


def test_stream_reset_requires_confirmation(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("IPL_DATA_DIR", str(tmp_path))
    events = tmp_path / "bronze" / "stream_events"
    events.mkdir(parents=True)
    batch_table = tmp_path / "gold" / "points_table"
    batch_table.mkdir(parents=True)
    assert main(["stream-reset"]) == EXIT_USAGE
    assert events.exists()
    assert main(["stream-reset", "--yes"]) == EXIT_OK
    assert not events.exists()
    assert batch_table.exists(), "batch tables must never be touched"


def test_sql_list_on_empty_lake(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("IPL_DATA_DIR", str(tmp_path))
    assert main(["sql", "--list"]) == EXIT_OK
    assert capsys.readouterr().out == ""
