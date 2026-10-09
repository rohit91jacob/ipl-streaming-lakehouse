"""Static results site built from the gold tables (``ipl report --out site/``).

Plain HTML + one stylesheet, no JavaScript and no external assets, so it can be served from
GitHub Pages or opened from disk. Reads the lake through ``query.run_sql`` (delta-rs), so it
needs no JVM.

Pages:
* ``index.html``: the latest season (points table, Orange/Purple Cap top 10, recent results),
  all-time leaders and champions by season.
* ``seasons/<year>.html``: points table, caps and result of the final for every season.
* ``summary.json``: machine-readable freshness metadata (as-of date, counts, generated time).
"""

from __future__ import annotations

import html
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ipl_lakehouse.config import Settings
from ipl_lakehouse.query import run_sql

REPO_URL = "https://github.com/rohit91jacob/ipl-streaming-lakehouse"
CRICSHEET_URL = "https://cricsheet.org/"
ODC_BY_URL = "https://opendatacommons.org/licenses/by/1-0/"
TOP_N = 10

Row = dict[str, Any]
Query = Callable[[str], list[Row]]


# --------------------------------------------------------------------------- formatting
def _text(value: object) -> str:
    return "&ndash;" if value is None else html.escape(str(value))


def _decimal(value: object, places: int = 2) -> str:
    return "&ndash;" if value is None else f"{float(value):.{places}f}"


def _nrr(value: object) -> str:
    return "&ndash;" if value is None else f"{float(value):+.3f}"


def _teams(value: object) -> str:
    if value is None:
        return "&ndash;"
    if isinstance(value, str):
        return html.escape(value)
    return html.escape(", ".join(str(t) for t in value))


def _ratio(numerator: float, denominator: float, scale: float = 1.0) -> float | None:
    return None if not denominator else numerator * scale / denominator


@dataclass(frozen=True)
class Column:
    header: str
    render: Callable[[Row], str]
    numeric: bool = False
    title: str | None = None  # expanded header for screen readers (abbreviations)


def _table(caption: str, columns: Sequence[Column], rows: Iterable[Row], empty: str) -> str:
    rows = list(rows)
    if not rows:
        return f'<p class="empty">{html.escape(empty)}</p>'
    head = "".join(
        f'<th scope="col"{" class=num" if c.numeric else ""}>'
        + (
            f"<abbr title={_attr(c.title)}>{html.escape(c.header)}</abbr>"
            if c.title
            else html.escape(c.header)
        )
        + "</th>"
        for c in columns
    )
    body = "".join(
        "<tr"
        + (" class=highlight" if row.get("_highlight") else "")
        + ">"
        + "".join(f"<td{' class=num' if c.numeric else ''}>{c.render(row)}</td>" for c in columns)
        + "</tr>"
        for row in rows
    )
    return (
        f'<div class="table-wrap"><table><caption>{html.escape(caption)}</caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


def _attr(value: str) -> str:
    return '"' + html.escape(value, quote=True) + '"'


# --------------------------------------------------------------------------- data access
def _sql_query(settings: Settings) -> Query:
    return lambda sql: run_sql(settings, sql).to_pylist()


def _points(query: Query, season: int) -> list[Row]:
    rows = query(
        "SELECT position, team, played, won, lost, tied, no_result, points, net_run_rate "
        f"FROM points_table WHERE season = {int(season)} ORDER BY position"
    )
    for row in rows:
        row["_highlight"] = row["position"] is not None and row["position"] <= 4
    return rows


def _orange_cap(query: Query, season: int) -> list[Row]:
    return query(
        "SELECT player_name, teams, matches, innings, runs, highest_score_text, average, "
        "strike_rate, fifties, hundreds FROM player_season_batting "
        f"WHERE season = {int(season)} "
        f"ORDER BY runs DESC, strike_rate DESC, player_name LIMIT {TOP_N}"
    )


def _purple_cap(query: Query, season: int) -> list[Row]:
    return query(
        "SELECT player_name, teams, matches, overs, wickets, runs_conceded, economy, average, "
        "best_figures FROM player_season_bowling "
        f"WHERE season = {int(season)} AND wickets > 0 "
        f"ORDER BY wickets DESC, economy ASC, player_name LIMIT {TOP_N}"
    )


def _recent(query: Query, season: int, limit: int = TOP_N) -> list[Row]:
    return query(
        "SELECT match_date, stage, venue, first_batting_team, first_innings_score, "
        "second_batting_team, second_innings_score, result_text, player_of_match "
        f"FROM match_summary WHERE season = {int(season)} "
        f"ORDER BY match_date DESC, match_id DESC LIMIT {int(limit)}"
    )


def _final(query: Query, season: int) -> Row | None:
    rows = query(
        "SELECT match_date, venue, winner, result_text, player_of_match FROM match_summary "
        f"WHERE season = {int(season)} AND stage = 'Final' ORDER BY match_date DESC LIMIT 1"
    )
    return rows[0] if rows else None


def _all_time_batting(query: Query) -> list[Row]:
    rows = query(
        "SELECT player_id, max(player_name) AS player_name, count(*) AS seasons, "
        "sum(innings) AS innings, sum(runs) AS runs, sum(balls) AS balls, sum(outs) AS outs, "
        "sum(fifties) AS fifties, sum(hundreds) AS hundreds "
        "FROM player_season_batting GROUP BY player_id "
        f"ORDER BY runs DESC, player_name LIMIT {TOP_N}"
    )
    for row in rows:
        row["average"] = _ratio(row["runs"], row["outs"])
        row["strike_rate"] = _ratio(row["runs"], row["balls"], 100)
    return rows


def _all_time_bowling(query: Query) -> list[Row]:
    rows = query(
        "SELECT player_id, max(player_name) AS player_name, count(*) AS seasons, "
        "sum(wickets) AS wickets, sum(runs_conceded) AS runs_conceded, sum(balls) AS balls "
        "FROM player_season_bowling GROUP BY player_id "
        f"ORDER BY wickets DESC, runs_conceded ASC, player_name LIMIT {TOP_N}"
    )
    for row in rows:
        row["economy"] = _ratio(row["runs_conceded"], row["balls"], 6)
        row["average"] = _ratio(row["runs_conceded"], row["wickets"])
    return rows


# --------------------------------------------------------------------------- sections
POINTS_COLUMNS = (
    Column("#", lambda r: _text(r["position"]), numeric=True, title="Position"),
    Column(
        "Team",
        lambda r: (
            _text(r["team"])
            + (
                ' <span class="badge">Q<span class="sr-only">ualified</span></span>'
                if r.get("_highlight")
                else ""
            )
        ),
    ),
    Column("P", lambda r: _text(r["played"]), numeric=True, title="Played"),
    Column("W", lambda r: _text(r["won"]), numeric=True, title="Won"),
    Column("L", lambda r: _text(r["lost"]), numeric=True, title="Lost"),
    Column(
        "SO",
        lambda r: _text(r["tied"]),
        numeric=True,
        title="Tied matches decided by a super over (already counted in W and L)",
    ),
    Column("NR", lambda r: _text(r["no_result"]), numeric=True, title="No result"),
    Column("Pts", lambda r: _text(r["points"]), numeric=True, title="Points"),
    Column("NRR", lambda r: _nrr(r["net_run_rate"]), numeric=True, title="Net run rate"),
)

BATTING_COLUMNS = (
    Column("Player", lambda r: _text(r["player_name"])),
    Column("Team", lambda r: _teams(r["teams"])),
    Column("M", lambda r: _text(r["matches"]), numeric=True, title="Matches"),
    Column("Runs", lambda r: _text(r["runs"]), numeric=True),
    Column("HS", lambda r: _text(r["highest_score_text"]), numeric=True, title="Highest score"),
    Column("Avg", lambda r: _decimal(r["average"]), numeric=True, title="Batting average"),
    Column("SR", lambda r: _decimal(r["strike_rate"]), numeric=True, title="Strike rate"),
    Column(
        "50/100",
        lambda r: f"{_text(r['fifties'])}/{_text(r['hundreds'])}",
        numeric=True,
        title="Fifties / hundreds",
    ),
)

BOWLING_COLUMNS = (
    Column("Player", lambda r: _text(r["player_name"])),
    Column("Team", lambda r: _teams(r["teams"])),
    Column("M", lambda r: _text(r["matches"]), numeric=True, title="Matches"),
    Column("Overs", lambda r: _text(r["overs"]), numeric=True),
    Column("Wkts", lambda r: _text(r["wickets"]), numeric=True, title="Wickets"),
    Column("Econ", lambda r: _decimal(r["economy"]), numeric=True, title="Economy rate"),
    Column("Avg", lambda r: _decimal(r["average"]), numeric=True, title="Bowling average"),
    Column("Best", lambda r: _text(r["best_figures"]), numeric=True, title="Best figures"),
)

RESULT_COLUMNS = (
    Column("Date", lambda r: _text(r["match_date"])),
    Column("Stage", lambda r: _text(r["stage"])),
    Column(
        "Match",
        lambda r: (
            f"{_text(r['first_batting_team'])} {_text(r['first_innings_score'])}"
            f"<br>{_text(r['second_batting_team'])} {_text(r['second_innings_score'])}"
        ),
    ),
    Column("Result", lambda r: _text(r["result_text"])),
    Column("Player of the match", lambda r: _teams(r["player_of_match"])),
)

ALL_TIME_BATTING_COLUMNS = (
    Column("Player", lambda r: _text(r["player_name"])),
    Column("Seasons", lambda r: _text(r["seasons"]), numeric=True),
    Column("Inns", lambda r: _text(r["innings"]), numeric=True, title="Innings"),
    Column("Runs", lambda r: _text(r["runs"]), numeric=True),
    Column("Avg", lambda r: _decimal(r["average"]), numeric=True, title="Batting average"),
    Column("SR", lambda r: _decimal(r["strike_rate"]), numeric=True, title="Strike rate"),
    Column(
        "50/100",
        lambda r: f"{_text(r['fifties'])}/{_text(r['hundreds'])}",
        numeric=True,
        title="Fifties / hundreds",
    ),
)

ALL_TIME_BOWLING_COLUMNS = (
    Column("Player", lambda r: _text(r["player_name"])),
    Column("Seasons", lambda r: _text(r["seasons"]), numeric=True),
    Column("Wkts", lambda r: _text(r["wickets"]), numeric=True, title="Wickets"),
    Column("Econ", lambda r: _decimal(r["economy"]), numeric=True, title="Economy rate"),
    Column("Avg", lambda r: _decimal(r["average"]), numeric=True, title="Bowling average"),
)


def _season_sections(query: Query, season: int, *, heading_level: int) -> str:
    h = f"h{heading_level}"
    final = _final(query, season)
    final_html = (
        f"<p class=lede><strong>Final:</strong> {_text(final['result_text'])} "
        f"({_text(final['venue'])}, {_text(final['match_date'])}); "
        f"player of the match {_teams(final['player_of_match'])}.</p>"
        if final
        else ""
    )
    return (
        final_html
        + f"<section><{h}>Points table</{h}>"
        + _table(
            f"IPL {season} league stage",
            POINTS_COLUMNS,
            _points(query, season),
            "No league table yet.",
        )
        + '<p class="note">Ranked on points, then net run rate. '
        "<span class=badge>Q</span> marks the four playoff places. SO counts ties settled by "
        "a super over; those games are already in W and L, so P = W + L + NR.</p></section>"
        + f"<section><{h}>Orange Cap (most runs)</{h}>"
        + _table(
            f"Top {TOP_N} run scorers, IPL {season}",
            BATTING_COLUMNS,
            _orange_cap(query, season),
            "No batting figures yet.",
        )
        + f"</section><section><{h}>Purple Cap (most wickets)</{h}>"
        + _table(
            f"Top {TOP_N} wicket takers, IPL {season}",
            BOWLING_COLUMNS,
            _purple_cap(query, season),
            "No bowling figures yet.",
        )
        + "</section>"
    )


# --------------------------------------------------------------------------- pages
STYLE = """
:root { color-scheme: light dark; --fg:#1b1f24; --muted:#59636e; --bg:#ffffff; --line:#d1d9e0;
  --accent:#0b5cad; --hi:#eef5fc; --badge-bg:#0b5cad; --badge-fg:#ffffff; }
@media (prefers-color-scheme: dark) { :root { --fg:#e6edf3; --muted:#9198a1; --bg:#0d1117;
  --line:#3d444d; --accent:#6cb6ff; --hi:#122236; --badge-bg:#6cb6ff; --badge-fg:#0d1117; } }
* { box-sizing: border-box; }
body { margin:0; font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  color:var(--fg); background:var(--bg); }
header, main, footer { max-width: 64rem; margin: 0 auto; padding: 0 1rem; }
header { padding-top: 1.5rem; border-bottom: 1px solid var(--line); }
h1 { font-size: 1.6rem; margin: 0 0 .25rem; } h2 { font-size: 1.3rem; margin-top: 2rem; }
h3 { font-size: 1.1rem; margin-top: 1.5rem; }
a { color: var(--accent); }
.meta, .note, footer { color: var(--muted); font-size: .875rem; }
.lede { font-size: 1.05rem; }
nav ul { list-style: none; padding: 0; display: flex; flex-wrap: wrap; gap: .25rem .75rem; }
.table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
caption { text-align: left; color: var(--muted); font-size: .875rem; padding: .25rem 0; }
th, td { padding: .4rem .6rem; border-bottom: 1px solid var(--line); text-align: left;
  vertical-align: top; }
th { font-weight: 600; white-space: nowrap; } td.num, th.num { text-align: right; }
tr.highlight td { background: var(--hi); }
abbr { text-decoration: none; }
.badge { display: inline-block; font-size: .7rem; font-weight: 700; padding: 0 .35rem;
  border-radius: .25rem; background: var(--badge-bg); color: var(--badge-fg); }
.sr-only { position:absolute; width:1px; height:1px; overflow:hidden; clip:rect(0 0 0 0); }
.empty { color: var(--muted); font-style: italic; }
footer { border-top: 1px solid var(--line); margin-top: 3rem; padding: 1rem; }
"""


def _page(title: str, body: str, *, as_of: str, generated: str, root: str) -> str:
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        '<meta name=viewport content="width=device-width, initial-scale=1">'
        f"<title>{html.escape(title)}</title>"
        f'<link rel=stylesheet href="{root}style.css"></head><body>'
        f'<header><p class=meta><a href="{root}index.html">IPL Lakehouse results</a></p>'
        f"<h1>{html.escape(title)}</h1>"
        f'<p class=meta>Data as of <time datetime="{html.escape(as_of)}">{html.escape(as_of)}</time>'
        f' (latest match in the data) · generated <time datetime="{generated}">{generated}</time></p>'
        f"</header><main>{body}</main>"
        "<footer><p>Ball-by-ball data from "
        f'<a href="{CRICSHEET_URL}">Cricsheet</a>, licensed under the '
        f'<a href="{ODC_BY_URL}">Open Data Commons Attribution License (ODC-By 1.0)</a>. '
        f'Built by <a href="{REPO_URL}">ipl-streaming-lakehouse</a>; '
        "refreshed on a schedule by GitHub Actions. Not affiliated with the IPL or BCCI.</p>"
        "</footer></body></html>"
    )


def build_site(
    out: Path,
    settings: Settings | None = None,
    *,
    query: Query | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Write the site to ``out`` and return the summary metadata."""
    if query is None:
        if settings is None:
            raise ValueError("pass settings or a query function")
        query = _sql_query(settings)
    generated = (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")

    stats = query(
        "SELECT count(*) AS matches, max(match_date) AS as_of, max(season) AS latest_season, "
        "min(season) AS first_season FROM match_summary"
    )[0]
    if not stats["matches"]:
        raise ValueError("gold.match_summary is empty: run `ipl batch` first")
    latest, as_of = int(stats["latest_season"]), str(stats["as_of"])
    seasons = [
        int(r["season"])
        for r in query("SELECT DISTINCT season FROM match_summary ORDER BY season DESC")
    ]
    page = lambda title, body, root: _page(title, body, as_of=as_of, generated=generated, root=root)  # noqa: E731

    (out / "seasons").mkdir(parents=True, exist_ok=True)
    (out / "style.css").write_text(STYLE.strip() + "\n", encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")

    season_nav = (
        '<nav aria-label="Seasons"><ul>'
        + "".join(f'<li><a href="seasons/{s}.html">{s}</a></li>' for s in seasons)
        + "</ul></nav>"
    )
    champions = [{"season": s, **(_final(query, s) or {})} for s in seasons]
    champions_columns = (
        Column("Season", lambda r: f'<a href="seasons/{r["season"]}.html">{r["season"]}</a>'),
        Column("Champion", lambda r: _text(r.get("winner"))),
        Column("Final", lambda r: _text(r.get("result_text"))),
    )
    index_body = (
        f"<p class=lede>{stats['matches']:,} matches across {len(seasons)} seasons "
        f"({stats['first_season']}&ndash;{latest}). The latest season is shown first; every season "
        "has its own page.</p>"
        + season_nav
        + f"<h2>IPL {latest}</h2>"
        + _season_sections(query, latest, heading_level=3)
        + "<section><h3>Recent results</h3>"
        + _table(
            f"Latest {TOP_N} matches of IPL {latest}",
            RESULT_COLUMNS,
            _recent(query, latest),
            "No matches yet.",
        )
        + "</section><h2>All-time leaders</h2><section><h3>Most runs</h3>"
        + _table(
            "Most runs, all IPL seasons (super overs excluded)",
            ALL_TIME_BATTING_COLUMNS,
            _all_time_batting(query),
            "No batting figures yet.",
        )
        + "</section><section><h3>Most wickets</h3>"
        + _table(
            "Most wickets, all IPL seasons (super overs excluded)",
            ALL_TIME_BOWLING_COLUMNS,
            _all_time_bowling(query),
            "No bowling figures yet.",
        )
        + "</section><section><h2>Champions</h2>"
        + _table("Winner of every IPL final", champions_columns, champions, "No finals yet.")
        + "</section>"
    )
    (out / "index.html").write_text(page("IPL results", index_body, ""), encoding="utf-8")

    for season in seasons:
        body = _season_sections(query, season, heading_level=2) + (
            "<section><h2>Results</h2>"
            + _table(
                f"All matches of IPL {season}, newest first",
                RESULT_COLUMNS,
                _recent(query, season, limit=200),
                "No matches.",
            )
            + "</section>"
        )
        (out / "seasons" / f"{season}.html").write_text(
            page(f"IPL {season}", body, "../"), encoding="utf-8"
        )

    summary = {
        "as_of": as_of,
        "generated_at": generated,
        "latest_season": latest,
        "seasons": len(seasons),
        "matches": stats["matches"],
        "source": CRICSHEET_URL,
        "license": "ODC-By 1.0",
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
