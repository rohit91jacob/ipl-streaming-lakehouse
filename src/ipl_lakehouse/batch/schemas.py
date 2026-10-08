"""Explicit Spark schema for Cricsheet match JSON (format docs: https://cricsheet.org/format/json/).

Reading with a declared schema (never inference) keeps the bronze -> silver contract stable:
fields Cricsheet adds later are ignored until we model them, and type drift fails loudly.
``info.season`` is a string because Cricsheet mixes ``2017`` and ``"2007/08"``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def _s(*fields: StructField) -> StructType:
    return StructType(list(fields))


def _f(name: str, dtype, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)


_NAMES = ArrayType(StringType())

RUNS = _s(
    _f("batter", LongType()),
    _f("extras", LongType()),
    _f("total", LongType()),
    _f("non_boundary", BooleanType()),
)

EXTRAS = _s(
    _f("wides", LongType()),
    _f("noballs", LongType()),
    _f("byes", LongType()),
    _f("legbyes", LongType()),
    _f("penalty", LongType()),
)

FIELDER = _s(_f("name", StringType()), _f("substitute", BooleanType()))

WICKET = _s(
    _f("player_out", StringType()),
    _f("kind", StringType()),
    _f("fielders", ArrayType(FIELDER)),
)

REVIEW = _s(
    _f("by", StringType()),
    _f("umpire", StringType()),
    _f("batter", StringType()),
    _f("decision", StringType()),
    _f("umpires_call", BooleanType()),
    _f("type", StringType()),
)

REPLACEMENT_MATCH = _s(
    _f("in", StringType()),
    _f("out", StringType()),
    _f("team", StringType()),
    _f("reason", StringType()),
)

REPLACEMENT_ROLE = _s(
    _f("in", StringType()),
    _f("out", StringType()),
    _f("reason", StringType()),
    _f("role", StringType()),
)

DELIVERY = _s(
    _f("actual_delivery", StringType()),
    _f("batter", StringType()),
    _f("bowler", StringType()),
    _f("non_striker", StringType()),
    _f("runs", RUNS),
    _f("extras", EXTRAS),
    _f("wickets", ArrayType(WICKET)),
    _f("review", REVIEW),
    _f(
        "replacements",
        _s(_f("match", ArrayType(REPLACEMENT_MATCH)), _f("role", ArrayType(REPLACEMENT_ROLE))),
    ),
)

OVER = _s(_f("over", LongType()), _f("deliveries", ArrayType(DELIVERY)))

INNINGS = _s(
    _f("team", StringType()),
    _f("overs", ArrayType(OVER)),
    _f(
        "powerplays",
        ArrayType(_s(_f("from", DoubleType()), _f("to", DoubleType()), _f("type", StringType()))),
    ),
    _f("target", _s(_f("overs", DoubleType()), _f("runs", LongType()))),
    _f("super_over", BooleanType()),
    _f("absent_hurt", _NAMES),
    _f("penalty_runs", _s(_f("pre", LongType()), _f("post", LongType()))),
    _f("declared", BooleanType()),
    _f("forfeited", BooleanType()),
    _f(
        "miscounted_overs",
        MapType(StringType(), _s(_f("balls", LongType()), _f("umpire", StringType()))),
    ),
)

OUTCOME = _s(
    _f("winner", StringType()),
    _f("by", _s(_f("runs", LongType()), _f("wickets", LongType()), _f("innings", LongType()))),
    _f("result", StringType()),
    _f("method", StringType()),
    _f("eliminator", StringType()),
    _f("bowl_out", StringType()),
)

INFO = _s(
    _f("balls_per_over", LongType()),
    _f("city", StringType()),
    _f("dates", _NAMES),
    _f(
        "event",
        _s(
            _f("name", StringType()),
            _f("match_number", LongType()),
            _f("stage", StringType()),
            _f("group", StringType()),
        ),
    ),
    _f("gender", StringType()),
    _f("match_type", StringType()),
    _f("match_type_number", LongType()),
    _f(
        "officials",
        _s(
            _f("umpires", _NAMES),
            _f("tv_umpires", _NAMES),
            _f("match_referees", _NAMES),
            _f("reserve_umpires", _NAMES),
        ),
    ),
    _f("outcome", OUTCOME),
    _f("overs", LongType()),
    _f("player_of_match", _NAMES),
    _f("players", MapType(StringType(), _NAMES)),
    _f("registry", _s(_f("people", MapType(StringType(), StringType())))),
    _f("season", StringType()),
    _f("team_type", StringType()),
    _f("teams", _NAMES),
    _f("toss", _s(_f("decision", StringType()), _f("winner", StringType()))),
    _f("venue", StringType()),
)

MATCH = _s(
    _f(
        "meta",
        _s(
            _f("data_version", StringType()),
            _f("created", StringType()),
            _f("revision", LongType()),
        ),
    ),
    _f("info", INFO),
    _f("innings", ArrayType(INNINGS)),
)
