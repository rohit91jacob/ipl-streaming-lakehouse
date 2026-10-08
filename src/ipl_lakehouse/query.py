"""Ad-hoc SQL over the lakehouse without a JVM (delta-rs + DataFusion via ``deltalake``).

Every Delta table under silver/, gold/ and ops/ is registered by its folder name
(``points_table``, ``deliveries``, ``live_scorecard``, ``dq_results``, ...).
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, QueryBuilder

from ipl_lakehouse.config import Settings


def discover_tables(settings: Settings) -> dict[str, Path]:
    tables: dict[str, Path] = {}
    for layer in ("silver", "gold", "ops"):
        root = settings.data_dir / layer
        if not root.is_dir():
            continue
        for child in sorted(root.iterdir()):
            if (child / "_delta_log").is_dir():
                tables.setdefault(child.name, child)
    stream_events = settings.lake.stream_events
    if (stream_events / "_delta_log").is_dir():
        tables["stream_events"] = stream_events
    if (settings.lake.manifest / "_delta_log").is_dir():
        tables["bronze_manifest"] = settings.lake.manifest
    return tables


def run_sql(settings: Settings, sql: str) -> pa.Table:
    builder = QueryBuilder()
    for name, path in discover_tables(settings).items():
        builder = builder.register(name, DeltaTable(str(path)))
    return pa.table(builder.execute(sql).read_all())


def sql_literal(value: object) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def read_table(path: Path, **filters: object) -> pa.Table:
    """Read a Delta table into Arrow, optionally filtering on equality (column=value).

    Goes through DataFusion rather than ``DeltaTable.to_pyarrow_*``: the latter can abort the
    interpreter at exit (``terminate called without an active exception``) with deltalake 1.x.
    """
    where = " AND ".join(f'"{column}" = {sql_literal(value)}' for column, value in filters.items())
    sql = "SELECT * FROM t" + (f" WHERE {where}" if where else "")
    builder = QueryBuilder().register("t", DeltaTable(str(path)))
    return pa.table(builder.execute(sql).read_all())


def format_table(table: pa.Table, max_rows: int = 50) -> str:
    rows = table.slice(0, max_rows).to_pylist()
    columns = table.column_names
    cells = [[("" if r[c] is None else str(r[c])) for c in columns] for r in rows]
    widths = [max([len(c)] + [len(row[i]) for row in cells]) for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)) for row in cells]
    if table.num_rows > max_rows:
        out.append(f"... {table.num_rows - max_rows} more rows")
    return "\n".join(out)
