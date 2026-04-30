"""Privacy-safe local spool evidence summaries."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from gateway.spool import GatewaySpool

SAFE_LATEST_COLUMNS = ("id", "state", "event_type", "target_profile", "created_at", "updated_at")
FORBIDDEN_COLUMNS = {"payload_json", "chat_id", "user_id", "thread_id", "text", "token", "secret"}


def summarize_spool(db_path: Path | None = None, *, limit: int = 10) -> dict[str, Any]:
    """Read only non-private metadata from the gateway spool DB."""

    spool = GatewaySpool(db_path) if db_path is not None else GatewaySpool()
    if not spool.db_path.exists():
        return {"db_path": str(spool.db_path), "exists": False, "counts_by_state": {}, "latest": []}

    with sqlite3.connect(spool.db_path) as conn:
        conn.row_factory = sqlite3.Row
        columns = _columns(conn)
        _assert_no_forbidden_selection(SAFE_LATEST_COLUMNS)
        counts = {
            row["state"]: int(row["count"])
            for row in conn.execute("SELECT state, COUNT(*) AS count FROM gateway_spool GROUP BY state ORDER BY state")
        }
        select_columns = [column for column in SAFE_LATEST_COLUMNS if column in columns]
        latest: list[dict[str, Any]] = []
        if select_columns:
            sql = f"SELECT {', '.join(select_columns)} FROM gateway_spool ORDER BY id DESC LIMIT ?"
            latest = [dict(row) for row in conn.execute(sql, (limit,)).fetchall()]
    return {"db_path": str(spool.db_path), "exists": True, "counts_by_state": counts, "latest": latest}


def _columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(gateway_spool)").fetchall()
    return {str(row[1]) for row in rows}


def _assert_no_forbidden_selection(columns: tuple[str, ...] | list[str]) -> None:
    lowered = {column.lower() for column in columns}
    forbidden = lowered & FORBIDDEN_COLUMNS
    if forbidden:
        raise RuntimeError(f"forbidden evidence columns requested: {sorted(forbidden)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit privacy-safe gateway spool evidence JSON")
    parser.add_argument("--spool-db", type=Path, default=None, help="SQLite spool DB path")
    parser.add_argument("--limit", type=int, default=10, help="Latest metadata rows to include")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = summarize_spool(args.spool_db, limit=args.limit)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
