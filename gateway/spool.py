"""Durable local SQLite spool primitives for gateway updates."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home


ALLOWED_STATES = {
    "received",
    "queued",
    "waking",
    "dispatched",
    "processing",
    "done",
    "failed",
    "dead_letter",
    "duplicate",
    "rejected",
}


@dataclass(frozen=True)
class EnqueueResult:
    id: int
    inserted: bool
    duplicate: bool
    state: str


class GatewaySpool:
    """Small SQLite-backed durable queue for gateway update payloads.

    Current worker-created lifecycle is queued/failed -> processing -> done or
    failed/dead_letter. ``dispatched`` remains an allowed legacy state for
    existing rows, but new worker evidence should gate on fresh rows reaching
    ``done`` rather than treating historical ``dispatched`` rows as completed.
    """

    def __init__(self, db_path: Path | None = None):
        self.db_path = Path(db_path) if db_path is not None else get_hermes_home() / "gateway_spool.db"

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        created = not self.db_path.exists()
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gateway_spool (
                    id INTEGER PRIMARY KEY,
                    platform TEXT NOT NULL,
                    route TEXT NOT NULL,
                    target_profile TEXT NOT NULL,
                    update_id TEXT NOT NULL,
                    chat_id TEXT,
                    thread_id TEXT,
                    user_id TEXT,
                    event_type TEXT,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(platform, route, update_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_gateway_spool_queued
                ON gateway_spool(target_profile, state, id)
                """
            )
        self._restrict_file_permissions()

    def enqueue_update(
        self,
        platform: str,
        route: str,
        target_profile: str,
        update_id: str | int,
        payload: Any,
        chat_id: str | int | None = None,
        thread_id: str | int | None = None,
        user_id: str | int | None = None,
        event_type: str | None = None,
    ) -> EnqueueResult:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        update_id_text = str(update_id)
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO gateway_spool (
                        platform, route, target_profile, update_id,
                        chat_id, thread_id, user_id, event_type,
                        state, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
                    """,
                    (
                        platform,
                        route,
                        target_profile,
                        update_id_text,
                        self._optional_text(chat_id),
                        self._optional_text(thread_id),
                        self._optional_text(user_id),
                        event_type,
                        payload_json,
                    ),
                )
                self._restrict_file_permissions()
                return EnqueueResult(id=int(cursor.lastrowid), inserted=True, duplicate=False, state="queued")
        except sqlite3.IntegrityError:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT id, state FROM gateway_spool
                    WHERE platform = ? AND route = ? AND update_id = ?
                    """,
                    (platform, route, update_id_text),
                ).fetchone()
            if row is None:
                raise
            return EnqueueResult(id=int(row["id"]), inserted=False, duplicate=True, state=row["state"])

    def get_next_queued(self, target_profile: str | None = None) -> dict[str, Any] | None:
        sql = "SELECT * FROM gateway_spool WHERE state IN ('queued', 'failed')"
        params: tuple[Any, ...] = ()
        if target_profile is not None:
            sql += " AND target_profile = ?"
            params = (target_profile,)
        sql += " ORDER BY id ASC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def claim_next_queued(self, target_profile: str | None = None) -> dict[str, Any] | None:
        """Atomically claim the oldest queued/failed row and mark it processing."""
        where = "state IN ('queued', 'failed')"
        params: tuple[Any, ...] = ()
        if target_profile is not None:
            where += " AND target_profile = ?"
            params = (target_profile,)
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE gateway_spool
                SET state = 'processing', error = NULL, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM gateway_spool
                    WHERE {where}
                    ORDER BY id ASC
                    LIMIT 1
                )
                RETURNING *
                """,
                params,
            )
            row = cursor.fetchone()
        return dict(row) if row is not None else None

    def requeue_stale_processing(self, *, older_than_seconds: int = 900, target_profile: str | None = None) -> int:
        """Move stale processing rows back to queued so a crashed worker can retry them."""
        if older_than_seconds < 0:
            raise ValueError("older_than_seconds must be non-negative")
        sql = """
            UPDATE gateway_spool
            SET state = 'queued', error = 'requeued_stale_processing', updated_at = CURRENT_TIMESTAMP
            WHERE state = 'processing'
              AND updated_at <= datetime('now', ?)
        """
        params: tuple[Any, ...]
        interval = f"-{int(older_than_seconds)} seconds"
        if target_profile is not None:
            sql += " AND target_profile = ?"
            params = (interval, target_profile)
        else:
            params = (interval,)
        with self._connect() as conn:
            cursor = conn.execute(sql, params)
        return int(cursor.rowcount or 0)

    def get_row(self, id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM gateway_spool WHERE id = ?", (id,)).fetchone()
        return dict(row) if row is not None else None

    def mark_state(self, id: int, state: str, error: str | None = None) -> None:
        if state not in ALLOWED_STATES:
            raise ValueError(f"Invalid gateway spool state: {state}")
        increment_attempts = state == "processing"
        with self._connect() as conn:
            if increment_attempts:
                cursor = conn.execute(
                    """
                    UPDATE gateway_spool
                    SET state = ?, error = ?, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (state, error, id),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE gateway_spool
                    SET state = ?, error = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (state, error, id),
                )
        if cursor.rowcount != 1:
            raise KeyError(f"Gateway spool row not found: {id}")

    def count_by_state(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM gateway_spool GROUP BY state ORDER BY state"
            ).fetchall()
        return {row["state"]: int(row["count"]) for row in rows}

    def count_by_state_for_profile(self, target_profile: str) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM gateway_spool
                WHERE target_profile = ?
                GROUP BY state
                ORDER BY state
                """,
                (target_profile,),
            ).fetchall()
        return {row["state"]: int(row["count"]) for row in rows}

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _restrict_file_permissions(self) -> None:
        for path in self._permission_paths():
            if not path.exists():
                continue
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass

    def _permission_paths(self) -> tuple[Path, Path, Path]:
        return (
            self.db_path,
            Path(str(self.db_path) + "-wal"),
            Path(str(self.db_path) + "-shm"),
        )

    @staticmethod
    def _optional_text(value: str | int | None) -> str | None:
        return None if value is None else str(value)
