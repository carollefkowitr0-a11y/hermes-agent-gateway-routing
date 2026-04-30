import json
import sqlite3

from gateway.evidence import main, summarize_spool
from gateway.spool import GatewaySpool


def test_evidence_outputs_counts_and_safe_latest_metadata_only(tmp_path):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    first = spool.enqueue_update(
        "telegram",
        "naval",
        "naval",
        1,
        {"message": {"text": "private text"}},
        chat_id="chat-private",
        thread_id="thread-private",
        user_id="user-private",
        event_type="message",
    )
    second = spool.enqueue_update("telegram", "naval", "naval", 2, {"secret": "raw-secret"}, event_type="callback")
    spool.mark_state(second.id, "done")

    summary = summarize_spool(spool.db_path, limit=5)

    assert summary["exists"] is True
    assert summary["counts_by_state"] == {"done": 1, "queued": 1}
    assert [row["id"] for row in summary["latest"]] == [second.id, first.id]
    assert summary["latest"][0]["state"] == "done"
    assert summary["latest"][0]["event_type"] == "callback"
    text = json.dumps(summary, sort_keys=True)
    for forbidden in ["payload_json", "chat_id", "user_id", "thread_id", "private text", "raw-secret", "chat-private"]:
        assert forbidden not in text


def test_evidence_cli_outputs_json_without_private_fields(tmp_path, capsys):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    spool.enqueue_update("telegram", "naval", "naval", 1, {"message": {"text": "hide me"}}, chat_id="42")

    exit_code = main(["--spool-db", str(spool.db_path), "--limit", "1"])
    captured = capsys.readouterr()
    data = json.loads(captured.out)

    assert exit_code == 0
    assert data["counts_by_state"] == {"queued": 1}
    assert len(data["latest"]) == 1
    assert "payload_json" not in captured.out
    assert "chat_id" not in captured.out
    assert "hide me" not in captured.out
    assert "42" not in json.dumps(data["latest"])


def test_missing_db_returns_empty_summary(tmp_path):
    summary = summarize_spool(tmp_path / "missing.db")

    assert summary["exists"] is False
    assert summary["counts_by_state"] == {}
    assert summary["latest"] == []


def test_evidence_query_does_not_select_forbidden_columns(tmp_path, monkeypatch):
    spool = GatewaySpool(tmp_path / "spool.db")
    spool.initialize()
    spool.enqueue_update("telegram", "naval", "naval", 1, {"message": {"text": "hide"}}, chat_id="42")
    statements = []
    real_connect = sqlite3.connect

    class RecordingConnection:
        def __init__(self, conn):
            self._conn = conn

        def __enter__(self):
            self._conn.__enter__()
            return self

        def __exit__(self, *args):
            return self._conn.__exit__(*args)

        @property
        def row_factory(self):
            return self._conn.row_factory

        @row_factory.setter
        def row_factory(self, value):
            self._conn.row_factory = value

        def execute(self, sql, *args, **kwargs):
            statements.append(sql.lower())
            return self._conn.execute(sql, *args, **kwargs)

    def recording_connect(*args, **kwargs):
        return RecordingConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(sqlite3, "connect", recording_connect)

    summarize_spool(spool.db_path)

    selected = "\n".join(sql for sql in statements if sql.strip().startswith("select"))
    assert "payload_json" not in selected
    assert "chat_id" not in selected
    assert "user_id" not in selected
    assert "thread_id" not in selected
