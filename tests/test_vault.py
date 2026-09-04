from __future__ import annotations

import json
import shutil
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path

from harness_vault.db import open_database
from harness_vault.ingest import SyncOptions, VaultSyncer, normalize_git_remote
from harness_vault.reporting import export_native, export_unified, status_data, verify


PARENT = "019dc002-6a31-7893-834c-52264540040d"
CHILD = "019dc004-c895-78b2-bcd2-5def029b00a8"
TURN1 = "turn-one"
TURN2 = "turn-two"
CLAUDE = "fcff4a36-be0a-470e-abab-b3a75bb44cd6"
OPENCODE = "ses_fixture_root"
OPENCODE_CHILD = "ses_fixture_child"


def line(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


def write_lines(path: Path, values: list[object], *, partial: object | None = None) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = b"".join(line(value) for value in values)
    if partial is not None:
        data += json.dumps(partial, separators=(",", ":")).encode()
    path.write_bytes(data)
    return data


class VaultTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.codex = self.root / ".codex"
        self.claude = self.root / ".claude"
        self.desktop = self.root / "claude-code-sessions"
        self.opencode = self.root / ".local/share/opencode"
        self.vault = self.root / "vault.sqlite"
        self.parent_rollout = self.codex / "sessions/2026/01/01" / f"rollout-2026-01-01T00-00-00-{PARENT}.jsonl"
        self._make_codex()
        self._make_claude()
        self._make_opencode()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def options(self, **changes: object) -> SyncOptions:
        values = {
            "db_path": self.vault,
            "host_id": "fixture-host",
            "codex_home": self.codex,
            "claude_home": self.claude,
            "claude_app_support": self.desktop,
            "opencode_data": self.opencode,
            "artifact_max_bytes": 32,
            "max_record_bytes": 1024 * 1024,
            "ignore_space_check": True,
        }
        values.update(changes)
        return SyncOptions(**values)

    def sync(self) -> tuple[sqlite3.Connection, object]:
        conn = open_database(self.vault)
        stats = VaultSyncer(conn, self.options()).sync()
        return conn, stats

    def _make_codex(self) -> None:
        first = {
            "input_tokens": 10, "cached_input_tokens": 2, "cache_write_input_tokens": 1,
            "output_tokens": 4, "reasoning_output_tokens": 3, "total_tokens": 17,
        }
        second = {
            "input_tokens": 19, "cached_input_tokens": 3, "cache_write_input_tokens": 1,
            "output_tokens": 9, "reasoning_output_tokens": 5, "total_tokens": 32,
        }
        parent = [
            {"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta", "ordinal": 0, "payload": {"id": PARENT, "originator": "Codex Desktop", "cwd": str(self.root / "repo"), "timestamp": "2026-01-01T00:00:00Z", "base_instructions": {"text": "fixture", "provenance": {"type": "model"}}}},
            {"timestamp": "2026-01-01T00:00:01Z", "type": "turn_context", "ordinal": 1, "payload": {"turn_id": TURN1, "cwd": str(self.root / "repo"), "model": "gpt-fixture", "effort": "high"}},
            {"timestamp": "2026-01-01T00:00:02Z", "type": "event_msg", "ordinal": 2, "payload": {"type": "task_started", "turn_id": TURN1}},
            {"timestamp": "2026-01-01T00:00:03Z", "type": "response_item", "ordinal": 3, "payload": {"type": "custom_tool_call", "id": "i1", "call_id": "call-1", "name": "exec", "input": "pwd"}},
            {"timestamp": "2026-01-01T00:00:04Z", "type": "response_item", "ordinal": 4, "payload": {"type": "custom_tool_call_output", "call_id": "call-1", "output": "ok"}},
            {"timestamp": "2026-01-01T00:00:05Z", "type": "response_item", "ordinal": 5, "payload": {"type": "reasoning", "id": "r1", "encrypted_content": "cipher", "summary": [{"type": "summary_text", "text": "summary"}]}},
            {"timestamp": "2026-01-01T00:00:06Z", "type": "event_msg", "ordinal": 6, "payload": {"type": "token_count", "info": {"total_token_usage": first}}},
            {"timestamp": "2026-01-01T00:00:07Z", "type": "event_msg", "ordinal": 7, "payload": {"type": "task_complete", "turn_id": TURN1}},
            {"timestamp": "2026-01-01T00:01:00Z", "type": "turn_context", "ordinal": 8, "payload": {"turn_id": TURN2, "cwd": str(self.root / "repo"), "model": "gpt-fixture"}},
            {"timestamp": "2026-01-01T00:01:01Z", "type": "event_msg", "ordinal": 9, "payload": {"type": "task_started", "turn_id": TURN2}},
            {"timestamp": "2026-01-01T00:01:02Z", "type": "event_msg", "ordinal": 10, "payload": {"type": "token_count", "info": {"total_token_usage": second}}},
            {"timestamp": "2026-01-01T00:01:03Z", "type": "event_msg", "ordinal": 11, "payload": {"type": "task_complete", "turn_id": TURN2}},
            {"timestamp": "2026-01-01T00:01:04Z", "type": "event_msg", "ordinal": 12, "payload": {"type": "agent_reasoning", "text": "visible summary"}},
            {"timestamp": "2026-01-01T00:01:05Z", "type": "event_msg", "ordinal": 13, "payload": {"type": "sub_agent_activity", "kind": "started"}},
            {"timestamp": "2026-01-01T00:01:06Z", "type": "event_msg", "ordinal": 14, "payload": {"type": "mcp_tool_call_end", "call_id": "mcp-1", "invocation": {"tool": "list"}, "result": {"Ok": {}}}},
            {"timestamp": "2026-01-01T00:01:07Z", "type": "response_item", "ordinal": 15, "payload": {"type": "agent_message", "content": [{"type": "output_text", "text": "agent reply"}]}},
            {"timestamp": "2026-01-01T00:01:08Z", "type": "compacted", "ordinal": 16, "payload": {"replacement_history": []}},
        ]
        write_lines(self.parent_rollout, parent)
        child_path = self.codex / "sessions/2026/01/01" / f"rollout-2026-01-01T00-02-00-{CHILD}.jsonl"
        write_lines(child_path, [
            {"timestamp": "2026-01-01T00:02:00Z", "type": "session_meta", "ordinal": 0, "payload": {"id": CHILD, "originator": "Codex Desktop", "source": {"subagent": {"thread_spawn": {"parent_thread_id": PARENT, "depth": 1, "agent_nickname": "Kepler"}}}}},
            {"timestamp": "2026-01-01T00:02:01Z", "type": "mystery", "ordinal": 1, "payload": {}},
        ])
        plan = self.codex / "plans" / PARENT / TURN1 / "PLAN.md"
        plan.parent.mkdir(parents=True)
        plan.write_text("# Fixture plan\n", encoding="utf-8")
        self.codex.mkdir(exist_ok=True)
        (self.codex / "session_index.jsonl").write_bytes(line({"id": PARENT, "thread_name": "Fixture run", "updated_at": "2026-01-01T01:00:00Z"}))
        goals = sqlite3.connect(self.codex / "goals_1.sqlite")
        goals.execute("CREATE TABLE thread_goals(thread_id TEXT,goal_id TEXT,objective TEXT,status TEXT,token_budget INTEGER,tokens_used INTEGER,time_used_seconds INTEGER,created_at_ms INTEGER,updated_at_ms INTEGER)")
        goals.execute("INSERT INTO thread_goals VALUES(?,?,?,?,?,?,?,?,?)", (PARENT, "goal-1", "fixture", "complete", 100, 20, 1, 1, 2))
        goals.commit()
        goals.close()

    def _make_claude(self) -> None:
        transcript = self.claude / "projects" / "-fixture" / f"{CLAUDE}.jsonl"
        usage = {"input_tokens": 5, "cache_read_input_tokens": 2, "cache_creation_input_tokens": 1, "output_tokens": 7, "output_tokens_details": {"thinking_tokens": 3}}
        write_lines(transcript, [
            {"type": "user", "sessionId": CLAUDE, "uuid": "u1", "promptId": "prompt-1", "timestamp": "2026-01-02T00:00:00Z", "cwd": str(self.root / "repo"), "entrypoint": "cli", "message": {"role": "user", "content": "do it"}},
            {"type": "assistant", "sessionId": CLAUDE, "uuid": "a1", "requestId": "req-1", "timestamp": "2026-01-02T00:00:01Z", "cwd": str(self.root / "repo"), "message": {"id": "msg-1", "role": "assistant", "content": [{"type": "thinking", "thinking": "visible thought", "signature": "sig"}], "usage": usage}},
            {"type": "assistant", "sessionId": CLAUDE, "uuid": "a2", "requestId": "req-1", "timestamp": "2026-01-02T00:00:02Z", "cwd": str(self.root / "repo"), "message": {"id": "msg-1", "role": "assistant", "content": [{"type": "tool_use", "id": "tool-1", "name": "Bash", "input": {"command": "pwd"}}], "usage": usage}},
            {"type": "user", "sessionId": CLAUDE, "uuid": "u2", "timestamp": "2026-01-02T00:00:03Z", "cwd": str(self.root / "repo"), "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tool-1", "content": "ok"}]}},
            {"type": "assistant", "sessionId": CLAUDE, "uuid": "a3", "requestId": "req-1", "timestamp": "2026-01-02T00:00:04Z", "cwd": str(self.root / "repo"), "message": {"id": "msg-1", "role": "assistant", "content": [{"type": "text", "text": "done"}], "usage": usage}},
            {"type": "mode", "sessionId": CLAUDE, "timestamp": "2026-01-02T00:00:05Z", "mode": "normal"},
            {"type": "ai-title", "sessionId": CLAUDE, "timestamp": "2026-01-02T00:00:06Z", "aiTitle": "Fixture AI title"},
        ])
        sub = transcript.parent / CLAUDE / "subagents" / "agent-agent1.jsonl"
        write_lines(sub, [
            {"type": "user", "sessionId": CLAUDE, "agentId": "agent1", "uuid": "su1", "promptId": "sp1", "timestamp": "2026-01-02T00:01:00Z", "message": {"role": "user", "content": "inspect"}},
            {"type": "assistant", "sessionId": CLAUDE, "agentId": "agent1", "uuid": "sa1", "requestId": "sreq", "timestamp": "2026-01-02T00:01:01Z", "message": {"id": "smsg", "role": "assistant", "content": [{"type": "text", "text": "found"}], "usage": {"input_tokens": 2, "output_tokens": 2, "output_tokens_details": {"thinking_tokens": 0}}}},
        ])
        (sub.parent / "agent-agent1.meta.json").write_text(json.dumps({"agentType": "Explore", "toolUseId": "tool-1", "spawnDepth": 1}), encoding="utf-8")
        tool_result = transcript.parent / CLAUDE / "tool-results" / "tool-1.txt"
        tool_result.parent.mkdir(parents=True)
        tool_result.write_bytes(b"this artifact is deliberately larger than cap")
        task = self.claude / "tasks" / CLAUDE / "1.json"
        task.parent.mkdir(parents=True)
        task.write_text(json.dumps({"id": "1", "subject": "fixture", "status": "completed"}), encoding="utf-8")
        plan = self.claude / "plans" / "fixture-plan.md"
        plan.parent.mkdir(parents=True)
        plan.write_text("# Claude plan\n", encoding="utf-8")
        desktop = self.desktop / "account" / "project" / "local_fixture.json"
        desktop.parent.mkdir(parents=True)
        desktop.write_text(json.dumps({"cliSessionId": CLAUDE, "title": "Claude fixture", "cwd": str(self.root / "repo"), "planPath": str(plan), "model": "claude-fixture", "createdAt": "2026-01-02T00:00:00Z", "lastActivityAt": "2026-01-02T01:00:00Z"}), encoding="utf-8")

    def _make_opencode(self) -> None:
        self.opencode.mkdir(parents=True)
        conn = sqlite3.connect(self.opencode / "opencode.db")
        conn.executescript(
            """
            CREATE TABLE project(id TEXT PRIMARY KEY,worktree TEXT,vcs TEXT,name TEXT,time_created INTEGER,time_updated INTEGER);
            CREATE TABLE session(id TEXT PRIMARY KEY,project_id TEXT,parent_id TEXT,slug TEXT,directory TEXT,title TEXT,version TEXT,time_created INTEGER,time_updated INTEGER,time_archived INTEGER,model TEXT,agent TEXT);
            CREATE TABLE message(id TEXT PRIMARY KEY,session_id TEXT,time_created INTEGER,time_updated INTEGER,data TEXT);
            CREATE TABLE part(id TEXT PRIMARY KEY,message_id TEXT,session_id TEXT,time_created INTEGER,time_updated INTEGER,data TEXT);
            CREATE TABLE todo(session_id TEXT,content TEXT,status TEXT,priority TEXT,position INTEGER,time_created INTEGER,time_updated INTEGER,PRIMARY KEY(session_id,position));
            """
        )
        now = 1_767_312_000_000
        conn.execute("INSERT INTO project VALUES(?,?,?,?,?,?)", ("project-1", str(self.root / "repo"), "git", "fixture", now, now))
        conn.execute("INSERT INTO session VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (OPENCODE, "project-1", None, "root", str(self.root / "repo"), "OpenCode fixture", "1.0", now, now + 10, None, "model-fixture", "build"))
        conn.execute("INSERT INTO session VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (OPENCODE_CHILD, "project-1", OPENCODE, "child", str(self.root / "repo"), "OpenCode child", "1.0", now + 1, now + 9, None, "model-fixture", "explore"))
        user = {"role": "user", "time": {"created": now}, "agent": "build", "model": {"providerID": "fixture", "modelID": "model-fixture"}}
        assistant = {"role": "assistant", "parentID": "msg-user", "time": {"created": now + 2, "completed": now + 8}, "providerID": "fixture", "modelID": "model-fixture", "agent": "build", "tokens": {"input": 11, "output": 7, "reasoning": 3, "total": 18, "cache": {"read": 2, "write": 1}}}
        conn.execute("INSERT INTO message VALUES(?,?,?,?,?)", ("msg-user", OPENCODE, now, now, json.dumps(user)))
        conn.execute("INSERT INTO message VALUES(?,?,?,?,?)", ("msg-assistant", OPENCODE, now + 2, now + 8, json.dumps(assistant)))
        parts = [
            ("part-user", "msg-user", {"type": "text", "text": "do it"}),
            ("part-reason", "msg-assistant", {"type": "reasoning", "text": "visible reasoning", "metadata": {"anthropic": {"signature": "sig"}}}),
            ("part-tool", "msg-assistant", {"type": "tool", "tool": "bash", "callID": "tool_fixture", "state": {"status": "completed", "input": {"command": "pwd"}, "output": "ok", "time": {"start": now + 4, "end": now + 5}}}),
            ("part-text", "msg-assistant", {"type": "text", "text": "done"}),
        ]
        for offset, (part_id, message_id, payload) in enumerate(parts, 3):
            conn.execute("INSERT INTO part VALUES(?,?,?,?,?,?)", (part_id, message_id, OPENCODE, now + offset, now + offset, json.dumps(payload)))
        conn.execute("INSERT INTO todo VALUES(?,?,?,?,?,?,?)", (OPENCODE, "fixture todo", "completed", "high", 0, now, now + 7))
        conn.commit()
        conn.close()
        tool = self.opencode / "tool-output" / "tool_fixture"
        tool.parent.mkdir()
        tool.write_text("complete tool output", encoding="utf-8")

    def test_sync_normalizes_run_trees_usage_and_artifacts(self) -> None:
        conn, stats = self.sync()
        self.assertGreater(stats.records_added, 0)
        self.assertGreaterEqual(stats.warnings, 1)  # deliberately unknown Codex record
        codex_child = f"codex:fixture-host:{CHILD}:main"
        claude_child = f"claude:fixture-host:{CLAUDE}/agent1:agent:agent1"
        opencode_child = f"opencode:fixture-host:{OPENCODE_CHILD}:main"
        self.assertTrue(conn.execute("SELECT 1 FROM run_edges WHERE child_run_id=?", (codex_child,)).fetchone())
        self.assertTrue(conn.execute("SELECT 1 FROM run_edges WHERE child_run_id=?", (claude_child,)).fetchone())
        self.assertTrue(conn.execute("SELECT 1 FROM run_edges WHERE child_run_id=?", (opencode_child,)).fetchone())
        claude_usage = conn.execute("SELECT COUNT(*),SUM(reasoning_tokens),SUM(total_tokens) FROM usage_records WHERE provider='claude' AND scope='response'").fetchone()
        self.assertEqual(tuple(claude_usage), (2, 3, 19))
        codex_turns = conn.execute("SELECT input_tokens,reasoning_tokens,total_tokens FROM usage_records WHERE provider='codex' AND scope='turn' ORDER BY sequence").fetchall()
        self.assertEqual([tuple(row) for row in codex_turns], [(10, 3, 17), (9, 2, 15)])
        opencode_usage = conn.execute("SELECT input_tokens,output_tokens,reasoning_tokens,total_tokens FROM usage_records WHERE provider='opencode' AND scope='response'").fetchone()
        self.assertEqual(tuple(opencode_usage), (11, 7, 3, 18))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM artifacts WHERE kind='plan' AND run_id IS NOT NULL").fetchone()[0], 2)
        kinds = dict(conn.execute("SELECT item_type,kind FROM events WHERE item_type IN ('agent_reasoning','sub_agent_activity','mcp_tool_call_end','agent_message','compacted','mode','ai-title')"))
        self.assertEqual(kinds["agent_reasoning"], "reasoning")
        self.assertEqual(kinds["sub_agent_activity"], "status")
        self.assertEqual(kinds["mcp_tool_call_end"], "tool_result")
        self.assertEqual(kinds["agent_message"], "message")
        self.assertEqual(kinds["compacted"], "context")
        self.assertEqual(kinds["mode"], "context")
        self.assertEqual(kinds["ai-title"], "status")
        opencode_kinds = dict(conn.execute("SELECT item_type,kind FROM events WHERE run_id LIKE 'opencode:%' AND item_type IN ('reasoning','tool','todo') ORDER BY sub_index"))
        self.assertEqual(opencode_kinds["reasoning"], "reasoning")
        self.assertEqual(opencode_kinds["tool"], "tool_result")
        self.assertEqual(opencode_kinds["todo"], "status")
        linked_tool = conn.execute("SELECT run_id,turn_id,blob_sha256 FROM artifacts WHERE kind='tool_output'").fetchone()
        self.assertTrue(all(linked_tool))
        oversized = conn.execute("SELECT oversized,blob_sha256 FROM artifacts WHERE kind='tool_result_file'").fetchone()
        self.assertEqual(tuple(oversized), (1, None))
        data = status_data(conn)
        self.assertEqual(data["workloads"], 3)
        self.assertEqual(data["unresolved_children"], 0)
        conn.close()

    def test_incremental_partial_move_and_idempotence(self) -> None:
        conn, _ = self.sync()
        raw_before = conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0]
        events_before = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        conn.close()
        original = self.parent_rollout.read_bytes()
        rewritten = original.replace(b'"text":"fixture"', b'"text":"Fixture"', 1)
        self.assertEqual(len(rewritten), len(original))
        self.parent_rollout.write_bytes(rewritten)
        conn = open_database(self.vault)
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], raw_before + 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events_before)
        self.assertTrue(conn.execute("SELECT 1 FROM events WHERE payload_json LIKE '%Fixture%'").fetchone())
        conn.close()
        partial = {"timestamp": "2026-01-01T02:00:00Z", "type": "event_msg", "ordinal": 99, "payload": {"type": "thread_name_updated"}}
        with self.parent_rollout.open("ab") as handle:
            handle.write(json.dumps(partial, separators=(",", ":")).encode())
        conn = open_database(self.vault)
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], raw_before + 1)
        with self.parent_rollout.open("ab") as handle:
            handle.write(b"\n")
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], raw_before + 2)
        complete = self.parent_rollout.read_bytes()
        self.parent_rollout.write_bytes(complete.rsplit(b"\n", 2)[0] + b"\n")
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events_before)
        self.parent_rollout.write_bytes(complete)
        VaultSyncer(conn, self.options()).sync()
        self.assertGreater(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events_before)
        archived = self.codex / "archived_sessions" / self.parent_rollout.name
        archived.parent.mkdir()
        shutil.copy2(self.parent_rollout, archived)
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0], raw_before + 2)
        self.assertGreater(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events_before)
        conn.close()

    def test_exports_and_verification(self) -> None:
        conn, _ = self.sync()
        unified = self.root / "vault.jsonl"
        export_unified(conn, unified)
        record_types = {json.loads(value)["record_type"] for value in unified.read_text().splitlines()}
        self.assertTrue({"workload", "run", "raw_record", "event", "usage", "artifact"}.issubset(record_types))
        native = self.root / "native"
        export_native(conn, native)
        exported = list(native.rglob(self.parent_rollout.name))
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0].read_bytes(), self.parent_rollout.read_bytes())
        projected = list(native.rglob("opencode.db/session.jsonl"))
        self.assertEqual(len(projected), 1)
        projected_rows = [json.loads(value) for value in projected[0].read_text().splitlines()]
        self.assertEqual(len(projected_rows), 2)
        self.assertTrue(all(value["table"] == "session" for value in projected_rows))
        errors, warnings = verify(conn, self.vault)
        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])
        self.assertEqual(stat.S_IMODE(self.vault.stat().st_mode), 0o600)
        conn.close()

    def test_malformed_and_oversized_records_are_preserved_or_deferred(self) -> None:
        bad = self.codex / "sessions/2026/01/01" / "unknown.jsonl"
        bad.write_bytes(b"{bad json}\n" + b"x" * 80 + b"\n")
        conn = open_database(self.vault)
        options = self.options(max_record_bytes=32)
        stats = VaultSyncer(conn, options).sync()
        self.assertGreaterEqual(stats.warnings, 2)
        row = conn.execute("SELECT parsed,raw_zlib FROM raw_records r JOIN source_files f ON f.id=r.source_file_id WHERE f.relative_path LIKE '%unknown.jsonl' LIMIT 1").fetchone()
        self.assertEqual(row["parsed"], 0)
        source = conn.execute("SELECT checkpoint_line FROM source_files WHERE relative_path LIKE '%unknown.jsonl'").fetchone()
        self.assertEqual(source[0], 1)
        conn.close()

    def test_git_remote_normalization_strips_credentials(self) -> None:
        self.assertEqual(normalize_git_remote("git@GitHub.com:Owner/Repo.git"), "github.com/Owner/Repo")
        self.assertEqual(normalize_git_remote("https://token@example.com/org/repo.git"), "example.com/org/repo")

    def test_refuses_unrelated_sqlite_database(self) -> None:
        unrelated = self.root / "unrelated.sqlite"
        conn = sqlite3.connect(unrelated)
        conn.execute("CREATE TABLE user_data(value TEXT)")
        conn.commit()
        conn.close()
        with self.assertRaisesRegex(RuntimeError, "non-vault"):
            open_database(unrelated)
        check = sqlite3.connect(unrelated)
        self.assertEqual(check.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [("user_data",)])
        check.close()

    def test_migrates_v1_vault_for_opencode(self) -> None:
        legacy = self.root / "legacy.sqlite"
        conn = sqlite3.connect(legacy)
        conn.executescript(
            """
            CREATE TABLE vault_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO vault_meta VALUES('schema_version','1');
            CREATE TABLE source_instances(
                id INTEGER PRIMARY KEY,
                provider TEXT NOT NULL CHECK(provider IN ('codex','claude')),
                host_id TEXT NOT NULL,root_path TEXT NOT NULL,surface TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL,
                UNIQUE(provider,host_id,root_path,surface)
            );
            """
        )
        conn.commit()
        conn.close()
        migrated = open_database(legacy)
        self.assertEqual(migrated.execute("SELECT value FROM vault_meta WHERE key='schema_version'").fetchone()[0], "2")
        migrated.execute("INSERT INTO source_instances(provider,host_id,root_path,surface,first_seen_at,last_seen_at) VALUES('opencode','host','root','sqlite','now','now')")
        migrated.commit()
        migrated.close()


if __name__ == "__main__":
    unittest.main()
