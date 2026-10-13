from __future__ import annotations

import base64
import json
import shutil
import sqlite3
import stat
import tempfile
import unittest
import os
from unittest.mock import patch
from pathlib import Path

from cairn.db import open_database
from cairn.ingest import SyncOptions, VaultSyncer, normalize_git_remote
from cairn.reporting import export_native, export_unified, status_data, verify


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
        # Resolved so macOS /var -> /private/var matches the vault's resolved roots.
        self.root = Path(self.temp.name).resolve()
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
        self.doCleanups()
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

    def _make_opencode_v2(self) -> Path:
        root = self.root / "opencode-v2"
        root.mkdir()
        conn = sqlite3.connect(root / "opencode.db")
        conn.executescript(
            """
            CREATE TABLE project(id TEXT PRIMARY KEY,worktree TEXT,vcs TEXT,name TEXT,time_created INTEGER,time_updated INTEGER,sandboxes TEXT);
            CREATE TABLE session_v2(id TEXT PRIMARY KEY,project_id TEXT,parent_id TEXT,fork_session_id TEXT,fork_boundary TEXT,slug TEXT,directory TEXT,title TEXT,version TEXT,metadata TEXT,agent TEXT,model TEXT,time_created INTEGER,time_updated INTEGER,time_archived INTEGER);
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,time_created INTEGER,time_updated INTEGER,data TEXT);
            CREATE TABLE session_inbox(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,payload TEXT,delivery TEXT,enqueued_seq INTEGER,time_created INTEGER);
            CREATE TABLE instruction_blob(hash TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE instruction_state(session_id TEXT PRIMARY KEY,epoch_start INTEGER,through_seq INTEGER,initial_values TEXT,current_values TEXT);
            CREATE TABLE kv(key TEXT PRIMARY KEY,value TEXT,time_created INTEGER,time_updated INTEGER);
            CREATE TABLE credential(id TEXT PRIMARY KEY,label TEXT,value TEXT,time_created INTEGER,time_updated INTEGER);
            CREATE TABLE event(id TEXT PRIMARY KEY,data TEXT);
            """
        )
        now = 1_791_000_000_000
        model = json.dumps({"id": "model-v2", "providerID": "fixture", "variant": "high"})
        conn.execute("INSERT INTO project VALUES(?,?,?,?,?,?,?)", ("proj-v2", str(self.root / "repo"), "git", "fixture", now, now, "[]"))
        sessions = [
            ("ses_v2_root", None, None, "Root V2", '{"openchamber":{"delivered":"on"}}'),
            ("ses_v2_child", "ses_v2_root", None, "Child V2", None),
            ("ses_v2_fork", None, "ses_v2_root", "Fork V2", None),
        ]
        for offset, (sid, parent, fork, title, metadata) in enumerate(sessions):
            conn.execute(
                "INSERT INTO session_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, "proj-v2", parent, fork, "msg_boundary" if fork else None, sid, str(self.root / "repo"), title, "2.0", metadata, "build", model, now + offset, now + offset, None),
            )
        tool = {
            "type": "tool", "id": "call_v2", "name": "webfetch",
            "state": {"status": "completed", "input": {"url": "https://example.com"}, "content": [{"type": "text", "text": "ok"}],
                      "metadata": {"outputPath": str(root / "tool-output" / "tool_v2"), "shellID": "sh_v2"}},
        }
        messages = [
            ("synthetic", {"time": {"created": now + 1}, "text": "reminder"}),
            ("user", {"time": {"created": now + 2}, "text": "do it", "files": [{"path": "a.png"}]}),
            ("assistant", {"time": {"created": now + 3}, "model": {"id": "model-v2"}, "content": [
                {"type": "reasoning", "text": "thinking"}, {"type": "text", "text": "working"}, tool,
            ]}),
            ("assistant", {"time": {"created": now + 4, "completed": now + 5}, "model": {"id": "model-v2"}, "content": [{"type": "text", "text": "done"}],
                           "tokens": {"input": 11, "output": 7, "reasoning": 3, "cache": {"read": 2, "write": 1}}, "cost": 0.1}),
            ("idle", {"time": {"created": now + 6}, "outcome": "succeeded"}),
            ("model-switched", {"time": {"created": now + 7}, "model": {"id": "other"}}),
        ]
        for seq, (kind, data) in enumerate(messages, 1):
            conn.execute("INSERT INTO session_message VALUES(?,?,?,?,?,?,?)", (f"msg_v2_{seq}", "ses_v2_root", kind, seq, now + seq, now + seq, json.dumps(data)))
        conn.execute("INSERT INTO session_inbox VALUES(?,?,?,?,?,?,?)", ("inbox_v2", "ses_v2_root", "synthetic", '{"text":"steer"}', "steer", 9, now + 9))
        conn.execute("INSERT INTO instruction_blob VALUES(?,?)", ("hash-1", '"system prompt"'))
        conn.execute("INSERT INTO instruction_state VALUES(?,?,?,?,?)", ("ses_v2_root", 1, 6, '{"core":"hash-1"}', '{"core":"hash-1"}'))
        job = {"id": "sh_v2", "recovery": {"kind": "shell", "sessionID": "ses_v2_root", "shellID": "sh_v2"}, "status": "running"}
        conn.execute("INSERT INTO kv VALUES(?,?,?,?)", ("job.background/msg_v2_3", json.dumps(job), now, now))
        conn.execute("INSERT INTO kv VALUES(?,?,?,?)", ("models-dev:catalog", '{"huge":"cache"}', now, now))
        conn.execute("INSERT INTO credential VALUES(?,?,?,?,?)", ("cred", "token", "secret-value", now, now))
        conn.commit()
        conn.close()
        (root / "tool-output").mkdir()
        (root / "tool-output" / "tool_v2").write_text("full output", encoding="utf-8")
        (root / "shell" / "proj-v2").mkdir(parents=True)
        (root / "shell" / "proj-v2" / "sh_v2.out").write_text("shell output", encoding="utf-8")
        (root / "shell" / "proj-v2" / "sh_unlinked.out").write_text("foreground", encoding="utf-8")
        return root

    def test_opencode_v2_sessions_turns_blocks_and_files(self) -> None:
        root = self._make_opencode_v2()
        conn = open_database(self.vault)
        self.addCleanup(conn.close)
        options = self.options(codex_home=None, claude_home=None, claude_app_support=None, opencode_data=root, artifact_max_bytes=1024)
        VaultSyncer(conn, options).sync()
        run = lambda sid: f"opencode:fixture-host:{sid}:main"
        root_run = conn.execute("SELECT * FROM runs WHERE id=?", (run("ses_v2_root"),)).fetchone()
        self.assertEqual((root_run["title"], root_run["model"], root_run["reasoning_effort"], root_run["surface"]), ("Root V2", "model-v2", "high", "OpenChamber"))
        edges = {(row[0], row[1]) for row in conn.execute("SELECT child_run_id,relation FROM run_edges")}
        self.assertEqual(edges, {(run("ses_v2_child"), "spawn"), (run("ses_v2_fork"), "fork")})
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM workloads").fetchone()[0], 1)
        turns = conn.execute("SELECT vendor_turn_id,status,sequence FROM turns WHERE run_id=?", (run("ses_v2_root"),)).fetchall()
        self.assertEqual([tuple(row) for row in turns], [("msg_v2_2", "completed", 2)])
        kinds = [tuple(row) for row in conn.execute(
            "SELECT kind,item_type,turn_id IS NOT NULL FROM events WHERE run_id=? AND item_type<>'session' ORDER BY sequence,sub_index",
            (run("ses_v2_root"),),
        )]
        self.assertIn(("instruction", "synthetic", 0), kinds)
        self.assertIn(("attachment", "file", 1), kinds)
        for expected in ("reasoning", "tool_call", "tool_result"):
            self.assertIn(expected, {kind for kind, _, linked in kinds if linked})
        self.assertIn(("status", "session_inbox:synthetic", 0), kinds)
        self.assertIn(("instruction", "instruction_state", 0), kinds)
        self.assertIn(("status", "background_job", 0), kinds)
        usage = conn.execute("SELECT input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,turn_id FROM usage_records WHERE provider='opencode'").fetchall()
        self.assertEqual([tuple(row)[:4] for row in usage], [(11, 2, 7, 3)])
        self.assertTrue(usage[0]["turn_id"])
        files = {row[0]: (row[1], row[2]) for row in conn.execute("SELECT relative_path,run_id,turn_id FROM artifacts")}
        self.assertEqual(files["tool-output/tool_v2"][0], run("ses_v2_root"))
        self.assertEqual(files["shell/proj-v2/sh_v2.out"][0], run("ses_v2_root"))
        self.assertTrue(files["tool-output/tool_v2"][1])
        self.assertEqual(files["shell/proj-v2/sh_unlinked.out"], (None, None))
        tables = {row[0] for row in conn.execute("SELECT DISTINCT native_type FROM raw_records")}
        self.assertFalse(tables & {"credential", "event"})
        self.assertFalse(conn.execute("SELECT 1 FROM raw_records WHERE logical_key LIKE '%models-dev%'").fetchone())
        before = conn.execute("SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM turns),(SELECT COUNT(*) FROM usage_records)").fetchone()
        # A vault normalized by an older reader is rebuilt from its archived rows.
        conn.execute("UPDATE vault_meta SET value='1' WHERE key='opencode_normalizer'")
        conn.execute("INSERT INTO turns(id,run_id,vendor_turn_id) VALUES('stale',?,'msg_v2_3')", (run("ses_v2_root"),))
        conn.commit()
        VaultSyncer(conn, options).sync()
        after = conn.execute("SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM turns),(SELECT COUNT(*) FROM usage_records)").fetchone()
        self.assertEqual(tuple(after), tuple(before))
        self.assertEqual(verify(conn, self.vault)[0], [])

    def test_openchamber_run_and_project_files_without_secrets(self) -> None:
        opencode = self._make_opencode_v2()
        db = sqlite3.connect(opencode / "opencode.db")
        created = {"type": "tool", "id": "call_create", "name": "openchamber", "state": {
            "status": "completed", "input": {"action": "session.create", "parameters": {"title": "worker"}},
            "content": [{"type": "text", "text": json.dumps({"ok": True, "action": "session.create", "data": {"sessionId": "ses_v2_worker"}})}],
        }}
        db.execute("INSERT INTO session_v2(id,project_id,slug,directory,title,version,model,time_created,time_updated) VALUES('ses_v2_worker','proj-v2','w',?,'Worker','2.0','{}',1,1)", (str(self.root / "repo"),))
        db.execute("INSERT INTO session_message VALUES('msg_v2_create','ses_v2_root','assistant',8,8,8,?)", (json.dumps({"content": [created]}),))
        chat = self.root / "openchamber/chats/2026-10-07/session-abc"
        db.execute("INSERT INTO session_v2(id,project_id,slug,directory,title,version,model,time_created,time_updated) VALUES('ses_v2_chat','proj-v2','c',?,'Chat','2.0','{}',1,1)", (str(chat),))
        db.commit()
        db.close()
        data = self.root / "openchamber"
        repo = self.root / "repo"
        files = {
            data / "goals/ses_v2_root.md": b"Ship the fixture",
            data / "dispatch-results.json": b'{"version":1,"entries":[]}',
            data / "sessions-archive.json": b'{"ses_v2_child":1791000000000,"ses_v2_fork":null}',
            data / "sessions-directories.json": b'{"version":1,"foldersMap":{}}',
            data / "projects" / ("path_" + base64.urlsafe_b64encode(str(repo).encode()).decode().rstrip("=") + ".json"): b'{"version":1,"scheduledTasks":[]}',
            data / "projects/path_x/context.json": b'{"version":2,"notes":[]}',
            data / "projects/path_x/plans/1-plan.md": b"# Personal plan",
            data / "guest-storage/agent-canvas.json": b'{"draft":{"notes":"n"}}',
            chat / "notes.txt": b"chat output",
            repo / ".openchamber/project.json": b'{"version":1,"plansDir":"docs/plans"}',
            repo / ".openchamber/screenshots/shot.jpg": b"jpeg",
            repo / "docs/plans/shared-plan.md": b"# Shared plan",
            repo / ".agents/loops/nightly.md": b"---\nschedule: daily\n---\nrun it",
        }
        secrets = {
            data / "settings.json": json.dumps({"projects": [{"path": str(repo)}], "desktopUiPassword": "hunter2"}).encode(),
            data / "jwt-secret": b"secret",
            data / "environment.json": b'{"TOKEN":"secret"}',
            data / "projects/path_x.json.lock": b"lock",
            chat / "node_modules/pkg/index.js": b"vendored",
            repo / ".openchamber/npm-global/bin/tool": b"binary",
            repo / "docs/plans/../notes.md": b"outside",
        }
        for path, content in {**files, **secrets}.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        conn = open_database(self.vault)
        self.addCleanup(conn.close)
        options = self.options(codex_home=None, claude_home=None, claude_app_support=None, opencode_data=opencode,
                               openchamber_data=data, scan_project_dirs=True, artifact_max_bytes=1024)
        self.assertEqual(VaultSyncer(conn, options).sync().warnings, 0)
        rows = conn.execute("SELECT a.kind,a.run_id,s.root_path,a.relative_path,b.data FROM artifacts a JOIN source_instances s ON s.id=a.source_instance_id LEFT JOIN blobs b ON b.sha256=a.blob_sha256").fetchall()
        archived = {Path(row["root_path"]) / row["relative_path"]: row for row in rows}
        for path, content in files.items():
            self.assertIn(path, archived)
            self.assertEqual(archived[path]["data"], content)
        self.assertFalse(set(secrets) & set(archived))
        self.assertFalse(conn.execute("SELECT 1 FROM blobs WHERE data LIKE '%hunter2%' OR data LIKE '%secret%'").fetchone())
        run = lambda sid: f"opencode:fixture-host:{sid}:main"
        self.assertEqual(archived[data / "goals/ses_v2_root.md"]["run_id"], run("ses_v2_root"))
        self.assertEqual(archived[chat / "notes.txt"]["run_id"], run("ses_v2_chat"))
        kinds = {path.name: row["kind"] for path, row in archived.items()}
        self.assertEqual((kinds["shared-plan.md"], kinds["1-plan.md"], kinds["shot.jpg"], kinds["nightly.md"]), ("plan", "plan", "screenshot", "loop"))
        self.assertEqual(conn.execute("SELECT relation FROM run_edges WHERE child_run_id=?", (run("ses_v2_worker"),)).fetchone()[0], "dispatch")
        self.assertEqual(conn.execute("SELECT status FROM runs WHERE id=?", (run("ses_v2_child"),)).fetchone()[0], "archived")
        self.assertEqual(conn.execute("SELECT status FROM runs WHERE id=?", (run("ses_v2_fork"),)).fetchone()[0], "active")
        # Project folders are only scanned for this machine's own histories.
        imported = self.root / "imported.sqlite"
        other = open_database(imported)
        self.addCleanup(other.close)
        VaultSyncer(other, self.options(db_path=imported, codex_home=None, claude_home=None, claude_app_support=None,
                                        opencode_data=None, openchamber_data=data)).sync()
        self.assertFalse(other.execute("SELECT 1 FROM artifacts WHERE kind IN ('screenshot','loop')").fetchone())
        self.assertEqual(verify(conn, self.vault)[0], [])

    def test_unreadable_task_directory_preserves_sibling_artifacts(self) -> None:
        empty = self.claude / "tasks/session-empty"
        empty.mkdir(parents=True)
        (empty / "unreadable.json").write_bytes(b'{}')
        good = self.claude / "tasks/session-good/1.json"
        good.parent.mkdir()
        good.write_bytes(b'{"subject":"keep me"}')
        scandir = os.scandir

        def scan(path):
            if Path(path) == empty:
                error = FileNotFoundError(2, "The system cannot find the file specified", str(path))
                error.winerror = 2
                raise error
            return scandir(path)

        with patch("cairn.ingest.os.scandir", side_effect=scan):
            conn, stats = self.sync()
        self.addCleanup(conn.close)
        self.assertGreater(stats.records_added, 0)
        self.assertTrue(conn.execute("SELECT 1 FROM artifacts WHERE relative_path LIKE '%session-good%'").fetchone())
        warnings = conn.execute("SELECT message FROM ingest_errors WHERE code='discovery_unreadable'").fetchall()
        self.assertEqual(len(warnings), 1)
        self.assertIn(str(empty), warnings[0][0])
        self.assertEqual(conn.execute("SELECT status FROM sync_runs ORDER BY id DESC LIMIT 1").fetchone()[0], "warnings")
        with patch("cairn.ingest.os.scandir", side_effect=scan):
            repeated = VaultSyncer(conn, self.options()).sync()
        self.assertGreater(repeated.warnings, 0)
        VaultSyncer(conn, self.options()).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM ingest_errors WHERE code='discovery_unreadable'").fetchone()[0], 1)

    def test_failed_recursive_scan_keeps_checkpoints_and_discovers_siblings(self) -> None:
        conn, _ = self.sync()
        self.addCleanup(conn.close)
        before = conn.execute("SELECT checkpoint_offset FROM source_files WHERE relative_path LIKE ?", ("%" + self.parent_rollout.name,)).fetchone()[0]
        good = self.codex / "sessions/other/unknown.jsonl"
        write_lines(good, [{"type": "fixture"}])
        scandir = os.scandir

        def scan(path):
            if Path(path) == self.parent_rollout.parent:
                raise PermissionError(13, "Access denied", str(path))
            return scandir(path)

        with patch("cairn.ingest.os.scandir", side_effect=scan):
            stats = VaultSyncer(conn, self.options()).sync()
        self.assertGreater(stats.warnings, 0)
        row = conn.execute("SELECT checkpoint_offset,state FROM source_files WHERE relative_path LIKE ?", ("%" + self.parent_rollout.name,)).fetchone()
        self.assertEqual(tuple(row), (before, "present"))
        self.assertTrue(conn.execute("SELECT 1 FROM source_files WHERE relative_path LIKE '%unknown.jsonl'").fetchone())
        self.assertEqual(conn.execute("PRAGMA quick_check").fetchone()[0], "ok")

    def test_empty_optional_directories_are_accepted(self) -> None:
        for relative in ("tasks/session-empty", "teams/empty", "plans", "projects/empty/memories"):
            (self.claude / relative).mkdir(parents=True, exist_ok=True)
        scandir = os.scandir

        def scan(path):
            if Path(path).is_dir() and not os.listdir(path):
                error = FileNotFoundError(2, "The system cannot find the file specified", str(path))
                error.winerror = 2
                raise error
            return scandir(path)

        with patch("cairn.ingest.os.scandir", side_effect=scan):
            conn, stats = self.sync()
        self.addCleanup(conn.close)
        self.assertGreater(stats.records_added, 0)
        self.assertFalse(conn.execute("SELECT 1 FROM ingest_errors WHERE code='discovery_unreadable'").fetchone())

    def test_sqlite_snapshot_preserves_wal_and_ignores_copied_shm(self) -> None:
        live = self.root / "live.sqlite"
        writer = sqlite3.connect(live)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE fixture(value TEXT)")
        writer.execute("INSERT INTO fixture VALUES('only in WAL')")
        writer.commit()
        source = self.root / "closed-backup.sqlite"
        source.write_bytes(live.read_bytes())
        wal = Path(f"{source}-wal")
        wal.write_bytes(Path(f"{live}-wal").read_bytes())
        writer.close()
        shm = Path(f"{source}-shm")
        shm.write_bytes(b"bad")
        original = (source.read_bytes(), wal.read_bytes(), shm.read_bytes())
        conn = open_database(self.vault)
        self.addCleanup(conn.close)
        syncer = VaultSyncer(conn, self.options(snapshot_source_sqlite=True))
        with syncer._readonly_sqlite(source) as snapshot:
            self.assertEqual(snapshot.execute("SELECT value FROM fixture").fetchone()[0], "only in WAL")
            copied = Path(snapshot.execute("PRAGMA database_list").fetchone()[2])
            self.assertNotEqual(copied, source)
            self.assertTrue(copied.exists())
        self.assertFalse(copied.parent.exists())
        self.assertEqual((source.read_bytes(), wal.read_bytes(), shm.read_bytes()), original)

    def test_project_memories_are_versioned_exportable_context(self) -> None:
        project = self.claude / "projects/fixture-project"
        expected = {
            project / "memory/MEMORY.md": b"Shared project context\n",
            project / "memory/nested/notes.jsonl": b'{"memory":"keep raw"}\n',
            project / "memories/design.txt": b"Design decision\n",
            self.codex / "memories/memory_summary.md": b"Codex project summary\n",
        }
        excluded = {
            self.claude / "skills/custom/SKILL.md",
            self.claude / "settings.json",
            self.codex / "rules/custom.rules",
            self.codex / "hooks.json",
            self.codex / "automations/example/automation.toml",
        }
        for path, data in expected.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        for path in excluded:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not memory")
        conn = open_database(self.vault)
        self.addCleanup(conn.close)
        options = self.options(artifact_max_bytes=1024 * 1024)
        VaultSyncer(conn, options).sync()
        rows = conn.execute("SELECT a.*,s.root_path,b.data FROM artifacts a JOIN source_instances s ON s.id=a.source_instance_id JOIN blobs b ON b.sha256=a.blob_sha256 WHERE a.kind IN ('project_memory','memory_file')").fetchall()
        self.assertEqual(len(rows), len(expected))
        for row in rows:
            source = Path(row["root_path"]) / row["relative_path"]
            self.assertEqual(row["data"], expected[source])
            self.assertIsNone(row["run_id"])
            self.assertIsNone(row["turn_id"])
        self.assertFalse(conn.execute("SELECT 1 FROM source_files WHERE relative_path LIKE '%notes.jsonl'").fetchone())
        all_paths = {Path(row["root_path"]) / row["relative_path"] for row in conn.execute("SELECT a.relative_path,s.root_path FROM artifacts a JOIN source_instances s ON s.id=a.source_instance_id")}
        self.assertFalse(all_paths & excluded)
        VaultSyncer(conn, options).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM artifacts WHERE kind IN ('project_memory','memory_file')").fetchone()[0], len(expected))
        memory = project / "memory/MEMORY.md"
        memory.write_bytes(b"Updated context\n")
        VaultSyncer(conn, options).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM artifacts WHERE kind='project_memory'").fetchone()[0], 4)
        output = self.root / "memory-export"
        export_native(conn, output)
        recovered = {p.read_bytes() for p in output.rglob("MEMORY.md")}
        self.assertEqual(recovered, {b"Shared project context\n", b"Updated context\n"})
        self.assertEqual(verify(conn, self.vault)[0], [])

    def test_codex_memory_database_snapshot_contains_wal_and_all_tables(self) -> None:
        live = self.root / "live-memories.sqlite"
        writer = sqlite3.connect(live)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE stage1_outputs(thread_id TEXT PRIMARY KEY, raw_memory TEXT)")
        writer.execute("INSERT INTO stage1_outputs VALUES('thread-1','Memory in WAL')")
        writer.execute("CREATE TABLE future_memory_schema(content TEXT)")
        writer.execute("INSERT INTO future_memory_schema VALUES('Future context')")
        writer.commit()
        source = self.codex / "memories_1.sqlite"
        source.write_bytes(live.read_bytes())
        wal = Path(f"{source}-wal")
        wal.write_bytes(Path(f"{live}-wal").read_bytes())
        writer.close()
        shm = Path(f"{source}-shm")
        shm.write_bytes(b"bad")
        original = (source.read_bytes(), wal.read_bytes(), shm.read_bytes())
        conn = open_database(self.vault)
        self.addCleanup(conn.close)
        options = self.options(snapshot_source_sqlite=True, artifact_max_bytes=1024 * 1024)
        VaultSyncer(conn, options).sync()
        row = conn.execute("SELECT a.relative_path,a.run_id,b.data FROM artifacts a JOIN blobs b ON b.sha256=a.blob_sha256 WHERE a.kind='memory_database'").fetchone()
        self.assertEqual(row["relative_path"], source.name)
        self.assertIsNone(row["run_id"])
        recovered = self.root / "recovered-memory.sqlite"
        recovered.write_bytes(row["data"])
        restored = sqlite3.connect(recovered.as_uri() + "?mode=ro", uri=True)
        try:
            self.assertEqual(restored.execute("SELECT raw_memory FROM stage1_outputs").fetchone()[0], "Memory in WAL")
            self.assertEqual(restored.execute("SELECT content FROM future_memory_schema").fetchone()[0], "Future context")
            self.assertEqual(restored.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(restored.execute("PRAGMA quick_check").fetchone()[0], "ok")
        finally:
            restored.close()
        VaultSyncer(conn, options).sync()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM artifacts WHERE kind='memory_database'").fetchone()[0], 1)
        self.assertEqual((source.read_bytes(), wal.read_bytes(), shm.read_bytes()), original)
        output = self.root / "memory-db-export"
        export_native(conn, output)
        exported = list(output.rglob("memories_1.sqlite"))
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0].read_bytes(), row["data"])

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
        if os.name != "nt":
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
