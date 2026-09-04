from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import zlib
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit

from .db import json_text, stable_id, utc_now

DEFAULT_ARTIFACT_MAX = 64 * 1024 * 1024
DEFAULT_RECORD_MAX = 256 * 1024 * 1024
PROBE_BYTES = 4096
CODEX_RUN_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f-]{27})\.jsonl$", re.I)


@dataclass(frozen=True)
class SyncOptions:
    db_path: Path
    host_id: str = field(default_factory=socket.gethostname)
    codex_home: Path | None = None
    claude_home: Path | None = None
    claude_app_support: Path | None = None
    opencode_data: Path | None = None
    artifact_max_bytes: int = DEFAULT_ARTIFACT_MAX
    max_record_bytes: int = DEFAULT_RECORD_MAX
    ignore_space_check: bool = False


@dataclass
class SyncStats:
    files_seen: int = 0
    records_added: int = 0
    events_added: int = 0
    artifacts_added: int = 0
    warnings: int = 0


@dataclass(frozen=True)
class JsonlSource:
    provider: str
    root: Path
    path: Path
    kind: str
    logical_key: str
    vendor_run_id: str | None
    stream_key: str
    parent_vendor_id: str | None = None
    agent_id: str | None = None


@dataclass
class FileState:
    run_id: str | None
    current_turn_id: str | None = None


class VaultSyncer:
    def __init__(self, conn: sqlite3.Connection, options: SyncOptions):
        self.conn = conn
        self.options = options
        self.stats = SyncStats()
        self.sync_id = 0
        self.source_ids: dict[tuple[str, str, str], int] = {}
        self.project_cache: dict[tuple[str, str | None], str | None] = {}
        self.plan_links: dict[str, str] = {}
        self.referenced_uploads: set[Path] = set()

    def sync(self) -> SyncStats:
        started = utc_now()
        cur = self.conn.execute(
            "INSERT INTO sync_runs(started_at, host_id) VALUES(?, ?)",
            (started, self.options.host_id),
        )
        self.sync_id = int(cur.lastrowid)
        self.conn.commit()
        status = "ok"
        try:
            sources = list(self.discover_jsonl())
            artifact_candidates = list(self.discover_artifacts())
            self._space_check(sources, artifact_candidates)
            for source in sources:
                self.ingest_jsonl(source)
            self.ingest_codex_databases()
            self.ingest_opencode_databases()
            self.ingest_desktop_metadata()
            # Desktop metadata supplies authoritative plan-to-run links.
            for provider, root, path, kind, run_id, turn_id in self.discover_artifacts():
                self.ingest_artifact(provider, root, path, kind, run_id, turn_id)
            self.ingest_referenced_uploads()
            self._mark_missing_files()
            if self.options.codex_home is not None:
                self._recompute_all_codex_usage()
            self._resolve_workloads()
            if self.stats.warnings:
                status = "warnings"
        except Exception:
            self.conn.rollback()
            status = "failed"
            raise
        finally:
            self.conn.execute(
                """UPDATE sync_runs
                      SET finished_at=?, files_seen=?, records_added=?, events_added=?,
                          artifacts_added=?, warnings=?, status=?
                    WHERE id=?""",
                (
                    utc_now(),
                    self.stats.files_seen,
                    self.stats.records_added,
                    self.stats.events_added,
                    self.stats.artifacts_added,
                    self.stats.warnings,
                    status,
                    self.sync_id,
                ),
            )
            self.conn.commit()
            for suffix in ("", "-wal", "-shm"):
                candidate = Path(f"{self.options.db_path}{suffix}")
                if candidate.exists():
                    os.chmod(candidate, 0o600)
        return self.stats

    def discover_jsonl(self) -> Iterable[JsonlSource]:
        codex = self.options.codex_home
        if codex and codex.is_dir():
            for base, kind in ((codex / "sessions", "rollout"), (codex / "archived_sessions", "rollout")):
                if not base.is_dir():
                    continue
                for path in sorted(base.rglob("*.jsonl")):
                    match = CODEX_RUN_RE.search(path.name)
                    if not match:
                        yield JsonlSource("codex", codex, path, "unknown_jsonl", f"file:{path.relative_to(codex)}", None, "unknown")
                        continue
                    vendor_id = match.group(1)
                    yield JsonlSource("codex", codex, path, kind, f"run:{vendor_id}", vendor_id, "main")
            index = codex / "session_index.jsonl"
            if index.is_file():
                yield JsonlSource("codex", codex, index, "session_index", "session_index", None, "index")

        claude = self.options.claude_home
        projects = claude / "projects" if claude else None
        if projects and projects.is_dir():
            for path in sorted(projects.rglob("*.jsonl")):
                parts = path.relative_to(projects).parts
                if "subagents" in parts and path.name.startswith("agent-"):
                    parent_id = path.parent.parent.name
                    agent_id = path.stem.removeprefix("agent-")
                    yield JsonlSource(
                        "claude", claude, path, "subagent", f"agent:{parent_id}:{agent_id}",
                        f"{parent_id}/{agent_id}", f"agent:{agent_id}", parent_id, agent_id,
                    )
                else:
                    vendor_id = path.stem
                    yield JsonlSource("claude", claude, path, "transcript", f"main:{vendor_id}", vendor_id, "main")

    def discover_artifacts(self) -> Iterable[tuple[str, Path, Path, str, str | None, str | None]]:
        codex = self.options.codex_home
        if codex and codex.is_dir():
            plans = codex / "plans"
            if plans.is_dir():
                for path in sorted(plans.glob("*/*/PLAN.md")):
                    relative = path.relative_to(plans)
                    vendor_id, vendor_turn = relative.parts[:2]
                    run_id = self.run_id("codex", vendor_id, "main")
                    turn_id = self.turn_id(run_id, vendor_turn)
                    yield "codex", codex, path, "plan", run_id, turn_id

        opencode = self.options.opencode_data
        tool_output = opencode / "tool-output" if opencode else None
        if tool_output and tool_output.is_dir():
            for path in sorted(p for p in tool_output.iterdir() if p.is_file() or p.is_symlink()):
                linked = self.conn.execute(
                    """SELECT run_id,turn_id FROM events
                         WHERE call_id=? AND run_id IS NOT NULL
                         ORDER BY sequence DESC LIMIT 1""",
                    (path.name,),
                ).fetchone()
                yield (
                    "opencode", opencode, path, "tool_output",
                    str(linked[0]) if linked else None,
                    str(linked[1]) if linked and linked[1] else None,
                )

        claude = self.options.claude_home
        if not claude or not claude.is_dir():
            return
        plans = claude / "plans"
        if plans.is_dir():
            for path in sorted(plans.glob("*.md")):
                run_id = self.plan_links.get(str(path.resolve()))
                yield "claude", claude, path, "plan", run_id, None
        tasks = claude / "tasks"
        if tasks.is_dir():
            for path in sorted(tasks.glob("*/*.json")):
                vendor_id = path.parent.name
                run_id = self.existing_run_id("claude", vendor_id, "main")
                yield "claude", claude, path, "task", run_id, None
        teams = claude / "teams"
        if teams.is_dir():
            for path in sorted(p for p in teams.rglob("*") if p.is_file() and not p.name.startswith(".")):
                run_id = self._artifact_session_link(path)
                yield "claude", claude, path, "team", run_id, None
        projects = claude / "projects"
        if projects.is_dir():
            for path in sorted(projects.rglob("*")):
                if not path.is_file() or path.suffix == ".jsonl":
                    continue
                parts = path.relative_to(projects).parts
                if "tool-results" in parts:
                    session_id = parts[parts.index("tool-results") - 1]
                    yield "claude", claude, path, "tool_result_file", self.existing_run_id("claude", session_id, "main"), None
                elif path.name.endswith(".meta.json") and "subagents" in parts:
                    session_id = parts[parts.index("subagents") - 1]
                    agent_id = path.name.removeprefix("agent-").removesuffix(".meta.json")
                    yield "claude", claude, path, "subagent_meta", self.existing_run_id("claude", f"{session_id}/{agent_id}", f"agent:{agent_id}"), None
                elif path.name == "bridge-pointer.json":
                    yield "claude", claude, path, "bridge_pointer", self._artifact_session_link(path), None

    def _space_check(self, sources: list[JsonlSource], artifacts: list[tuple[str, Path, Path, str, str | None, str | None]]) -> None:
        if self.options.ignore_space_check or self.conn.execute("SELECT 1 FROM raw_records LIMIT 1").fetchone():
            return
        raw = sum(_safe_size(source.path) for source in sources)
        if self.options.opencode_data:
            raw += sum(_safe_size(path) for path in self.options.opencode_data.glob("opencode*.db"))
        stored_artifacts = sum(min(_safe_size(item[2]), self.options.artifact_max_bytes) for item in artifacts)
        required = int((raw + stored_artifacts) * 1.2)
        free = shutil.disk_usage(self.options.db_path.parent).free
        if required > free:
            raise RuntimeError(
                f"insufficient free space: need approximately {required} bytes, have {free}; "
                "free space or pass --ignore-space-check"
            )

    def source_instance(self, provider: str, root: Path, surface: str) -> int:
        root = root.expanduser().resolve()
        key = (provider, str(root), surface)
        if key in self.source_ids:
            return self.source_ids[key]
        now = utc_now()
        self.conn.execute(
            """INSERT INTO source_instances(provider,host_id,root_path,surface,first_seen_at,last_seen_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(provider,host_id,root_path,surface)
               DO UPDATE SET last_seen_at=excluded.last_seen_at""",
            (provider, self.options.host_id, str(root), surface, now, now),
        )
        row = self.conn.execute(
            "SELECT id FROM source_instances WHERE provider=? AND host_id=? AND root_path=? AND surface=?",
            (provider, self.options.host_id, str(root), surface),
        ).fetchone()
        assert row
        self.source_ids[key] = int(row[0])
        return int(row[0])

    def run_id(self, provider: str, vendor_id: str, stream_key: str) -> str:
        return f"{provider}:{self.options.host_id}:{vendor_id}:{stream_key}"

    @staticmethod
    def turn_id(run_id: str, vendor_turn_id: str) -> str:
        return f"{run_id}:turn:{vendor_turn_id}"

    def existing_run_id(self, provider: str, vendor_id: str, stream_key: str, *, parent_vendor_id: str | None = None) -> str | None:
        if parent_vendor_id and stream_key.startswith("agent:"):
            candidate = self.run_id(provider, vendor_id, stream_key)
        else:
            candidate = self.run_id(provider, vendor_id, stream_key)
        return candidate if self.conn.execute("SELECT 1 FROM runs WHERE id=?", (candidate,)).fetchone() else None

    def ensure_run(
        self,
        provider: str,
        vendor_id: str,
        stream_key: str,
        *,
        parent_vendor_id: str | None = None,
        surface: str | None = None,
        cwd: str | None = None,
        title: str | None = None,
        started_at: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        run_id = self.run_id(provider, vendor_id, stream_key)
        project_id = self.ensure_project(cwd) if cwd else None
        if parent_vendor_id:
            parent_id = self.ensure_run(provider, parent_vendor_id, "main")
            parent = self.conn.execute("SELECT workload_id FROM runs WHERE id=?", (parent_id,)).fetchone()
            workload_id = str(parent[0]) if parent and parent[0] else f"workload:{parent_id}"
        else:
            workload_id = f"workload:{run_id}"
        self.conn.execute(
            """INSERT INTO workloads(id,provider,host_id,root_run_id,project_id,title,started_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING""",
            (workload_id, provider, self.options.host_id, run_id if not parent_vendor_id else self.run_id(provider, parent_vendor_id, "main"), project_id, title, started_at, started_at),
        )
        self.conn.execute(
            """INSERT INTO runs(id,provider,host_id,vendor_run_id,stream_key,workload_id,project_id,
                                 surface,title,cwd,started_at,updated_at,metadata_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 project_id=COALESCE(excluded.project_id,runs.project_id),
                 surface=COALESCE(excluded.surface,runs.surface),
                 title=COALESCE(excluded.title,runs.title),
                 cwd=COALESCE(excluded.cwd,runs.cwd),
                 started_at=COALESCE(runs.started_at,excluded.started_at),
                 updated_at=COALESCE(excluded.updated_at,runs.updated_at),
                 metadata_json=CASE WHEN excluded.metadata_json='{}' THEN runs.metadata_json ELSE excluded.metadata_json END""",
            (
                run_id, provider, self.options.host_id, vendor_id, stream_key, workload_id,
                project_id, surface, title, cwd, started_at, started_at,
                json_text(metadata or {}),
            ),
        )
        if not parent_vendor_id:
            self.conn.execute(
                "UPDATE workloads SET root_run_id=?, project_id=COALESCE(project_id,?), title=COALESCE(title,?) WHERE id=?",
                (run_id, project_id, title, workload_id),
            )
        else:
            self.link_runs(self.run_id(provider, parent_vendor_id, "main"), run_id, "spawn")
        return run_id

    def link_runs(self, parent_id: str, child_id: str, relation: str, status: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        if parent_id == child_id:
            self.warn(None, "self_edge", f"ignored self edge for {parent_id}")
            return
        parent = self.conn.execute("SELECT workload_id FROM runs WHERE id=?", (parent_id,)).fetchone()
        child = self.conn.execute("SELECT workload_id FROM runs WHERE id=?", (child_id,)).fetchone()
        if not parent or not child:
            return
        creates_cycle = self.conn.execute(
            """WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION
                   SELECT e.child_run_id FROM run_edges e JOIN descendants d ON e.parent_run_id=d.id
               ) SELECT 1 FROM descendants WHERE id=?""",
            (child_id, parent_id),
        ).fetchone()
        if creates_cycle:
            self.warn(None, "run_edge_cycle", f"ignored run edge {parent_id} -> {child_id}")
            return
        self.conn.execute(
            """INSERT INTO run_edges(parent_run_id,child_run_id,relation,status,metadata_json)
               VALUES(?,?,?,?,?) ON CONFLICT(parent_run_id,child_run_id,relation)
               DO UPDATE SET status=COALESCE(excluded.status,run_edges.status), metadata_json=excluded.metadata_json""",
            (parent_id, child_id, relation, status, json_text(metadata or {})),
        )
        parent_workload = str(parent[0])
        old_workload = str(child[0])
        self.conn.execute(
            """WITH RECURSIVE descendants(id) AS (
                   SELECT ? UNION ALL
                   SELECT e.child_run_id FROM run_edges e JOIN descendants d ON e.parent_run_id=d.id
               ) UPDATE runs SET workload_id=? WHERE id IN (SELECT id FROM descendants)""",
            (child_id, parent_workload),
        )
        if old_workload != parent_workload:
            self.conn.execute(
                "DELETE FROM workloads WHERE id=? AND NOT EXISTS(SELECT 1 FROM runs WHERE workload_id=?)",
                (old_workload, old_workload),
            )

    def ensure_turn(self, run_id: str, vendor_turn: str, *, sequence: int | None = None, status: str | None = None, started: str | None = None, completed: str | None = None, metadata: dict[str, Any] | None = None) -> str:
        turn_id = self.turn_id(run_id, vendor_turn)
        self.conn.execute(
            """INSERT INTO turns(id,run_id,vendor_turn_id,sequence,status,started_at,completed_at,metadata_json)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                 sequence=COALESCE(turns.sequence,excluded.sequence),
                 status=COALESCE(excluded.status,turns.status),
                 started_at=COALESCE(turns.started_at,excluded.started_at),
                 completed_at=COALESCE(excluded.completed_at,turns.completed_at),
                 metadata_json=CASE WHEN excluded.metadata_json='{}' THEN turns.metadata_json ELSE excluded.metadata_json END""",
            (turn_id, run_id, vendor_turn, sequence, status, started, completed, json_text(metadata or {})),
        )
        return turn_id

    def ingest_jsonl(self, source: JsonlSource) -> None:
        source_id = self.source_instance(source.provider, source.root, "history")
        stat = source.path.stat()
        relative = str(source.path.relative_to(source.root))
        self.conn.execute(
            """INSERT INTO source_files(source_instance_id,logical_key,relative_path,kind,size,mtime_ns,device,inode,last_seen_sync)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source_instance_id,relative_path) DO UPDATE SET
                 logical_key=excluded.logical_key,kind=excluded.kind,size=excluded.size,
                 mtime_ns=excluded.mtime_ns,device=excluded.device,inode=excluded.inode,
                 last_seen_sync=excluded.last_seen_sync,state='present'""",
            (source_id, source.logical_key, relative, source.kind, stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino, self.sync_id),
        )
        file_row = self.conn.execute(
            "SELECT * FROM source_files WHERE source_instance_id=? AND relative_path=?",
            (source_id, relative),
        ).fetchone()
        assert file_row
        file_id = int(file_row["id"])
        self.stats.files_seen += 1
        offset = int(file_row["checkpoint_offset"])
        line_index = int(file_row["checkpoint_line"])
        identity_changed = (
            file_row["device"] is not None and file_row["inode"] is not None
            and (int(file_row["device"]), int(file_row["inode"])) != (stat.st_dev, stat.st_ino)
        )
        same_size_rewrite = stat.st_size == offset and stat.st_mtime_ns != int(file_row["mtime_ns"])
        rescanned = (
            stat.st_size < offset or identity_changed or same_size_rewrite
            or not self._probe_matches(source.path, offset, file_row["checkpoint_probe_sha256"])
        )
        if rescanned:
            offset = 0
            line_index = 0
        run_id = None
        if source.vendor_run_id:
            run_id = self.ensure_run(
                source.provider, source.vendor_run_id, source.stream_key,
                parent_vendor_id=source.parent_vendor_id,
                surface="subagent" if source.parent_vendor_id else None,
            )
        last_turn = self.conn.execute(
            "SELECT turn_id FROM events WHERE run_id=? AND turn_id IS NOT NULL ORDER BY sequence DESC LIMIT 1",
            (run_id,),
        ).fetchone() if run_id else None
        state = FileState(run_id, str(last_turn[0]) if last_turn else None)
        new_offset = offset
        new_line = line_index
        with self.conn:
            with source.path.open("rb") as handle:
                handle.seek(offset)
                while True:
                    start = handle.tell()
                    raw = handle.readline(self.options.max_record_bytes + 1)
                    if not raw:
                        break
                    if len(raw) > self.options.max_record_bytes:
                        self.warn(file_id, "record_too_large", f"record {new_line} exceeds {self.options.max_record_bytes} bytes", new_line)
                        break
                    if not raw.endswith(b"\n"):
                        break
                    added = self._ingest_raw_record(source, source_id, file_id, state, new_line, start, raw)
                    if added:
                        self.stats.records_added += 1
                    new_offset = handle.tell()
                    new_line += 1
            probe = self._probe_hash(source.path, new_offset)
            if rescanned:
                stale = [
                    int(row[0])
                    for row in self.conn.execute(
                        """SELECT raw_record_id FROM stream_records
                            WHERE source_instance_id=? AND logical_key=? AND record_index>=?""",
                        (source_id, source.logical_key, new_line),
                    )
                ]
                self._deactivate_raw_records(stale)
                self.conn.execute(
                    """DELETE FROM stream_records
                        WHERE source_instance_id=? AND logical_key=? AND record_index>=?""",
                    (source_id, source.logical_key, new_line),
                )
            final_stat = source.path.stat()
            self.conn.execute(
                """UPDATE source_files SET size=?,mtime_ns=?,checkpoint_offset=?,checkpoint_line=?,
                       checkpoint_probe_sha256=?,last_seen_sync=?,state='present' WHERE id=?""",
                (final_stat.st_size, final_stat.st_mtime_ns, new_offset, new_line, probe, self.sync_id, file_id),
            )

    def _ingest_raw_record(self, source: JsonlSource, source_id: int, file_id: int, state: FileState, index: int, offset: int, raw: bytes) -> bool:
        digest = hashlib.sha256(raw).hexdigest()
        try:
            data = json.loads(raw)
            parsed = 1
            error = None
            native_type = str(data.get("type", "")) if isinstance(data, dict) else type(data).__name__
        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as exc:
            data = None
            parsed = 0
            error = str(exc)
            native_type = None
        inserted = self.conn.execute(
            """INSERT OR IGNORE INTO raw_records(
                   source_instance_id,source_file_id,logical_key,record_index,byte_offset,
                   raw_sha256,raw_zlib,parsed,parse_error,native_type,run_id,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (source_id, file_id, source.logical_key, index, offset, digest, zlib.compress(raw, 6), parsed, error, native_type, state.run_id, utc_now()),
        )
        row = self.conn.execute(
            """SELECT id,source_file_id,parsed,normalized FROM raw_records
                WHERE source_instance_id=? AND logical_key=? AND record_index=? AND raw_sha256=?""",
            (source_id, source.logical_key, index, digest),
        ).fetchone()
        assert row
        raw_id = int(row[0])
        prior = self.conn.execute(
            """SELECT raw_record_id FROM stream_records
                WHERE source_instance_id=? AND logical_key=? AND record_index=?""",
            (source_id, source.logical_key, index),
        ).fetchone()
        if prior and int(prior[0]) != raw_id:
            self._deactivate_raw_records([int(prior[0])])
        self.conn.execute(
            """INSERT INTO stream_records(source_instance_id,logical_key,record_index,raw_record_id)
               VALUES(?,?,?,?) ON CONFLICT(source_instance_id,logical_key,record_index)
               DO UPDATE SET raw_record_id=excluded.raw_record_id""",
            (source_id, source.logical_key, index, raw_id),
        )
        if row["normalized"]:
            return bool(inserted.rowcount)
        if not parsed:
            self.warn(file_id, "invalid_json", error or "invalid JSON", index)
            self.conn.execute("UPDATE raw_records SET normalized=1 WHERE id=?", (raw_id,))
            return bool(inserted.rowcount)
        if source.provider == "codex":
            self._process_codex(source, raw_id, index, data, state)
        else:
            self._process_claude(source, raw_id, index, data, state)
        self.conn.execute("UPDATE raw_records SET run_id=?,normalized=1 WHERE id=?", (state.run_id, raw_id))
        return bool(inserted.rowcount)

    def _deactivate_raw_records(self, raw_ids: list[int]) -> None:
        if not raw_ids:
            return
        placeholders = ",".join("?" for _ in raw_ids)
        self.conn.execute(f"DELETE FROM events WHERE raw_record_id IN ({placeholders})", raw_ids)
        usage_ids = [
            str(row[0])
            for row in self.conn.execute(
                f"SELECT DISTINCT usage_id FROM usage_sources WHERE raw_record_id IN ({placeholders})",
                raw_ids,
            )
        ]
        self.conn.execute(f"DELETE FROM usage_sources WHERE raw_record_id IN ({placeholders})", raw_ids)
        if usage_ids:
            usage_placeholders = ",".join("?" for _ in usage_ids)
            self.conn.execute(
                f"""DELETE FROM usage_records WHERE id IN ({usage_placeholders})
                     AND NOT EXISTS(SELECT 1 FROM usage_sources s WHERE s.usage_id=usage_records.id)""",
                usage_ids,
            )
        self.conn.execute(f"UPDATE raw_records SET normalized=0 WHERE id IN ({placeholders})", raw_ids)

    def _process_codex(self, source: JsonlSource, raw_id: int, sequence: int, data: Any, state: FileState) -> None:
        if not isinstance(data, dict):
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, None, "unknown", data)
            self.warn_for_raw(raw_id, "unknown_codex_record", "non-object Codex record", sequence)
            return
        if source.kind == "session_index":
            vendor_id = data.get("id")
            if vendor_id:
                run_id = self.ensure_run("codex", str(vendor_id), "main", title=_string(data.get("thread_name")))
                self.conn.execute("UPDATE runs SET updated_at=COALESCE(?,updated_at),title=COALESCE(?,title) WHERE id=?", (_string(data.get("updated_at")), _string(data.get("thread_name")), run_id))
                self.conn.execute("UPDATE workloads SET title=COALESCE(?,title),updated_at=COALESCE(?,updated_at) WHERE root_run_id=?", (_string(data.get("thread_name")), _string(data.get("updated_at")), run_id))
            return
        top_type = data.get("type")
        payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
        timestamp = _string(data.get("timestamp"))
        ordinal = _integer(data.get("ordinal"), sequence)
        if top_type == "session_meta":
            vendor_id = _string(payload.get("id") or payload.get("session_id") or source.vendor_run_id)
            if vendor_id:
                source_info = payload.get("source") if isinstance(payload.get("source"), dict) else {}
                spawn = _nested(source_info, "subagent", "thread_spawn")
                parent_vendor = _string(spawn.get("parent_thread_id")) if isinstance(spawn, dict) else source.parent_vendor_id
                state.run_id = self.ensure_run(
                    "codex", vendor_id, source.stream_key, parent_vendor_id=parent_vendor,
                    surface=_string(payload.get("originator") or payload.get("source")),
                    cwd=_string(payload.get("cwd")), started_at=_string(payload.get("timestamp") or timestamp),
                    metadata=payload,
                )
                if isinstance(spawn, dict) and parent_vendor:
                    self.conn.execute(
                        "UPDATE runs SET title=COALESCE(?,title),metadata_json=? WHERE id=?",
                        (_string(spawn.get("agent_nickname")), json_text(payload), state.run_id),
                    )
            self.add_event(raw_id, state.run_id, None, ordinal, 0, timestamp, "context", payload, item_type="session_meta")
            base = payload.get("base_instructions")
            if isinstance(base, dict) and base.get("text") is not None:
                self.add_event(raw_id, state.run_id, None, ordinal, 1, timestamp, "instruction", base, role="developer", item_type="base_instructions")
            return
        if top_type == "turn_context":
            vendor_turn = _string(payload.get("turn_id"))
            if state.run_id and vendor_turn:
                state.current_turn_id = self.ensure_turn(state.run_id, vendor_turn, sequence=ordinal, started=timestamp, metadata=payload)
                project_id = self.ensure_project(_string(payload.get("cwd")))
                self.conn.execute(
                    """UPDATE runs SET cwd=COALESCE(?,cwd),project_id=COALESCE(?,project_id),
                         model=COALESCE(?,model),reasoning_effort=COALESCE(?,reasoning_effort),updated_at=? WHERE id=?""",
                    (_string(payload.get("cwd")), project_id, _string(payload.get("model")), _string(payload.get("effort")), timestamp, state.run_id),
                )
            self.add_event(raw_id, state.run_id, state.current_turn_id, ordinal, 0, timestamp, "context", payload, item_type="turn_context")
            return
        if top_type == "world_state":
            self.add_event(raw_id, state.run_id, state.current_turn_id, ordinal, 0, timestamp, "context", payload, item_type="world_state")
            return
        if top_type in {"compacted", "inter_agent_communication_metadata"}:
            self.add_event(raw_id, state.run_id, state.current_turn_id, ordinal, 0, timestamp, "context", data, item_type=_string(top_type))
            return
        if top_type == "response_item":
            self._codex_response_item(raw_id, state, ordinal, timestamp, payload)
            return
        if top_type == "event_msg":
            self._codex_event_message(raw_id, state, ordinal, timestamp, payload)
            return
        self.add_event(raw_id, state.run_id, state.current_turn_id, ordinal, 0, timestamp, "unknown", data, item_type=_string(top_type))
        self.warn_for_raw(raw_id, "unknown_codex_record", f"unknown Codex record type {top_type!r}", sequence)

    def _codex_response_item(self, raw_id: int, state: FileState, sequence: int, timestamp: str | None, payload: dict[str, Any]) -> None:
        item_type = _string(payload.get("type")) or "unknown"
        turn_vendor = _string(_nested(payload, "internal_chat_message_metadata_passthrough", "turn_id"))
        if state.run_id and turn_vendor:
            state.current_turn_id = self.ensure_turn(state.run_id, turn_vendor)
        if item_type == "message":
            role = _string(payload.get("role"))
            phase = _string(payload.get("phase"))
            content = payload.get("content") if isinstance(payload.get("content"), list) else []
            if not content:
                content = [payload]
            for sub, block in enumerate(content):
                block_type = _string(block.get("type")) if isinstance(block, dict) else "text"
                kind = "instruction" if role in {"developer", "system"} else "message"
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, kind, block, role=role, phase=phase, item_type=block_type, message_id=_string(payload.get("id")))
            return
        if item_type == "reasoning":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "reasoning", payload, item_type=item_type, message_id=_string(payload.get("id")))
            return
        if item_type == "agent_message":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "message", payload, role="assistant", item_type=item_type, message_id=_string(payload.get("id")))
            return
        if item_type in {"function_call", "custom_tool_call", "mcp_call", "web_search_call", "computer_call", "tool_search_call", "image_generation_call"}:
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "tool_call", payload, item_type=item_type, tool_name=_string(payload.get("name") or item_type), call_id=_string(payload.get("call_id") or payload.get("id")))
            return
        if item_type in {"function_call_output", "custom_tool_call_output", "mcp_call_output", "computer_call_output", "tool_search_output"}:
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "tool_result", payload, item_type=item_type, call_id=_string(payload.get("call_id") or payload.get("id")))
            return
        self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "unknown", payload, item_type=item_type)
        self.warn_for_raw(raw_id, "unknown_codex_item", f"unknown Codex response item {item_type!r}", sequence)

    def _codex_event_message(self, raw_id: int, state: FileState, sequence: int, timestamp: str | None, payload: dict[str, Any]) -> None:
        event_type = _string(payload.get("type")) or "unknown"
        vendor_turn = _string(payload.get("turn_id"))
        if state.run_id and vendor_turn:
            status = "running" if event_type == "task_started" else None
            state.current_turn_id = self.ensure_turn(state.run_id, vendor_turn, sequence=sequence, status=status, started=timestamp if status else None)
        if event_type == "token_count":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "usage", payload, item_type=event_type)
            if state.run_id:
                self._insert_codex_snapshot(raw_id, state.run_id, state.current_turn_id, sequence, timestamp, payload.get("info"))
            return
        if event_type in {"agent_message", "user_message"}:
            role = "assistant" if event_type == "agent_message" else "user"
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "message", payload, role=role, item_type=event_type)
            return
        if event_type == "agent_reasoning":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "reasoning", payload, role="assistant", item_type=event_type)
            return
        if event_type == "thread_settings_applied":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "context", payload, item_type=event_type)
            return
        if event_type in {"mcp_tool_call_end", "image_generation_end"}:
            invocation = payload.get("invocation") if isinstance(payload.get("invocation"), dict) else {}
            self.add_event(
                raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp,
                "tool_result", payload, item_type=event_type,
                tool_name=_string(invocation.get("tool") or event_type.removesuffix("_end")),
                call_id=_string(payload.get("call_id") or payload.get("id")),
            )
            return
        if event_type == "view_image_tool_call":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "tool_call", payload, item_type=event_type, tool_name="view_image", call_id=_string(payload.get("call_id") or payload.get("id")))
            return
        if event_type == "context_compacted":
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "context", payload, item_type=event_type)
            return
        if event_type.endswith("_begin") and event_type.startswith(("exec_command", "patch_apply", "web_search")):
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "tool_call", payload, item_type=event_type, tool_name=event_type.removesuffix("_begin"))
            return
        if event_type.endswith("_end") and event_type.startswith(("exec_command", "patch_apply", "web_search")):
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "tool_result", payload, item_type=event_type, tool_name=event_type.removesuffix("_end"))
            return
        known_status = event_type in {
            "task_started", "task_complete", "item_completed", "thread_name_updated",
            "collab_agent_spawn_begin", "collab_agent_spawn_end", "collab_close_begin",
            "collab_close_end", "collab_waiting_begin", "collab_waiting_end", "turn_aborted",
            "sub_agent_activity", "guardian_assessment", "thread_goal_updated",
            "thread_rolled_back", "error",
        }
        if known_status:
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "status", payload, item_type=event_type)
            if state.current_turn_id and event_type in {"task_complete", "turn_aborted"}:
                self.conn.execute("UPDATE turns SET status=?,completed_at=? WHERE id=?", ("completed" if event_type == "task_complete" else "aborted", timestamp, state.current_turn_id))
            return
        self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "unknown", payload, item_type=event_type)
        self.warn_for_raw(raw_id, "unknown_codex_event", f"unknown Codex event {event_type!r}", sequence)

    def _process_claude(self, source: JsonlSource, raw_id: int, sequence: int, data: Any, state: FileState) -> None:
        if not isinstance(data, dict):
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, None, "unknown", data)
            self.warn_for_raw(raw_id, "unknown_claude_record", "non-object Claude record", sequence)
            return
        record_type = _string(data.get("type")) or "unknown"
        timestamp = _string(data.get("timestamp"))
        vendor_session = _string(data.get("sessionId"))
        if source.parent_vendor_id and source.agent_id:
            vendor_id = source.vendor_run_id or f"{source.parent_vendor_id}/{source.agent_id}"
            stream_key = f"agent:{source.agent_id}"
            parent = source.parent_vendor_id
        else:
            vendor_id = vendor_session or source.vendor_run_id or source.logical_key
            stream_key = source.stream_key
            parent = None
        state.run_id = self.ensure_run(
            "claude", vendor_id, stream_key, parent_vendor_id=parent,
            surface=_string(data.get("entrypoint")), cwd=_string(data.get("cwd")),
            started_at=timestamp, metadata={k: data[k] for k in ("version", "gitBranch", "slug", "entrypoint") if k in data},
        )
        self.conn.execute(
            "UPDATE runs SET updated_at=COALESCE(?,updated_at),reasoning_effort=COALESCE(?,reasoning_effort) WHERE id=?",
            (timestamp, _string(data.get("effort")), state.run_id),
        )
        if record_type == "user":
            prompt_id = _string(data.get("promptId") or data.get("uuid"))
            if prompt_id:
                state.current_turn_id = self.ensure_turn(state.run_id, prompt_id, sequence=sequence, status="running", started=timestamp)
            message = data.get("message") if isinstance(data.get("message"), dict) else {}
            self._claude_content(raw_id, state, sequence, timestamp, message.get("content"), _string(message.get("role") or "user"), bool(data.get("isMeta")))
            return
        if record_type == "assistant":
            message = data.get("message") if isinstance(data.get("message"), dict) else {}
            message_id = _string(message.get("id") or data.get("requestId") or data.get("uuid"))
            self._claude_content(raw_id, state, sequence, timestamp, message.get("content"), _string(message.get("role") or "assistant"), False, message_id)
            if message_id and isinstance(message.get("usage"), dict):
                self._insert_claude_usage(raw_id, state.run_id, state.current_turn_id, sequence, timestamp, message_id, message["usage"])
            return
        if record_type == "attachment":
            attachment = data.get("attachment", data)
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "attachment", attachment, item_type="attachment")
            self._collect_upload_paths(attachment)
            return
        if record_type in {"custom-title", "ai-title", "agent-name"}:
            title = _string(data.get("customTitle") or data.get("aiTitle") or data.get("agentName"))
            self.conn.execute("UPDATE runs SET title=COALESCE(?,title) WHERE id=?", (title, state.run_id))
            self.conn.execute("UPDATE workloads SET title=COALESCE(?,title) WHERE root_run_id=?", (title, state.run_id))
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "status", data, item_type=record_type)
            return
        if record_type == "bridge-session":
            self.conn.execute("UPDATE runs SET surface='Claude Desktop' WHERE id=?", (state.run_id,))
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "context", data, item_type=record_type)
            return
        if record_type in {"mode", "permission-mode", "frame-link", "pr-link", "file-history-snapshot"}:
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "context", data, item_type=record_type)
            return
        if record_type in {
            "queue-operation", "last-prompt", "atis-latch", "system", "relocated",
            "started", "failed", "result", "artifact-autoreact-ledger",
            "artifact-comment-monitor",
        }:
            self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "status", data, item_type=record_type)
            return
        self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, 0, timestamp, "unknown", data, item_type=record_type)
        self.warn_for_raw(raw_id, "unknown_claude_record", f"unknown Claude record {record_type!r}", sequence)

    def _claude_content(self, raw_id: int, state: FileState, sequence: int, timestamp: str | None, content: Any, role: str | None, is_meta: bool, message_id: str | None = None) -> None:
        blocks = content if isinstance(content, list) else [content]
        for sub, block in enumerate(blocks):
            if isinstance(block, str):
                block = {"type": "text", "text": block}
            if not isinstance(block, dict):
                block = {"type": "unknown", "value": block}
            block_type = _string(block.get("type")) or "unknown"
            if block_type == "text":
                kind = "instruction" if is_meta or role in {"system", "developer"} else "message"
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, kind, block, role=role, item_type=block_type, message_id=message_id)
            elif block_type in {"thinking", "redacted_thinking"}:
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, "reasoning", block, role=role, item_type=block_type, message_id=message_id)
            elif block_type in {"tool_use", "server_tool_use"}:
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, "tool_call", block, role=role, item_type=block_type, tool_name=_string(block.get("name")), call_id=_string(block.get("id")), message_id=message_id)
            elif block_type == "tool_result":
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, "tool_result", block, role=role, item_type=block_type, call_id=_string(block.get("tool_use_id")), message_id=message_id)
            elif block_type in {"image", "document"}:
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, "attachment", block, role=role, item_type=block_type, message_id=message_id)
                self._collect_upload_paths(block)
            else:
                self.add_event(raw_id, state.run_id, state.current_turn_id, sequence, sub, timestamp, "unknown", block, role=role, item_type=block_type, message_id=message_id)
                self.warn_for_raw(raw_id, "unknown_claude_content", f"unknown Claude content block {block_type!r}", sequence)

    def add_event(self, raw_id: int, run_id: str | None, turn_id: str | None, sequence: int, sub_index: int, timestamp: str | None, kind: str, payload: Any, *, role: str | None = None, phase: str | None = None, item_type: str | None = None, tool_name: str | None = None, call_id: str | None = None, message_id: str | None = None) -> None:
        event_id = stable_id("event", raw_id, sub_index)
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO events(id,raw_record_id,run_id,turn_id,sub_index,sequence,timestamp,
                   kind,role,phase,item_type,tool_name,call_id,message_id,payload_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (event_id, raw_id, run_id, turn_id, sub_index, sequence, timestamp, kind, role, phase, item_type, tool_name, call_id, message_id, json_text(payload)),
        )
        if cur.rowcount:
            self.stats.events_added += 1

    def _insert_claude_usage(self, raw_id: int, run_id: str, turn_id: str | None, sequence: int, timestamp: str | None, message_id: str, usage: dict[str, Any]) -> None:
        details = usage.get("output_tokens_details") if isinstance(usage.get("output_tokens_details"), dict) else {}
        input_tokens = _integer(usage.get("input_tokens"), 0)
        cached = _integer(usage.get("cache_read_input_tokens"), 0)
        cache_write = _integer(usage.get("cache_creation_input_tokens"), 0)
        output = _integer(usage.get("output_tokens"), 0)
        reasoning = _integer(details.get("thinking_tokens"), 0)
        total = _integer(usage.get("total_tokens"), input_tokens + cached + cache_write + output)
        usage_id = stable_id("usage", run_id, "response", message_id)
        self.conn.execute(
            """INSERT OR IGNORE INTO usage_records(id,run_id,turn_id,provider,scope,provider_key,sequence,timestamp,
                   input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_tokens,total_tokens,raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (usage_id, run_id, turn_id, "claude", "response", message_id, sequence, timestamp, input_tokens, cached, cache_write, output, reasoning, total, json_text(usage)),
        )
        self.conn.execute("INSERT OR IGNORE INTO usage_sources(usage_id,raw_record_id) VALUES(?,?)", (usage_id, raw_id))

    def _insert_codex_snapshot(self, raw_id: int, run_id: str, turn_id: str | None, sequence: int, timestamp: str | None, info: Any) -> None:
        if not isinstance(info, dict):
            return
        total = info.get("total_token_usage") if isinstance(info.get("total_token_usage"), dict) else {}
        key = f"snapshot:{sequence}:{stable_id(json_text(info))[:12]}"
        usage_id = stable_id("usage", run_id, key)
        self.conn.execute(
            """INSERT OR IGNORE INTO usage_records(id,run_id,turn_id,provider,scope,provider_key,sequence,timestamp,
                   input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_tokens,total_tokens,raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                usage_id, run_id, turn_id, "codex", "snapshot", key, sequence, timestamp,
                _nullable_int(total.get("input_tokens")), _nullable_int(total.get("cached_input_tokens")),
                _nullable_int(total.get("cache_write_input_tokens")), _nullable_int(total.get("output_tokens")),
                _nullable_int(total.get("reasoning_output_tokens")), _nullable_int(total.get("total_tokens")), json_text(info),
            ),
        )
        self.conn.execute("INSERT OR IGNORE INTO usage_sources(usage_id,raw_record_id) VALUES(?,?)", (usage_id, raw_id))

    def _recompute_all_codex_usage(self) -> None:
        run_ids = [row[0] for row in self.conn.execute("SELECT DISTINCT run_id FROM usage_records WHERE provider='codex' AND scope='snapshot'")]
        for run_id in run_ids:
            self._recompute_codex_usage(str(run_id))

    def _recompute_codex_usage(self, run_id: str) -> None:
        rows = self.conn.execute(
            """SELECT * FROM usage_records WHERE run_id=? AND scope='snapshot'
               ORDER BY sequence,id""",
            (run_id,),
        ).fetchall()
        final_by_turn: dict[str, sqlite3.Row] = {}
        for row in rows:
            if row["turn_id"]:
                final_by_turn[str(row["turn_id"])] = row
        previous = {name: 0 for name in _TOKEN_COLUMNS}
        self.conn.execute("DELETE FROM usage_records WHERE run_id=? AND scope IN ('turn','run')", (run_id,))
        for turn_id, row in final_by_turn.items():
            current = {name: _integer(row[name], 0) for name in _TOKEN_COLUMNS}
            if any(current[name] < previous[name] for name in _TOKEN_COLUMNS):
                self.warn(None, "codex_usage_reset", f"cumulative token usage reset in {run_id} at {turn_id}")
                previous = current
                continue
            delta = {name: current[name] - previous[name] for name in _TOKEN_COLUMNS}
            vendor_turn = turn_id.rsplit(":turn:", 1)[-1]
            self._insert_derived_codex_usage(run_id, turn_id, "turn", vendor_turn, row, delta)
            previous = current
        if rows:
            row = rows[-1]
            totals = {name: _integer(row[name], 0) for name in _TOKEN_COLUMNS}
            self._insert_derived_codex_usage(run_id, None, "run", "run-total", row, totals)

    def _insert_derived_codex_usage(self, run_id: str, turn_id: str | None, scope: str, key: str, source: sqlite3.Row, values: dict[str, int]) -> None:
        self.conn.execute(
            """INSERT INTO usage_records(id,run_id,turn_id,provider,scope,provider_key,sequence,timestamp,
                   input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_tokens,total_tokens,raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (stable_id("usage", run_id, scope, key), run_id, turn_id, "codex", scope, key, source["sequence"], source["timestamp"], *(values[name] for name in _TOKEN_COLUMNS), source["raw_json"]),
        )

    def ingest_opencode_databases(self) -> None:
        root = self.options.opencode_data
        if not root or not root.is_dir():
            return
        databases = sorted(root.glob("opencode*.db"))
        override = os.environ.get("OPENCODE_DB")
        if override:
            candidate = Path(override).expanduser()
            if candidate.is_file() and candidate.parent.resolve() == root.resolve() and candidate not in databases:
                databases.append(candidate)
        for path in databases:
            self._read_opencode_database(root, path)

    def _read_opencode_database(self, root: Path, path: Path) -> None:
        source_id = self.source_instance("opencode", root, f"sqlite:{path.name}")
        source_size, source_mtime = _sqlite_size_mtime(path)
        with closing(self._readonly_sqlite(path)) as src:
            src.execute("BEGIN")
            tables = (
                ("project", ("id",), "time_updated"),
                ("project_directory", ("project_id", "directory"), "time_created"),
                ("workspace", ("id",), None),
                ("session", ("id",), "time_updated"),
                ("message", ("id",), "time_updated"),
                ("part", ("id",), "time_updated"),
                ("session_message", ("id",), "time_updated"),
                ("session_input", ("id",), "time_created"),
                ("session_context_epoch", ("session_id",), None),
                ("todo", ("session_id", "position"), "time_updated"),
            )
            for table, keys, cursor_column in tables:
                if not self._table_exists(src, table):
                    continue
                self._read_opencode_table(
                    src, root, path, source_id, table, keys, cursor_column,
                    source_size, source_mtime,
                )

    def _read_opencode_table(
        self,
        src: sqlite3.Connection,
        root: Path,
        path: Path,
        source_id: int,
        table: str,
        keys: tuple[str, ...],
        cursor_column: str | None,
        source_size: int,
        source_mtime: int,
    ) -> None:
        relative = f"{path.name}/{table}.jsonl"
        logical_file = f"{path.name}:{table}"
        stat = path.stat()
        self.conn.execute(
            """INSERT INTO source_files(source_instance_id,logical_key,relative_path,kind,size,mtime_ns,device,inode,last_seen_sync)
               VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source_instance_id,relative_path) DO UPDATE SET
                 logical_key=excluded.logical_key,kind=excluded.kind,last_seen_sync=excluded.last_seen_sync,state='present'""",
            (source_id, logical_file, relative, "sqlite_projection", source_size, source_mtime, stat.st_dev, stat.st_ino, self.sync_id),
        )
        file_row = self.conn.execute(
            "SELECT * FROM source_files WHERE source_instance_id=? AND relative_path=?",
            (source_id, relative),
        ).fetchone()
        assert file_row
        file_id = int(file_row["id"])
        self.stats.files_seen += 1
        unchanged = int(file_row["size"]) == source_size and int(file_row["mtime_ns"]) == source_mtime
        if unchanged and int(file_row["checkpoint_line"]) > 0:
            return
        identity_changed = (
            file_row["device"] is not None and file_row["inode"] is not None
            and (int(file_row["device"]), int(file_row["inode"])) != (stat.st_dev, stat.st_ino)
        )
        cursor = 0 if identity_changed else int(file_row["checkpoint_offset"])
        if cursor_column:
            query = f'SELECT * FROM "{table}" WHERE "{cursor_column}">=? ORDER BY "{cursor_column}"'
            rows = src.execute(query, (cursor,))
        else:
            rows = src.execute(f'SELECT * FROM "{table}"')
        seen = 0
        maximum = cursor
        with self.conn:
            for row in rows:
                data = dict(row)
                key = "\0".join(str(data[name]) for name in keys)
                if cursor_column:
                    maximum = max(maximum, _integer(data.get(cursor_column), 0))
                if self._ingest_opencode_row(source_id, file_id, path.name, table, key, data):
                    self.stats.records_added += 1
                seen += 1
            self.conn.execute(
                """UPDATE source_files SET size=?,mtime_ns=?,device=?,inode=?,checkpoint_offset=?,
                       checkpoint_line=checkpoint_line+?,last_seen_sync=?,state='present' WHERE id=?""",
                (source_size, source_mtime, stat.st_dev, stat.st_ino, maximum, seen, self.sync_id, file_id),
            )

    def _ingest_opencode_row(
        self,
        source_id: int,
        file_id: int,
        database: str,
        table: str,
        key: str,
        data: dict[str, Any],
    ) -> bool:
        raw = (json_text({"table": table, "row": data}) + "\n").encode("utf-8", "surrogatepass")
        digest = hashlib.sha256(raw).hexdigest()
        logical_key = f"{database}:{table}:{key}"
        inserted = self.conn.execute(
            """INSERT OR IGNORE INTO raw_records(
                   source_instance_id,source_file_id,logical_key,record_index,byte_offset,
                   raw_sha256,raw_zlib,parsed,native_type,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (source_id, file_id, logical_key, 0, 0, digest, zlib.compress(raw, 6), 1, table, utc_now()),
        )
        row = self.conn.execute(
            """SELECT id,normalized FROM raw_records
                 WHERE source_instance_id=? AND logical_key=? AND record_index=0 AND raw_sha256=?""",
            (source_id, logical_key, digest),
        ).fetchone()
        assert row
        raw_id = int(row["id"])
        prior = self.conn.execute(
            """SELECT raw_record_id FROM stream_records
                 WHERE source_instance_id=? AND logical_key=? AND record_index=0""",
            (source_id, logical_key),
        ).fetchone()
        if prior and int(prior[0]) != raw_id:
            self._deactivate_raw_records([int(prior[0])])
        self.conn.execute(
            """INSERT INTO stream_records(source_instance_id,logical_key,record_index,raw_record_id)
               VALUES(?,?,0,?) ON CONFLICT(source_instance_id,logical_key,record_index)
               DO UPDATE SET raw_record_id=excluded.raw_record_id""",
            (source_id, logical_key, raw_id),
        )
        if not row["normalized"]:
            run_id = self._process_opencode_row(raw_id, table, data)
            self.conn.execute("UPDATE raw_records SET run_id=?,normalized=1 WHERE id=?", (run_id, raw_id))
        return bool(inserted.rowcount)

    def _process_opencode_row(self, raw_id: int, table: str, data: dict[str, Any]) -> str | None:
        if table == "session":
            vendor_id = str(data["id"])
            parent = _string(data.get("parent_id"))
            run_id = self.ensure_run(
                "opencode", vendor_id, "main", parent_vendor_id=parent,
                surface="OpenCode", cwd=_string(data.get("directory")),
                title=_string(data.get("title")), started_at=_time_value(data.get("time_created")),
                metadata=data,
            )
            self.conn.execute(
                """UPDATE runs SET status=?,model=COALESCE(?,model),reasoning_effort=COALESCE(?,reasoning_effort),
                     updated_at=COALESCE(?,updated_at) WHERE id=?""",
                (
                    "archived" if data.get("time_archived") else "active",
                    _string(data.get("model")), _string(data.get("agent")),
                    _time_value(data.get("time_updated")), run_id,
                ),
            )
            self.add_event(raw_id, run_id, None, _integer(data.get("time_created"), 0), 0, _time_value(data.get("time_created")), "context", data, item_type="session")
            return run_id
        if table in {"message", "session_message"}:
            return self._process_opencode_message(raw_id, data)
        if table == "part":
            return self._process_opencode_part(raw_id, data)
        if table == "todo":
            run_id = self.ensure_run("opencode", str(data["session_id"]), "main")
            self.add_event(raw_id, run_id, None, _integer(data.get("position"), 0), 0, _time_value(data.get("time_updated")), "status", data, item_type="todo")
            return run_id
        if table == "session_input":
            run_id = self.ensure_run("opencode", str(data["session_id"]), "main")
            turn_id = self.ensure_turn(run_id, str(data["id"]), sequence=_integer(data.get("admitted_seq"), 0), started=_time_value(data.get("time_created")))
            self.add_event(raw_id, run_id, turn_id, _integer(data.get("admitted_seq"), 0), 0, _time_value(data.get("time_created")), "message", data, role="user", item_type="session_input", message_id=str(data["id"]))
            return run_id
        if table == "session_context_epoch":
            run_id = self.ensure_run("opencode", str(data["session_id"]), "main")
            self.add_event(raw_id, run_id, None, _integer(data.get("baseline_seq"), 0), 0, None, "context", data, item_type="session_context_epoch")
            return run_id
        return None

    def _process_opencode_message(self, raw_id: int, data: dict[str, Any]) -> str:
        vendor_session = str(data["session_id"])
        run_id = self.ensure_run("opencode", vendor_session, "main")
        payload = _json_object(data.get("data"))
        role = _string(payload.get("role") or data.get("type"))
        message_id = str(data["id"])
        sequence = _integer(data.get("time_created") or data.get("seq"), 0)
        timestamp = _time_value(data.get("time_created") or _nested(payload, "time", "created"))
        if role == "user":
            turn_id = self.ensure_turn(run_id, message_id, sequence=sequence, status="running", started=timestamp)
        else:
            parent = _string(payload.get("parentID"))
            vendor_turn = parent or message_id
            turn_id = self.ensure_turn(run_id, vendor_turn, sequence=sequence)
        self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "message", payload or data, role=role, item_type="message", message_id=message_id)
        model = _string(payload.get("modelID") or _nested(payload, "model", "modelID"))
        if model:
            self.conn.execute("UPDATE runs SET model=COALESCE(?,model) WHERE id=?", (model, run_id))
        tokens = payload.get("tokens")
        if role == "assistant" and isinstance(tokens, dict):
            self._insert_opencode_usage(raw_id, run_id, turn_id, sequence, timestamp, message_id, tokens, payload)
            completed = _time_value(_nested(payload, "time", "completed"))
            if completed:
                self.conn.execute("UPDATE turns SET status='completed',completed_at=? WHERE id=?", (completed, turn_id))
        return run_id

    def _process_opencode_part(self, raw_id: int, data: dict[str, Any]) -> str:
        run_id = self.ensure_run("opencode", str(data["session_id"]), "main")
        payload = _json_object(data.get("data"))
        part_type = _string(payload.get("type")) or "unknown"
        message_id = str(data["message_id"])
        linked = self.conn.execute(
            "SELECT turn_id,role FROM events WHERE run_id=? AND message_id=? AND item_type='message' LIMIT 1",
            (run_id, message_id),
        ).fetchone()
        turn_id = str(linked[0]) if linked and linked[0] else None
        role = str(linked[1]) if linked and linked[1] else None
        sequence = _integer(data.get("time_created"), 0)
        timestamp = _time_value(data.get("time_created") or _nested(payload, "time", "start"))
        if part_type == "text":
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "message", payload, role=role, item_type=part_type, message_id=message_id)
        elif part_type == "reasoning":
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "reasoning", payload, role=role or "assistant", item_type=part_type, message_id=message_id)
        elif part_type == "tool":
            state = payload.get("state") if isinstance(payload.get("state"), dict) else {}
            call_id = _string(payload.get("callID"))
            tool = _string(payload.get("tool"))
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "tool_call", payload, role=role, item_type=part_type, tool_name=tool, call_id=call_id, message_id=message_id)
            if state.get("status") in {"completed", "error"} or "output" in state or "error" in state:
                self.add_event(raw_id, run_id, turn_id, sequence, 1, _time_value(_nested(state, "time", "end")) or timestamp, "tool_result", payload, role=role, item_type=part_type, tool_name=tool, call_id=call_id, message_id=message_id)
        elif part_type in {"file", "patch"}:
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "attachment", payload, role=role, item_type=part_type, message_id=message_id)
        elif part_type in {"step-start", "compaction"}:
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "context", payload, role=role, item_type=part_type, message_id=message_id)
        elif part_type == "step-finish":
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "usage", payload, role=role, item_type=part_type, message_id=message_id)
        else:
            self.add_event(raw_id, run_id, turn_id, sequence, 0, timestamp, "unknown", payload or data, role=role, item_type=part_type, message_id=message_id)
            self.warn_for_raw(raw_id, "unknown_opencode_part", f"unknown OpenCode part {part_type!r}")
        return run_id

    def _insert_opencode_usage(
        self,
        raw_id: int,
        run_id: str,
        turn_id: str | None,
        sequence: int,
        timestamp: str | None,
        message_id: str,
        tokens: dict[str, Any],
        payload: dict[str, Any],
    ) -> None:
        cache = tokens.get("cache") if isinstance(tokens.get("cache"), dict) else {}
        input_tokens = _integer(tokens.get("input"), 0)
        output_tokens = _integer(tokens.get("output"), 0)
        reasoning_tokens = _integer(tokens.get("reasoning"), 0)
        total_tokens = _integer(tokens.get("total"), input_tokens + output_tokens)
        usage_id = stable_id("usage", run_id, "response", message_id)
        self.conn.execute(
            """INSERT OR IGNORE INTO usage_records(id,run_id,turn_id,provider,scope,provider_key,sequence,timestamp,
                   input_tokens,cached_input_tokens,cache_write_input_tokens,output_tokens,reasoning_tokens,total_tokens,raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                usage_id, run_id, turn_id, "opencode", "response", message_id, sequence, timestamp,
                input_tokens, _integer(cache.get("read"), 0), _integer(cache.get("write"), 0),
                output_tokens, reasoning_tokens, total_tokens, json_text(payload),
            ),
        )
        self.conn.execute("INSERT OR IGNORE INTO usage_sources(usage_id,raw_record_id) VALUES(?,?)", (usage_id, raw_id))

    def ingest_codex_databases(self) -> None:
        root = self.options.codex_home
        if not root or not root.is_dir():
            return
        state = root / "state_5.sqlite"
        if state.is_file():
            self._read_codex_state(root, state)
        history = root / "thread_history_1.sqlite"
        if history.is_file():
            self._read_codex_turns(history)
        goals = root / "goals_1.sqlite"
        if goals.is_file():
            self._read_codex_goals(root, goals)

    def _readonly_sqlite(self, path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_exists(self, conn: sqlite3.Connection, table: str) -> bool:
        return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())

    def _read_codex_state(self, root: Path, path: Path) -> None:
        source_id = self.source_instance("codex", root, "state")
        with closing(self._readonly_sqlite(path)) as src, self.conn:
            if self._table_exists(src, "threads"):
                for row in src.execute("SELECT * FROM threads"):
                    data = dict(row)
                    vendor_id = str(data["id"])
                    run_id = self.ensure_run("codex", vendor_id, "main", surface=_string(data.get("source")), cwd=_string(data.get("cwd")), title=_string(data.get("title")), started_at=_time_value(data.get("created_at_ms") or data.get("created_at")), metadata=data)
                    project_id = self.ensure_project(_string(data.get("cwd")), _string(data.get("git_origin_url")))
                    self.conn.execute(
                        """UPDATE runs SET project_id=COALESCE(?,project_id),status=?,model=COALESCE(?,model),
                           reasoning_effort=COALESCE(?,reasoning_effort),updated_at=COALESCE(?,updated_at) WHERE id=?""",
                        (project_id, "archived" if data.get("archived") else "active", _string(data.get("model")), _string(data.get("reasoning_effort")), _time_value(data.get("updated_at_ms") or data.get("updated_at")), run_id),
                    )
            if self._table_exists(src, "thread_spawn_edges"):
                for row in src.execute("SELECT * FROM thread_spawn_edges"):
                    parent_vendor, child_vendor = str(row["parent_thread_id"]), str(row["child_thread_id"])
                    parent_id = self.ensure_run("codex", parent_vendor, "main")
                    child_id = self.ensure_run("codex", child_vendor, "main")
                    self.link_runs(parent_id, child_id, "spawn", _string(row["status"]), dict(row))
            if self._table_exists(src, "thread_artifacts"):
                for row in src.execute("SELECT * FROM thread_artifacts"):
                    data = dict(row)
                    run_id = self.ensure_run("codex", str(data["thread_id"]), "main")
                    relative = f"sqlite/thread_artifacts/{stable_id(data['id'])[:24]}.json"
                    self.ingest_virtual_artifact(source_id, relative, "thread_artifact", json_text(data).encode(), run_id, None)
                    payload = data.get("payload")
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except json.JSONDecodeError:
                            payload = None
                    for candidate in _structured_paths(payload):
                        artifact_path = Path(candidate).expanduser()
                        if not artifact_path.is_absolute():
                            artifact_path = root / artifact_path
                        try:
                            resolved = artifact_path.resolve()
                        except OSError:
                            continue
                        if resolved.is_relative_to(root.resolve()) and resolved.is_file():
                            self.ingest_artifact("codex", root, resolved, "thread_artifact_file", run_id, None, source_id=source_id)

    def _read_codex_turns(self, path: Path) -> None:
        with closing(self._readonly_sqlite(path)) as src, self.conn:
            if not self._table_exists(src, "thread_turns"):
                return
            for row in src.execute("SELECT * FROM thread_turns"):
                data = dict(row)
                run_id = self.ensure_run("codex", str(data["thread_id"]), "main")
                self.ensure_turn(run_id, str(data["turn_id"]), sequence=_nullable_int(data.get("rollout_ordinal")), status=_string(data.get("status")), started=_time_value(data.get("started_at")), completed=_time_value(data.get("completed_at")), metadata=data)

    def _read_codex_goals(self, root: Path, path: Path) -> None:
        source_id = self.source_instance("codex", root, "goals")
        with closing(self._readonly_sqlite(path)) as src, self.conn:
            if not self._table_exists(src, "thread_goals"):
                return
            for row in src.execute("SELECT * FROM thread_goals"):
                data = dict(row)
                run_id = self.ensure_run("codex", str(data["thread_id"]), "main")
                self.conn.execute("UPDATE runs SET status=COALESCE(?,status) WHERE id=?", (_string(data.get("status")), run_id))
                relative = f"sqlite/goals/{stable_id(data['goal_id'])[:24]}.json"
                self.ingest_virtual_artifact(source_id, relative, "goal", json_text(data).encode(), run_id, None)

    def ingest_desktop_metadata(self) -> None:
        root = self.options.claude_app_support
        if not root or not root.is_dir():
            return
        source_id = self.source_instance("claude", root, "desktop")
        for path in sorted(root.rglob("*.json")):
            try:
                data = json.loads(path.read_bytes())
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
                self.warn(None, "desktop_metadata_invalid", f"{path}: {exc}")
                continue
            cli_id = _string(data.get("cliSessionId")) if isinstance(data, dict) else None
            run_id = None
            if cli_id:
                run_id = self.ensure_run("claude", cli_id, "main", surface="Claude Desktop", cwd=_string(data.get("cwd") or data.get("originCwd")), title=_string(data.get("title")), started_at=_string(data.get("createdAt")), metadata=data)
                self.conn.execute(
                    """UPDATE runs SET model=COALESCE(?,model),reasoning_effort=COALESCE(?,reasoning_effort),
                       status=COALESCE(?,status),updated_at=COALESCE(?,updated_at) WHERE id=?""",
                    (_string(data.get("model")), _string(data.get("effort")), "archived" if data.get("isArchived") else "active", _string(data.get("lastActivityAt")), run_id),
                )
                plan_path = _string(data.get("planPath"))
                if plan_path:
                    self.plan_links[str(Path(plan_path).expanduser().resolve())] = run_id
            self.ingest_artifact("claude", root, path, "desktop_session", run_id, None, source_id=source_id)

    def ingest_artifact(self, provider: str, root: Path, path: Path, kind: str, run_id: str | None, turn_id: str | None, *, source_id: int | None = None) -> None:
        source_id = source_id or self.source_instance(provider, root, "artifacts")
        try:
            relative = str(path.relative_to(root))
        except ValueError:
            relative = path.name
        try:
            stat = path.lstat()
        except OSError as exc:
            self.warn(None, "artifact_unreadable", f"{path}: {exc}")
            return
        if path.is_symlink():
            target = os.readlink(path)
            digest = hashlib.sha256(("symlink\0" + target).encode()).hexdigest()
            self._insert_artifact(source_id, run_id, turn_id, kind, relative, digest, 0, None, None, False, target, {})
            return
        if not path.is_file():
            return
        digest = hashlib.sha256()
        data: bytes | None = None
        try:
            with path.open("rb") as handle:
                if stat.st_size <= self.options.artifact_max_bytes:
                    data = handle.read(self.options.artifact_max_bytes + 1)
                    digest.update(data)
                    if len(data) > self.options.artifact_max_bytes:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            digest.update(chunk)
                else:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
        except OSError as exc:
            self.warn(None, "artifact_unreadable", f"{path}: {exc}")
            return
        oversized = stat.st_size > self.options.artifact_max_bytes or data is not None and len(data) > self.options.artifact_max_bytes
        if oversized and data is not None:
            data = None
        mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        sha = digest.hexdigest()
        blob_sha = None
        if data is not None:
            self.conn.execute(
                "INSERT OR IGNORE INTO blobs(sha256,size,mime_type,codec,data) VALUES(?,?,?,?,?)",
                (sha, len(data), mime, "raw", data),
            )
            blob_sha = sha
        self._insert_artifact(source_id, run_id, turn_id, kind, relative, sha, stat.st_size, mime, blob_sha, oversized, None, {})

    def ingest_virtual_artifact(self, source_id: int, relative: str, kind: str, data: bytes, run_id: str | None, turn_id: str | None) -> None:
        sha = hashlib.sha256(data).hexdigest()
        mime = "application/json"
        self.conn.execute("INSERT OR IGNORE INTO blobs(sha256,size,mime_type,codec,data) VALUES(?,?,?,?,?)", (sha, len(data), mime, "raw", data))
        self._insert_artifact(source_id, run_id, turn_id, kind, relative, sha, len(data), mime, sha, False, None, {})

    def _insert_artifact(self, source_id: int, run_id: str | None, turn_id: str | None, kind: str, relative: str, sha: str, size: int, mime: str | None, blob_sha: str | None, oversized: bool, symlink_target: str | None, metadata: dict[str, Any]) -> None:
        if run_id and not self.conn.execute("SELECT 1 FROM runs WHERE id=?", (run_id,)).fetchone():
            run_id = None
        if turn_id and not self.conn.execute("SELECT 1 FROM turns WHERE id=?", (turn_id,)).fetchone():
            turn_id = None
        artifact_id = stable_id("artifact", source_id, relative, sha)
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO artifacts(id,source_instance_id,run_id,turn_id,kind,relative_path,
                   version_sha256,size,mime_type,blob_sha256,oversized,symlink_target,metadata_json,captured_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (artifact_id, source_id, run_id, turn_id, kind, relative, sha, size, mime, blob_sha, int(oversized), symlink_target, json_text(metadata), utc_now()),
        )
        if cur.rowcount:
            self.stats.artifacts_added += 1

    def ingest_referenced_uploads(self) -> None:
        root = self.options.claude_home
        uploads = root / "uploads" if root else None
        if not uploads or not uploads.is_dir():
            return
        allowed = uploads.resolve()
        for path in sorted(self.referenced_uploads):
            try:
                resolved = path.expanduser().resolve()
            except OSError:
                continue
            if resolved.is_relative_to(allowed) and resolved.is_file():
                self.ingest_artifact("claude", root, resolved, "upload", None, None)

    def _collect_upload_paths(self, value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"path", "file_path", "filePath"} and isinstance(item, str):
                    self.referenced_uploads.add(Path(item))
                else:
                    self._collect_upload_paths(item)
        elif isinstance(value, list):
            for item in value:
                self._collect_upload_paths(item)

    def _artifact_session_link(self, path: Path) -> str | None:
        try:
            data = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None
        if not isinstance(data, dict):
            return None
        vendor = _string(data.get("cliSessionId") or data.get("sessionId") or data.get("leadSessionId"))
        return self.existing_run_id("claude", vendor, "main") if vendor else None

    def ensure_project(self, cwd: str | None, explicit_remote: str | None = None) -> str | None:
        if not cwd and not explicit_remote:
            return None
        cache_key = (cwd or "", explicit_remote)
        if cache_key in self.project_cache:
            return self.project_cache[cache_key]
        repo_root = None
        remote = explicit_remote
        path = Path(cwd).expanduser() if cwd else None
        if path and path.is_dir():
            repo_root = _git(path, "rev-parse", "--show-toplevel")
            if repo_root and not remote:
                remote = _git(Path(repo_root), "config", "--get", "remote.origin.url")
        normalized = normalize_git_remote(remote) if remote else None
        fallback = str(Path(cwd).expanduser()) if cwd else None
        identity = normalized or repo_root or fallback
        if not identity:
            self.project_cache[cache_key] = None
            return None
        project_host = "*" if normalized else self.options.host_id
        project_id = f"project:{'git' if normalized else self.options.host_id}:{stable_id(identity)[:20]}"
        display = normalized.rsplit("/", 1)[-1] if normalized else Path(repo_root or fallback or identity).name
        self.conn.execute(
            """INSERT INTO projects(id,host_id,identity,display_name,git_remote,repo_root,fallback_path)
               VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
               git_remote=COALESCE(excluded.git_remote,projects.git_remote),
               repo_root=COALESCE(excluded.repo_root,projects.repo_root),
               fallback_path=COALESCE(excluded.fallback_path,projects.fallback_path)""",
            (project_id, project_host, identity, display, normalized, repo_root, fallback),
        )
        self.project_cache[cache_key] = project_id
        return project_id

    def _resolve_workloads(self) -> None:
        self.conn.execute(
            """UPDATE workloads SET
                 title=COALESCE(title,(SELECT title FROM runs WHERE id=root_run_id)),
                 status=COALESCE((SELECT status FROM runs WHERE id=root_run_id),status),
                 project_id=COALESCE(project_id,(SELECT project_id FROM runs WHERE id=root_run_id)),
                 started_at=COALESCE(started_at,(SELECT started_at FROM runs WHERE id=root_run_id)),
                 updated_at=COALESCE((SELECT updated_at FROM runs WHERE id=root_run_id),updated_at)"""
        )

    def _mark_missing_files(self) -> None:
        if not self.source_ids:
            return
        ids = tuple(self.source_ids.values())
        placeholders = ",".join("?" for _ in ids)
        self.conn.execute(
            f"UPDATE source_files SET state='missing' WHERE source_instance_id IN ({placeholders}) AND COALESCE(last_seen_sync,-1)<>?",
            (*ids, self.sync_id),
        )

    def _probe_matches(self, path: Path, offset: int, expected: str | None) -> bool:
        if offset == 0 or not expected:
            return True
        return self._probe_hash(path, offset) == expected

    @staticmethod
    def _probe_hash(path: Path, offset: int) -> str | None:
        if offset <= 0:
            return None
        start = max(0, offset - PROBE_BYTES)
        with path.open("rb") as handle:
            handle.seek(start)
            return hashlib.sha256(handle.read(offset - start)).hexdigest()

    def warn_for_raw(self, raw_id: int, code: str, message: str, record_index: int | None = None) -> None:
        row = self.conn.execute("SELECT source_file_id FROM raw_records WHERE id=?", (raw_id,)).fetchone()
        self.warn(int(row[0]) if row else None, code, message, record_index)

    def warn(self, file_id: int | None, code: str, message: str, record_index: int | None = None) -> None:
        # SQLite treats NULLs as distinct in UNIQUE constraints, so host-level
        # warnings need an explicit existence check to remain idempotent.
        if file_id is None and self.conn.execute(
            """SELECT 1 FROM ingest_errors
                WHERE source_file_id IS NULL AND code=? AND message=?
                  AND record_index IS ?""",
            (code, message, record_index),
        ).fetchone():
            return
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO ingest_errors(sync_run_id,source_file_id,severity,code,message,record_index,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (self.sync_id or None, file_id, "warning", code, message, record_index, utc_now()),
        )
        if cur.rowcount:
            self.stats.warnings += 1


_TOKEN_COLUMNS = (
    "input_tokens", "cached_input_tokens", "cache_write_input_tokens",
    "output_tokens", "reasoning_tokens", "total_tokens",
)


def normalize_git_remote(remote: str) -> str:
    remote = remote.strip()
    scp = re.match(r"(?:[^@]+@)?([^:]+):(.+)$", remote)
    if scp and "://" not in remote and not remote.startswith("/"):
        host, path = scp.groups()
        return f"{host.lower()}/{path.removesuffix('.git').strip('/')}"
    parsed = urlsplit(remote)
    if parsed.scheme and parsed.hostname:
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.hostname.lower()}{port}/{parsed.path.removesuffix('.git').strip('/')}"
    return str(Path(remote).expanduser().resolve())


def _git(path: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ("git", "-C", str(path), *args), capture_output=True, text=True,
            timeout=3, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


def _nested(value: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _structured_paths(value: Any) -> Iterable[str]:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"path", "file_path", "filePath", "image_path", "output_path"} and isinstance(item, str):
                yield item
            else:
                yield from _structured_paths(item)
    elif isinstance(value, list):
        for item in value:
            yield from _structured_paths(item)


def _string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json_text(value)
    return str(value)


def _integer(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _nullable_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _time_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000 if value > 10_000_000_000 else float(value)
        import time
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(seconds))
    return str(value)


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _sqlite_size_mtime(path: Path) -> tuple[int, int]:
    stats = [candidate.stat() for candidate in (path, Path(f"{path}-wal")) if candidate.exists()]
    return sum(value.st_size for value in stats), max(value.st_mtime_ns for value in stats)


def _safe_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0
