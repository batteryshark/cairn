"""SQLite schema, migrations, and connection handling for a cairn vault."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2


SCHEMA = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS vault_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    host_id TEXT NOT NULL,
    files_seen INTEGER NOT NULL DEFAULT 0,
    records_added INTEGER NOT NULL DEFAULT 0,
    events_added INTEGER NOT NULL DEFAULT 0,
    artifacts_added INTEGER NOT NULL DEFAULT 0,
    warnings INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS source_instances (
    id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'opencode')),
    host_id TEXT NOT NULL,
    root_path TEXT NOT NULL,
    surface TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    UNIQUE(provider, host_id, root_path, surface)
);

CREATE TABLE IF NOT EXISTS source_files (
    id INTEGER PRIMARY KEY,
    source_instance_id INTEGER NOT NULL REFERENCES source_instances(id),
    logical_key TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    kind TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    device INTEGER,
    inode INTEGER,
    checkpoint_offset INTEGER NOT NULL DEFAULT 0,
    checkpoint_line INTEGER NOT NULL DEFAULT 0,
    checkpoint_probe_sha256 TEXT,
    last_seen_sync INTEGER REFERENCES sync_runs(id),
    state TEXT NOT NULL DEFAULT 'present',
    UNIQUE(source_instance_id, relative_path)
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    host_id TEXT NOT NULL,
    identity TEXT NOT NULL,
    display_name TEXT NOT NULL,
    git_remote TEXT,
    repo_root TEXT,
    fallback_path TEXT,
    UNIQUE(host_id, identity)
);

CREATE TABLE IF NOT EXISTS workloads (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    host_id TEXT NOT NULL,
    root_run_id TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    title TEXT,
    status TEXT,
    started_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    host_id TEXT NOT NULL,
    vendor_run_id TEXT NOT NULL,
    stream_key TEXT NOT NULL,
    workload_id TEXT REFERENCES workloads(id),
    project_id TEXT REFERENCES projects(id),
    surface TEXT,
    title TEXT,
    status TEXT,
    cwd TEXT,
    model TEXT,
    reasoning_effort TEXT,
    started_at TEXT,
    updated_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(provider, host_id, vendor_run_id, stream_key)
);

CREATE TABLE IF NOT EXISTS run_edges (
    parent_run_id TEXT NOT NULL REFERENCES runs(id),
    child_run_id TEXT NOT NULL REFERENCES runs(id),
    relation TEXT NOT NULL,
    status TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(parent_run_id, child_run_id, relation),
    CHECK(parent_run_id <> child_run_id)
);

CREATE TABLE IF NOT EXISTS turns (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    vendor_turn_id TEXT NOT NULL,
    sequence INTEGER,
    status TEXT,
    started_at TEXT,
    completed_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(run_id, vendor_turn_id)
);

CREATE TABLE IF NOT EXISTS raw_records (
    id INTEGER PRIMARY KEY,
    source_instance_id INTEGER NOT NULL REFERENCES source_instances(id),
    source_file_id INTEGER NOT NULL REFERENCES source_files(id),
    logical_key TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    raw_sha256 TEXT NOT NULL,
    raw_zlib BLOB NOT NULL,
    parsed INTEGER NOT NULL,
    parse_error TEXT,
    native_type TEXT,
    run_id TEXT REFERENCES runs(id),
    normalized INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(source_instance_id, logical_key, record_index, raw_sha256)
);

CREATE TABLE IF NOT EXISTS stream_records (
    source_instance_id INTEGER NOT NULL REFERENCES source_instances(id),
    logical_key TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    raw_record_id INTEGER NOT NULL REFERENCES raw_records(id),
    PRIMARY KEY(source_instance_id, logical_key, record_index)
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    raw_record_id INTEGER NOT NULL REFERENCES raw_records(id),
    run_id TEXT REFERENCES runs(id),
    turn_id TEXT REFERENCES turns(id),
    sub_index INTEGER NOT NULL DEFAULT 0,
    sequence INTEGER,
    timestamp TEXT,
    kind TEXT NOT NULL CHECK(kind IN (
        'instruction', 'message', 'reasoning', 'tool_call', 'tool_result',
        'usage', 'status', 'context', 'attachment', 'unknown'
    )),
    role TEXT,
    phase TEXT,
    item_type TEXT,
    tool_name TEXT,
    call_id TEXT,
    message_id TEXT,
    payload_json TEXT NOT NULL,
    UNIQUE(raw_record_id, sub_index)
);

CREATE TABLE IF NOT EXISTS usage_records (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    turn_id TEXT REFERENCES turns(id),
    provider TEXT NOT NULL,
    scope TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    sequence INTEGER,
    timestamp TEXT,
    input_tokens INTEGER,
    cached_input_tokens INTEGER,
    cache_write_input_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    total_tokens INTEGER,
    raw_json TEXT NOT NULL,
    UNIQUE(run_id, scope, provider_key)
);

CREATE TABLE IF NOT EXISTS usage_sources (
    usage_id TEXT NOT NULL REFERENCES usage_records(id) ON DELETE CASCADE,
    raw_record_id INTEGER NOT NULL REFERENCES raw_records(id),
    PRIMARY KEY(usage_id, raw_record_id)
);

CREATE TABLE IF NOT EXISTS blobs (
    sha256 TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    mime_type TEXT,
    codec TEXT NOT NULL DEFAULT 'raw',
    data BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    source_instance_id INTEGER NOT NULL REFERENCES source_instances(id),
    run_id TEXT REFERENCES runs(id),
    turn_id TEXT REFERENCES turns(id),
    kind TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    version_sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    mime_type TEXT,
    blob_sha256 TEXT REFERENCES blobs(sha256),
    oversized INTEGER NOT NULL DEFAULT 0,
    symlink_target TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    captured_at TEXT NOT NULL,
    UNIQUE(source_instance_id, relative_path, version_sha256)
);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id INTEGER PRIMARY KEY,
    sync_run_id INTEGER REFERENCES sync_runs(id),
    source_file_id INTEGER REFERENCES source_files(id),
    severity TEXT NOT NULL CHECK(severity IN ('warning', 'error')),
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    record_index INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(source_file_id, code, record_index, message)
);

CREATE INDEX IF NOT EXISTS idx_raw_run ON raw_records(run_id);
CREATE INDEX IF NOT EXISTS idx_stream_records_raw ON stream_records(raw_record_id);
CREATE INDEX IF NOT EXISTS idx_events_run_kind ON events(run_id, kind);
CREATE INDEX IF NOT EXISTS idx_events_tool_call ON events(call_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_usage_run_scope ON usage_records(run_id, scope);
CREATE INDEX IF NOT EXISTS idx_usage_sources_raw ON usage_sources(raw_record_id);
CREATE INDEX IF NOT EXISTS idx_runs_workload ON runs(workload_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id);

CREATE VIEW IF NOT EXISTS run_summary AS
WITH event_totals AS (
    SELECT run_id,
           COUNT(*) AS event_count,
           SUM(kind = 'tool_call') AS tool_calls,
           SUM(kind = 'tool_result') AS tool_results
      FROM events GROUP BY run_id
), usage_totals AS (
    SELECT run_id,
           SUM(input_tokens) AS input_tokens,
           SUM(cached_input_tokens) AS cached_input_tokens,
           SUM(cache_write_input_tokens) AS cache_write_input_tokens,
           SUM(output_tokens) AS output_tokens,
           SUM(reasoning_tokens) AS reasoning_tokens,
           SUM(total_tokens) AS total_tokens
      FROM usage_records
     WHERE (provider IN ('claude', 'opencode') AND scope = 'response')
        OR (provider = 'codex' AND scope = 'turn')
     GROUP BY run_id
)
SELECT r.id, r.provider, r.host_id, r.vendor_run_id, r.workload_id,
       r.project_id, r.surface, r.title, r.status, r.cwd, r.model,
       r.reasoning_effort, r.started_at, r.updated_at,
       COALESCE(e.event_count, 0) AS event_count,
       COALESCE(e.tool_calls, 0) AS tool_calls,
       COALESCE(e.tool_results, 0) AS tool_results,
       COALESCE(u.input_tokens, 0) AS input_tokens,
       COALESCE(u.cached_input_tokens, 0) AS cached_input_tokens,
       COALESCE(u.cache_write_input_tokens, 0) AS cache_write_input_tokens,
       COALESCE(u.output_tokens, 0) AS output_tokens,
       COALESCE(u.reasoning_tokens, 0) AS reasoning_tokens,
       COALESCE(u.total_tokens, 0) AS total_tokens
  FROM runs r
  LEFT JOIN event_totals e ON e.run_id = r.id
  LEFT JOIN usage_totals u ON u.run_id = r.id;

CREATE VIEW IF NOT EXISTS workload_summary AS
SELECT w.id, w.provider, w.host_id, w.root_run_id, w.project_id,
       w.title, w.status, w.started_at, w.updated_at,
       COUNT(r.id) AS run_count,
       MAX(0, COUNT(r.id) - 1) AS child_run_count,
       COALESCE(SUM(r.event_count), 0) AS event_count,
       COALESCE(SUM(r.tool_calls), 0) AS tool_calls,
       COALESCE(SUM(r.tool_results), 0) AS tool_results,
       COALESCE(SUM(r.input_tokens), 0) AS input_tokens,
       COALESCE(SUM(r.cached_input_tokens), 0) AS cached_input_tokens,
       COALESCE(SUM(r.cache_write_input_tokens), 0) AS cache_write_input_tokens,
       COALESCE(SUM(r.output_tokens), 0) AS output_tokens,
       COALESCE(SUM(r.reasoning_tokens), 0) AS reasoning_tokens,
       COALESCE(SUM(r.total_tokens), 0) AS total_tokens
  FROM workloads w LEFT JOIN run_summary r ON r.workload_id = w.id
 GROUP BY w.id;

CREATE VIEW IF NOT EXISTS daily_usage AS
SELECT substr(timestamp, 1, 10) AS day, provider,
       SUM(input_tokens) AS input_tokens,
       SUM(cached_input_tokens) AS cached_input_tokens,
       SUM(cache_write_input_tokens) AS cache_write_input_tokens,
       SUM(output_tokens) AS output_tokens,
       SUM(reasoning_tokens) AS reasoning_tokens,
       SUM(total_tokens) AS total_tokens
  FROM usage_records
 WHERE timestamp IS NOT NULL
   AND ((provider IN ('claude', 'opencode') AND scope = 'response')
     OR (provider = 'codex' AND scope = 'turn'))
 GROUP BY day, provider;
"""


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def stable_id(*parts: object) -> str:
    material = "\0".join(str(part) for part in parts).encode("utf-8", "surrogatepass")
    return hashlib.sha256(material).hexdigest()


def open_database(path: Path, *, writable: bool = True) -> sqlite3.Connection:
    path = path.expanduser().resolve()
    if writable:
        path.parent.mkdir(parents=True, exist_ok=True)
        created = not path.exists()
        if created:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        conn = sqlite3.connect(path, timeout=30)
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if not created and existing_tables and "vault_meta" not in existing_tables:
            conn.close()
            raise RuntimeError(f"refusing to modify a non-vault SQLite database: {path}")
        os.chmod(path, 0o600)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        current = None
        if "vault_meta" in existing_tables:
            row = conn.execute(
                "SELECT value FROM vault_meta WHERE key = 'schema_version'"
            ).fetchone()
            current = int(row[0]) if row else None
        if current == 1:
            _migrate_v1_to_v2(conn)
            current = 2
        if current is not None and current != SCHEMA_VERSION:
            conn.close()
            raise RuntimeError(
                f"unsupported vault schema {current}; expected {SCHEMA_VERSION}"
            )
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO vault_meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        conn.execute(
            "INSERT OR IGNORE INTO vault_meta(key, value) VALUES('created_at', ?)",
            (utc_now(),),
        )
        conn.commit()
        for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
            if candidate.exists():
                os.chmod(candidate, 0o600)
    else:
        if not path.is_file():
            raise FileNotFoundError(path)
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            version = conn.execute(
                "SELECT value FROM vault_meta WHERE key = 'schema_version'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise RuntimeError(f"not a cairn database: {path}") from exc
        if not version or int(version[0]) != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported or missing vault schema in {path}")
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA legacy_alter_table = ON")
    try:
        conn.executescript(
            """
            BEGIN IMMEDIATE;
            ALTER TABLE source_instances RENAME TO source_instances_v1;
            CREATE TABLE source_instances (
                id INTEGER PRIMARY KEY,
                provider TEXT NOT NULL CHECK (provider IN ('codex', 'claude', 'opencode')),
                host_id TEXT NOT NULL,
                root_path TEXT NOT NULL,
                surface TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(provider, host_id, root_path, surface)
            );
            INSERT INTO source_instances
                SELECT * FROM source_instances_v1;
            DROP TABLE source_instances_v1;
            DROP VIEW IF EXISTS daily_usage;
            DROP VIEW IF EXISTS workload_summary;
            DROP VIEW IF EXISTS run_summary;
            UPDATE vault_meta SET value='2' WHERE key='schema_version';
            COMMIT;
            """
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA legacy_alter_table = OFF")
        conn.execute("PRAGMA foreign_keys = ON")
