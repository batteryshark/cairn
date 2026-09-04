from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import stat
import sys
import zlib
from pathlib import Path
from typing import Any, Callable, TextIO

from .db import SCHEMA_VERSION, json_text, utc_now


def status_data(conn: sqlite3.Connection) -> dict[str, Any]:
    latest = conn.execute("SELECT * FROM sync_runs ORDER BY id DESC LIMIT 1").fetchone()
    providers = [
        dict(row)
        for row in conn.execute(
            """SELECT r.provider, COUNT(*) AS runs,
                      SUM(NOT EXISTS(SELECT 1 FROM run_edges e WHERE e.child_run_id=r.id)) AS root_runs,
                      SUM(EXISTS(SELECT 1 FROM run_edges e WHERE e.child_run_id=r.id)) AS child_runs
                 FROM runs r GROUP BY r.provider ORDER BY r.provider"""
        )
    ]
    event_kinds = {str(row[0]): int(row[1]) for row in conn.execute("SELECT kind,COUNT(*) FROM events GROUP BY kind ORDER BY kind")}
    token_usage = [
        dict(row)
        for row in conn.execute(
            """SELECT provider,
                      COALESCE(SUM(input_tokens),0) AS input_tokens,
                      COALESCE(SUM(cached_input_tokens),0) AS cached_input_tokens,
                      COALESCE(SUM(cache_write_input_tokens),0) AS cache_write_input_tokens,
                      COALESCE(SUM(output_tokens),0) AS output_tokens,
                      COALESCE(SUM(reasoning_tokens),0) AS reasoning_tokens,
                      COALESCE(SUM(total_tokens),0) AS total_tokens
                 FROM usage_records
                WHERE (provider IN ('claude','opencode') AND scope='response')
                   OR (provider='codex' AND scope='turn')
                GROUP BY provider ORDER BY provider"""
        )
    ]
    scalar = lambda sql: int(conn.execute(sql).fetchone()[0])
    return {
        "schema_version": SCHEMA_VERSION,
        "latest_sync": dict(latest) if latest else None,
        "providers": providers,
        "workloads": scalar("SELECT COUNT(*) FROM workloads"),
        "turns": scalar("SELECT COUNT(*) FROM turns"),
        "events": scalar("SELECT COUNT(*) FROM events"),
        "event_kinds": event_kinds,
        "tool_calls": event_kinds.get("tool_call", 0),
        "tool_results": event_kinds.get("tool_result", 0),
        "artifacts": scalar("SELECT COUNT(*) FROM artifacts"),
        "stored_artifacts": scalar("SELECT COUNT(*) FROM artifacts WHERE blob_sha256 IS NOT NULL"),
        "oversized_artifacts": scalar("SELECT COUNT(*) FROM artifacts WHERE oversized"),
        "warnings": scalar("SELECT COUNT(*) FROM ingest_errors"),
        "missing_sources": scalar("SELECT COUNT(*) FROM source_files WHERE state='missing'"),
        "unresolved_children": scalar(
            """SELECT COUNT(*) FROM runs r WHERE r.stream_key<>'main'
                 AND NOT EXISTS(SELECT 1 FROM run_edges e WHERE e.child_run_id=r.id)"""
        ),
        "token_usage": token_usage,
    }


def print_status(data: dict[str, Any], *, as_json: bool = False, output: TextIO = sys.stdout) -> None:
    if as_json:
        print(json.dumps(data, indent=2, sort_keys=True), file=output)
        return
    latest = data["latest_sync"] or {}
    print(f"schema:       {data['schema_version']}", file=output)
    print(f"last sync:    {latest.get('finished_at', 'never')} ({latest.get('status', 'n/a')})", file=output)
    print(f"workloads:    {data['workloads']}", file=output)
    print(f"turns/events: {data['turns']} / {data['events']}", file=output)
    print(f"tools:        {data['tool_calls']} calls / {data['tool_results']} results", file=output)
    print(f"artifacts:    {data['artifacts']} ({data['stored_artifacts']} stored, {data['oversized_artifacts']} oversized)", file=output)
    print(f"warnings:     {data['warnings']} ({data['unresolved_children']} unresolved children, {data['missing_sources']} missing sources)", file=output)
    for provider in data["providers"]:
        print(f"{provider['provider']}: {provider['runs']} runs ({provider['child_runs']} children)", file=output)
    for usage in data["token_usage"]:
        print(
            f"{usage['provider']} tokens: {usage['total_tokens']} total, "
            f"{usage['reasoning_tokens']} reasoning, {usage['cached_input_tokens']} cached input",
            file=output,
        )


def export_unified(conn: sqlite3.Connection, output: str | Path) -> None:
    if str(output) == "-":
        handle: TextIO = sys.stdout
        close = False
    else:
        path = Path(output).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        handle = os.fdopen(fd, "w", encoding="utf-8")
        close = True
    try:
        for table, record_type in (
            ("projects", "project"),
            ("workloads", "workload"),
            ("runs", "run"),
            ("run_edges", "run_edge"),
            ("turns", "turn"),
        ):
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid"):
                _json_line(handle, record_type, dict(row))
        for row in conn.execute("SELECT * FROM raw_records ORDER BY source_instance_id,logical_key,record_index,id"):
            raw = zlib.decompress(row["raw_zlib"])
            data = {key: row[key] for key in row.keys() if key != "raw_zlib"}
            if row["parsed"]:
                try:
                    data["native"] = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data["raw_bytes_base64"] = base64.b64encode(raw).decode("ascii")
            else:
                data["raw_bytes_base64"] = base64.b64encode(raw).decode("ascii")
            _json_line(handle, "raw_record", data)
        for row in conn.execute("SELECT * FROM events ORDER BY sequence,id"):
            data = dict(row)
            data["payload"] = _json_or_text(data.pop("payload_json"))
            _json_line(handle, "event", data)
        for row in conn.execute("SELECT * FROM usage_records ORDER BY timestamp,sequence,id"):
            data = dict(row)
            data["provider_payload"] = _json_or_text(data.pop("raw_json"))
            _json_line(handle, "usage", data)
        for row in conn.execute(
            """SELECT a.*,b.codec,b.data FROM artifacts a
                 LEFT JOIN blobs b ON b.sha256=a.blob_sha256 ORDER BY a.captured_at,a.id"""
        ):
            data = {key: row[key] for key in row.keys() if key != "data"}
            data["metadata"] = _json_or_text(data.pop("metadata_json"))
            if row["data"] is not None:
                data["content_base64"] = base64.b64encode(row["data"]).decode("ascii")
            _json_line(handle, "artifact", data)
        for row in conn.execute("SELECT * FROM ingest_errors ORDER BY id"):
            _json_line(handle, "ingest_error", dict(row))
    finally:
        if close:
            handle.close()


def export_native(conn: sqlite3.Connection, output: Path) -> None:
    output = output.expanduser().resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"native export destination is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "created_at": utc_now(),
        "sources": [],
        "artifacts": [],
    }
    instances = {int(row["id"]): dict(row) for row in conn.execute("SELECT * FROM source_instances")}
    for source_file in conn.execute("SELECT * FROM source_files ORDER BY source_instance_id,relative_path"):
        instance = instances[int(source_file["source_instance_id"])]
        relative = _safe_relative(str(source_file["relative_path"]))
        destination = output / "sources" / instance["provider"] / str(instance["id"]) / relative
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "wb") as handle:
            if source_file["kind"] == "sqlite_projection":
                records = conn.execute(
                    """SELECT r.raw_zlib FROM stream_records s JOIN raw_records r ON r.id=s.raw_record_id
                        WHERE r.source_file_id=? ORDER BY r.logical_key,r.record_index,r.id""",
                    (source_file["id"],),
                )
            else:
                records = conn.execute(
                    """SELECT r.raw_zlib FROM stream_records s JOIN raw_records r ON r.id=s.raw_record_id
                        WHERE s.source_instance_id=? AND s.logical_key=? ORDER BY s.record_index""",
                    (source_file["source_instance_id"], source_file["logical_key"]),
                )
            for row in records:
                handle.write(zlib.decompress(row[0]))
        manifest["sources"].append(
            {
                "provider": instance["provider"],
                "host_id": instance["host_id"],
                "original_root": instance["root_path"],
                "original_path": source_file["relative_path"],
                "export_path": str(destination.relative_to(output)),
                "state": source_file["state"],
            }
        )
    for row in conn.execute("SELECT a.*,b.data FROM artifacts a LEFT JOIN blobs b ON b.sha256=a.blob_sha256 ORDER BY a.id"):
        record = {key: row[key] for key in row.keys() if key != "data"}
        if row["data"] is not None:
            relative = _safe_relative(str(row["relative_path"]))
            destination = output / "artifact_versions" / str(row["source_instance_id"]) / str(row["version_sha256"]) / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "wb") as handle:
                handle.write(row["data"])
            record["export_path"] = str(destination.relative_to(output))
        manifest["artifacts"].append(record)
    manifest_path = output / "manifest.json"
    fd = os.open(manifest_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


def verify(
    conn: sqlite3.Connection,
    db_path: Path,
    *,
    full: bool = False,
    progress: Callable[[str], None] | None = None,
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    report = progress or (lambda _message: None)
    check_name = "integrity_check" if full else "quick_check"
    report(f"sqlite: running {check_name}")
    integrity = conn.execute(f"PRAGMA {check_name}").fetchall()
    if len(integrity) != 1 or integrity[0][0] != "ok":
        errors.extend(f"integrity: {row[0]}" for row in integrity)
    report("sqlite: checking foreign keys")
    foreign = conn.execute("PRAGMA foreign_key_check").fetchall()
    errors.extend(f"foreign key: {tuple(row)}" for row in foreign)
    raw_total = int(conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0])
    report(f"native records: hashing 0/{raw_total}")
    for index, row in enumerate(conn.execute("SELECT id,raw_sha256,raw_zlib FROM raw_records"), 1):
        try:
            raw = zlib.decompress(row["raw_zlib"])
        except zlib.error as exc:
            errors.append(f"raw record {row['id']}: invalid zlib data: {exc}")
            continue
        if hashlib.sha256(raw).hexdigest() != row["raw_sha256"]:
            errors.append(f"raw record {row['id']}: SHA-256 mismatch")
        if index % 100_000 == 0 or index == raw_total:
            report(f"native records: hashing {index}/{raw_total}")
    blob_total = int(conn.execute("SELECT COUNT(*) FROM blobs").fetchone()[0])
    report(f"artifacts: hashing 0/{blob_total}")
    for index, row in enumerate(conn.execute("SELECT sha256,size,data FROM blobs"), 1):
        if len(row["data"]) != row["size"]:
            errors.append(f"blob {row['sha256']}: size mismatch")
        if hashlib.sha256(row["data"]).hexdigest() != row["sha256"]:
            errors.append(f"blob {row['sha256']}: SHA-256 mismatch")
        if index % 1_000 == 0 or index == blob_total:
            report(f"artifacts: hashing {index}/{blob_total}")
    report("normalized data: checking active revisions and run trees")
    inactive_events = conn.execute(
        """SELECT COUNT(*) FROM events e
             WHERE NOT EXISTS(SELECT 1 FROM stream_records s WHERE s.raw_record_id=e.raw_record_id)"""
    ).fetchone()[0]
    if inactive_events:
        errors.append(f"{inactive_events} normalized events reference inactive record revisions")
    inactive_usage = conn.execute(
        """SELECT COUNT(*) FROM usage_sources u
             WHERE NOT EXISTS(SELECT 1 FROM stream_records s WHERE s.raw_record_id=u.raw_record_id)"""
    ).fetchone()[0]
    if inactive_usage:
        errors.append(f"{inactive_usage} usage sources reference inactive record revisions")
    cycles = _run_cycles(conn)
    errors.extend(f"run edge cycle: {' -> '.join(cycle)}" for cycle in cycles)
    unresolved = conn.execute(
        """SELECT COUNT(*) FROM runs r WHERE r.stream_key<>'main'
             AND NOT EXISTS(SELECT 1 FROM run_edges e WHERE e.child_run_id=r.id)"""
    ).fetchone()[0]
    if unresolved:
        warnings.append(f"{unresolved} child runs have no parent edge")
    missing = conn.execute("SELECT COUNT(*) FROM source_files WHERE state='missing'").fetchone()[0]
    if missing:
        warnings.append(f"{missing} source file aliases are no longer present")
    if os.name != "nt":
        mode = stat.S_IMODE(db_path.stat().st_mode)
        if mode & 0o077:
            warnings.append(f"database permissions are {mode:o}; expected owner-only")
    report("verification checks complete")
    return errors, warnings


def _run_cycles(conn: sqlite3.Connection) -> list[list[str]]:
    graph: dict[str, list[str]] = {}
    for parent, child in conn.execute("SELECT parent_run_id,child_run_id FROM run_edges"):
        graph.setdefault(str(parent), []).append(str(child))
    visiting: set[str] = set()
    visited: set[str] = set()
    cycles: list[list[str]] = []

    def walk(node: str, path: list[str]) -> None:
        if node in visiting:
            start = path.index(node)
            cycles.append(path[start:] + [node])
            return
        if node in visited:
            return
        visiting.add(node)
        for child in graph.get(node, []):
            walk(child, path + [child])
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        walk(node, [node])
    return cycles


def _safe_relative(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe archived relative path: {value}")
    return path


def _json_or_text(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _json_line(handle: TextIO, record_type: str, data: dict[str, Any]) -> None:
    handle.write(json_text({"schema_version": SCHEMA_VERSION, "record_type": record_type, "data": data}))
    handle.write("\n")
