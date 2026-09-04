from __future__ import annotations

import argparse
import os
import socket
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

from .db import open_database
from .ingest import DEFAULT_ARTIFACT_MAX, DEFAULT_RECORD_MAX, SyncOptions, VaultSyncer
from .reporting import export_native, export_unified, print_status, status_data, verify


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="harness-vault", description="Archive Claude Code, Codex, and OpenCode runs in one SQLite vault")
    sub = result.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync", help="incrementally ingest local or copied histories")
    sync.add_argument("vault", type=Path)
    sync.add_argument("--host-id", help="stable identity for the source machine")
    sync.add_argument("--codex-home", type=Path, help="path to a .codex directory")
    sync.add_argument("--claude-home", type=Path, help="path to a .claude directory")
    sync.add_argument("--claude-app-support", type=Path, help="path to Claude/claude-code-sessions")
    sync.add_argument("--opencode-data", type=Path, help="path to an OpenCode data directory")
    sync.add_argument("--artifact-max-bytes", type=_positive_int, default=DEFAULT_ARTIFACT_MAX)
    sync.add_argument("--max-record-bytes", type=_positive_int, default=DEFAULT_RECORD_MAX)
    sync.add_argument("--ignore-space-check", action="store_true")

    status = sub.add_parser("status", help="show archive and usage totals")
    status.add_argument("vault", type=Path)
    status.add_argument("--json", action="store_true")

    check = sub.add_parser("verify", help="verify database integrity and archived bytes")
    check.add_argument("vault", type=Path)
    check.add_argument("--full", action="store_true", help="run SQLite's much slower full index audit")

    export = sub.add_parser("export", help="export normalized JSONL or reconstructed native files")
    export.add_argument("vault", type=Path)
    export.add_argument("--format", choices=("unified-jsonl", "native"), default="unified-jsonl")
    export.add_argument("--output", required=True, help="output file, '-' for stdout, or directory for native")
    return result


def main(argv: list[str] | None = None) -> int:
    if sys.version_info < (3, 11):
        print("harness-vault requires Python 3.11 or newer", file=sys.stderr)
        return 1
    os.umask(0o077)
    args = parser().parse_args(argv)
    try:
        if args.command == "sync":
            return _sync(args)
        if args.command == "status":
            with closing(open_database(args.vault, writable=False)) as conn:
                print_status(status_data(conn), as_json=args.json)
            return 0
        if args.command == "verify":
            with closing(open_database(args.vault, writable=False)) as conn:
                errors, warnings = verify(
                    conn,
                    args.vault.expanduser().resolve(),
                    full=args.full,
                    progress=lambda message: print(message, file=sys.stderr, flush=True),
                )
            for message in errors:
                print(f"error: {message}", file=sys.stderr)
            for message in warnings:
                print(f"warning: {message}", file=sys.stderr)
            if not errors:
                print("vault verified" if not warnings else "vault verified with warnings")
            return 1 if errors else 2 if warnings else 0
        if args.command == "export":
            with closing(open_database(args.vault, writable=False)) as conn:
                if args.format == "unified-jsonl":
                    export_unified(conn, args.output)
                else:
                    if args.output == "-":
                        raise ValueError("native export requires an output directory")
                    export_native(conn, Path(args.output))
            return 0
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        print(f"harness-vault: {exc}", file=sys.stderr)
        return 1
    return 1


def _sync(args: argparse.Namespace) -> int:
    explicit = any((args.codex_home, args.claude_home, args.claude_app_support, args.opencode_data))
    if explicit and not args.host_id:
        raise ValueError("--host-id is required when importing explicit source roots")
    home = Path.home()
    if explicit:
        codex = _pinned_directory(args.codex_home, "--codex-home")
        claude = _pinned_directory(args.claude_home, "--claude-home")
        app = _pinned_directory(args.claude_app_support, "--claude-app-support")
        opencode = _pinned_directory(args.opencode_data, "--opencode-data")
    else:
        codex = _optional_directory(home / ".codex")
        claude = _optional_directory(home / ".claude")
        app = _auto_claude_app_support(home)
        opencode = _auto_opencode_data(home)
    if not any((codex, claude, app, opencode)):
        raise RuntimeError("no Codex, Claude, or OpenCode history roots were found")
    options = SyncOptions(
        db_path=args.vault.expanduser().resolve(), host_id=args.host_id or socket.gethostname(),
        codex_home=codex, claude_home=claude, claude_app_support=app, opencode_data=opencode,
        artifact_max_bytes=args.artifact_max_bytes, max_record_bytes=args.max_record_bytes,
        ignore_space_check=args.ignore_space_check,
    )
    with closing(open_database(options.db_path)) as conn:
        stats = VaultSyncer(conn, options).sync()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    print(
        f"synced {stats.files_seen} files: {stats.records_added} records, "
        f"{stats.events_added} events, {stats.artifacts_added} artifacts, {stats.warnings} warnings"
    )
    return 2 if stats.warnings else 0


def _pinned_directory(value: Path | None, option: str) -> Path | None:
    if value is None:
        return None
    path = value.expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"{option} is not a directory: {path}")
    return path


def _optional_directory(path: Path) -> Path | None:
    return path.resolve() if path.is_dir() else None


def _auto_opencode_data(home: Path) -> Path | None:
    xdg = os.environ.get("XDG_DATA_HOME")
    return _optional_directory((Path(xdg).expanduser() if xdg else home / ".local" / "share") / "opencode")


def _auto_claude_app_support(home: Path) -> Path | None:
    if sys.platform == "darwin":
        candidates = (
            home / "Library" / "Application Support" / "Claude" / "claude-code-sessions",
            home / "Library" / "Application Support" / "Claude" / "local-agent-mode-sessions",
        )
    elif os.name == "nt":
        appdata = Path(os.environ.get("APPDATA", home / "AppData" / "Roaming"))
        candidates = (appdata / "Claude" / "claude-code-sessions", appdata / "Claude" / "local-agent-mode-sessions")
    else:
        candidates = (home / ".config" / "Claude" / "local-agent-mode-sessions",)
    return next((path.resolve() for path in candidates if path.is_dir()), None)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
