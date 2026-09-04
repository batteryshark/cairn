# harness-vault

`harness-vault` incrementally archives local Claude Code, Codex CLI/Desktop,
and OpenCode
runs in one SQLite database. It keeps each native JSONL record byte-for-byte,
then adds normalized workloads, run trees, turns, tools, exposed reasoning,
token usage, plans, tasks/goals, and harness-owned artifacts for reporting.

The program uses only Python 3.11+ and its standard library.

## Install and sync

Run directly without downloading build or runtime packages:

```sh
./harness-vault sync /path/to/vault.sqlite
```

Optionally expose that launcher on your path with a symlink (the symlink may
live elsewhere; the launcher resolves the repository containing `src`):

```sh
ln -s "$PWD/harness-vault" ~/.local/bin/harness-vault
```

Environments with standard Python build tooling can instead use
`python3 -m pip install -e .`.

With no source flags, `sync` discovers these local roots when present:

- `~/.codex`
- `~/.claude`
- `~/Library/Application Support/Claude/claude-code-sessions`
- `$XDG_DATA_HOME/opencode` or `~/.local/share/opencode`

To import a copied history, identify its originating machine explicitly:

```sh
harness-vault sync vault.sqlite \
  --host-id build-mac-01 \
  --codex-home /archives/build-mac-01/.codex \
  --claude-home /archives/build-mac-01/.claude \
  --claude-app-support /archives/build-mac-01/claude-code-sessions \
  --opencode-data /archives/build-mac-01/opencode
```

Explicit paths are pinned: a missing path is an error, and unspecified providers
are disabled for that invocation. Re-running the same command resumes complete
JSONL lines from saved offsets. A truncated or rewritten prefix is rescanned and
deduplicated; records already moved into an archive directory are not duplicated.

The first backfill performs a conservative free-space check. The default limits
are 64 MiB per copied artifact and 256 MiB per JSONL record:

```sh
harness-vault sync vault.sqlite \
  --artifact-max-bytes 134217728 \
  --max-record-bytes 536870912
```

Oversized artifacts retain metadata and a SHA-256 hash without bytes. An
oversized JSONL record stops that source at the preceding checkpoint so it can
be retried with a larger explicit limit; it is never silently skipped.

## Track and export

```sh
harness-vault status vault.sqlite
harness-vault status vault.sqlite --json
harness-vault verify vault.sqlite
harness-vault verify vault.sqlite --full
harness-vault export vault.sqlite --format unified-jsonl --output vault.jsonl
harness-vault export vault.sqlite --format native --output native-export
```

The default verifier runs SQLite `quick_check`, foreign-key and run-tree checks,
then decompresses and SHA-256 checks every native record and stored artifact. It
prints progress by stage and record count. Add `--full` for SQLite's exhaustive
index/table consistency audit; on multi-gigabyte vaults that audit can take
hours even when healthy.

`unified-jsonl` emits versioned `project`, `workload`, `run`, `run_edge`,
`turn`, `raw_record`, `event`, `usage`, `artifact`, and `ingest_error` envelopes.
Provider payloads remain present. Stored artifact bytes are base64 encoded.

`native` reconstructs every fully ingested JSONL stream and stored artifact
version under a new, empty directory, with a manifest mapping exported files to
their original roots and paths. Incomplete live JSONL tails are intentionally
absent until a later sync observes their newline.

The database also exposes stable views for direct SQL reporting:

```sql
SELECT * FROM workload_summary ORDER BY updated_at DESC;
SELECT * FROM run_summary WHERE tool_calls > 0;
SELECT * FROM daily_usage ORDER BY day, provider;
```

## Captured data

Codex ingestion covers active and archived rollouts, the session index,
thread/turn state, spawn edges, plans, goals, and thread-linked artifacts.
Claude ingestion covers project transcripts, nested subagent transcripts and
metadata, Desktop Code-session links, plans, tasks/team records, externalized
tool results, and referenced files inside the Claude uploads directory.

OpenCode ingestion covers SQLite session/message/part projections, explicit
parent sessions, text and reasoning parts, tool calls/results, per-response
tokens and cost payloads, todos, queued inputs, context epochs, workspaces, and
externalized `tool-output` files. OpenCode's internal `event` table is excluded:
it is an event-sourcing replay journal containing repeated historical snapshots
of those same projections (16.8 GiB versus 1.65 GiB on the measured store), not
additional exposed run content. The projection rows themselves are preserved as
canonical JSON records with their original JSON payload strings intact.

Ordinary consumer Claude chats, auth/configuration, global memory, caches,
debug logs, shell snapshots, and queues are intentionally excluded.

## Windows and cross-machine use

Build the dependency-free single-file launcher once:

```sh
python3 build_zipapp.py
```

Copy `harness-vault.pyz` (and optionally `harness-vault.cmd`) to Windows, install
Python 3.11 or newer, then run in PowerShell or Command Prompt:

```powershell
py -3 harness-vault.pyz sync D:\HarnessVault\vault.sqlite
py -3 harness-vault.pyz verify D:\HarnessVault\vault.sqlite
```

The Windows defaults are `%USERPROFILE%\.codex`, `%USERPROFILE%\.claude`, and
`%USERPROFILE%\.local\share\opencode`. Claude Desktop metadata is also checked
under `%APPDATA%\Claude`. OpenCode deliberately uses this XDG-style home on
Windows rather than `%LOCALAPPDATA%`; `opencode debug paths` prints the paths
used by the installed version.

For one combined vault, keep one writer: copy the closed/checkpointed SQLite
file to the Windows machine's local disk, run `sync` and `verify`, then copy it
back. Do not write a live vault from two machines or place the writer database
on OneDrive, SMB, or another network filesystem. Native Windows and WSL have
separate home directories and OpenCode databases; sync them separately and
sequentially, moving the closed vault onto the local filesystem of the
environment doing the write.

Claude response usage is counted once per message/request even when the same
usage object appears on separate thinking, tool, and text blocks. Codex turn
usage is calculated from successive final cumulative snapshots; the snapshots
themselves remain archived but are never summed. Provider token categories are
reported separately because they are not guaranteed to have identical billing
semantics.

Claude thinking blocks and signatures are preserved when the transcript exposes
them. Codex reasoning summaries, encrypted payloads, and reasoning-token counts
are preserved; unavailable raw chain-of-thought cannot be reconstructed.

## Security and operating model

The vault deliberately performs no redaction. Messages and tool results can
contain source code, credentials, environment values, or personal information.
On POSIX systems the database, WAL files, and exports are created owner-only.
Windows does not map that mode to a restrictive NTFS ACL, so keep the vault in a
user-only directory (and use `icacls` when a custom directory inherits broader
access). The surrounding disk and backups should be encrypted and
access-controlled; BitLocker is the normal Windows choice.

Use one writer per vault. Copy a closed/checkpointed database for read-only team
reporting or backup; do not put a live multi-writer vault on network storage.
Source histories are opened read-only and are never altered. Imported history is
append-only even when an original source later disappears.

## Development

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m harness_vault --help
```

Exit codes are `0` for clean success, `2` for completed work with warnings, and
`1` for fatal failure.
