<img src="docs/cairn.svg" alt="" width="72" align="left" hspace="12">

# cairn

Keep one archive of every local agent run. `cairn` reads the histories that
Codex, Claude Code, and OpenCode leave on disk and writes them into a single
SQLite file you own. That covers every Codex and Claude surface: the desktop
apps, the CLIs, the IDE extensions, and headless `codex exec`.

It runs on Python 3.11 or newer and uses only the standard library.

<br clear="left">

## What it stores

Every native JSONL record is kept byte-for-byte and compressed. On top of that,
`cairn` builds normalized tables you can query: workloads, run trees, turns,
messages, tool calls, exposed reasoning, token usage, plans, tasks, goals, and
the artifacts each harness writes.

Syncs are incremental. Re-running the same command resumes each source from a
saved offset.

## Quickstart

```sh
git clone https://github.com/batteryshark/cairn.git
cd cairn
./cairn sync ~/vault.sqlite
./cairn status ~/vault.sqlite
```

To run it from anywhere, symlink the launcher. It finds its own `src`
directory, so the symlink can live where you like:

```sh
ln -s "$PWD/cairn" ~/.local/bin/cairn
```

There is no PyPI package. Install from a checkout, or use
`python3 -m pip install -e .` if you prefer an editable install.

## Commands

```sh
cairn sync   ~/vault.sqlite
cairn status ~/vault.sqlite [--json]
cairn verify ~/vault.sqlite [--full]
cairn export ~/vault.sqlite --format unified-jsonl --output vault.jsonl
cairn export ~/vault.sqlite --format native --output native-export
```

Exit codes: `0` clean, `2` finished with warnings, `1` fatal.

## Sources

With no flags, `sync` uses whichever of these directories exist:

| Source | Path |
| --- | --- |
| Codex, every surface | `~/.codex` |
| Claude Code | `~/.claude` |
| Claude Desktop | `~/Library/Application Support/Claude/claude-code-sessions` |
| OpenCode | `$XDG_DATA_HOME/opencode` or `~/.local/share/opencode` |

Codex Desktop, the Codex CLI, the VS Code extension, and `codex exec` all write
to the same `~/.codex` root, so one path covers them. Each run keeps the
originator that produced it in `runs.surface`, for example `Codex Desktop`,
`codex_vscode`, or `codex_exec`. The Codex app's own Electron caches under
`~/Library/Application Support` hold no run content and are not read.

To import a history copied from another machine, name that machine:

```sh
cairn sync ~/vault.sqlite \
  --host-id build-mac-01 \
  --codex-home /archives/build-mac-01/.codex \
  --claude-home /archives/build-mac-01/.claude \
  --claude-app-support /archives/build-mac-01/claude-code-sessions \
  --opencode-data /archives/build-mac-01/opencode
```

Explicit paths are pinned. A missing path is an error, and any provider you do
not name is disabled for that run. That keeps an imported archive from being
silently attributed to the local machine.

## Size limits

The first sync checks free disk space. Defaults are 64 MiB per copied artifact
and 256 MiB per JSONL record:

```sh
cairn sync ~/vault.sqlite \
  --artifact-max-bytes 134217728 \
  --max-record-bytes 536870912
```

An oversized artifact keeps its metadata and SHA-256 without its bytes. An
oversized JSONL record stops that one source at the previous checkpoint so you
can retry with a larger limit. It is never skipped silently.

## Reading the vault

`status` prints archive and usage totals. `verify` runs SQLite `quick_check`,
foreign-key and run-tree checks, then decompresses and hashes every stored
record and artifact. `--full` adds SQLite's exhaustive index audit, which can
take hours on a multi-gigabyte vault.

`export --format unified-jsonl` emits versioned `project`, `workload`, `run`,
`run_edge`, `turn`, `raw_record`, `event`, `usage`, `artifact`, and
`ingest_error` envelopes with provider payloads intact and artifact bytes
base64-encoded.

`export --format native` rebuilds every fully ingested JSONL stream and stored
artifact under a new empty directory, with a manifest mapping each exported
file to its original root and path. A live JSONL tail with no trailing newline
stays out until a later sync sees it.

The database also carries views for direct SQL:

```sql
SELECT * FROM workload_summary ORDER BY updated_at DESC;
SELECT * FROM run_summary WHERE tool_calls > 0;
SELECT * FROM daily_usage ORDER BY day, provider;
```

## Coverage notes

Codex ingestion covers active and archived rollouts from every surface, the
session index, thread and turn state, spawn edges, subagent threads, plans,
goals, and thread-linked artifacts.

Claude ingestion covers project transcripts, nested subagent transcripts and
metadata, Desktop code-session links, plans, task and team records,
externalized tool results, and referenced files in the uploads directory.

OpenCode ingestion covers the SQLite session, message, and part projections,
parent sessions, text and reasoning parts, tool calls and results, per-response
tokens and cost, todos, queued inputs, context epochs, workspaces, and
externalized `tool-output` files. OpenCode's `event` table is excluded on
purpose: it is a replay journal of repeated snapshots of those same
projections, 16.8 GiB against 1.65 GiB on the store measured here, and holds no
run content the projections lack.

Ordinary consumer Claude chats, auth and configuration, global memory, caches,
debug logs, shell snapshots, and queues are excluded.

Token accounting is deliberate. Claude response usage counts once per request
even when the same usage object repeats on thinking, tool, and text blocks.
Codex turn usage is the difference between successive cumulative snapshots; the
snapshots stay archived but are never summed. Token categories stay separate
per provider because their billing semantics are not guaranteed to match.

Claude thinking blocks and signatures are kept when the transcript exposes
them. Codex reasoning summaries, encrypted payloads, and reasoning-token counts
are kept. Raw chain-of-thought that a provider never exposes cannot be
recovered.

## Windows and other machines

Build the single-file launcher once:

```sh
python3 build_zipapp.py
```

Copy `cairn.pyz` and, if you want the wrapper, `cairn.cmd` to the Windows
machine. Install Python 3.11 or newer, then:

```powershell
py -3 cairn.pyz sync D:\Vault\vault.sqlite
py -3 cairn.pyz verify D:\Vault\vault.sqlite
```

Windows defaults are `%USERPROFILE%\.codex`, `%USERPROFILE%\.claude`, and
`%USERPROFILE%\.local\share\opencode`, plus Claude Desktop metadata under
`%APPDATA%\Claude`. OpenCode uses that XDG-style path on Windows rather than
`%LOCALAPPDATA%`; run `opencode debug paths` to confirm for your version.

Use one writer per vault. To fold several machines into one file, copy the
closed vault to the next machine's local disk, run `sync` and `verify` there,
then copy it back. Do not write a live vault from two machines, and do not put
the writer database on OneDrive, SMB, or another network filesystem. Native
Windows and WSL have separate homes and separate OpenCode databases, so sync
them one after the other.

## Security

**`cairn` performs no redaction.** Messages and tool results can contain source
code, credentials, environment values, and personal information. Treat the
vault as being as sensitive as everything you have ever typed at an agent.

On POSIX systems the database, its WAL files, and exports are created
owner-only. Windows does not map that mode to a restrictive NTFS ACL, so keep
the vault in a user-only directory and use `icacls` if a custom directory
inherits wider access. Encrypt the disk that holds it.

Source histories are opened read-only and are never modified. Imported history
is append-only, and stays in the vault after the original source disappears.

## Layout

```
cairn            POSIX launcher, no install needed
cairn.cmd        Windows launcher
build_zipapp.py  builds the single-file cairn.pyz
src/cairn/       cli.py, ingest.py, db.py, reporting.py
tests/           unittest suite
```

## Development

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m cairn --help
```

## License

MIT. See [LICENSE](LICENSE).
