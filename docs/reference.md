# Cairn reference

## Commands

```sh
./cairn sync   ~/vault.sqlite
./cairn status ~/vault.sqlite --json
./cairn verify ~/vault.sqlite
./cairn verify ~/vault.sqlite --full
./cairn export ~/vault.sqlite --format unified-jsonl --output vault.jsonl
./cairn export ~/vault.sqlite --format native --output native-export
```

Exit codes are `0` for clean completion, `2` for completion with warnings and `1`
for fatal errors. `status --json` includes the latest sync, provider/run counts,
event kinds, artifacts, missing sources, unresolved children and token totals.
For ingestion warning details, inspect the vault's `ingest_errors` table.

`verify` runs SQLite `quick_check`, foreign-key and run-tree checks, then
decompresses and hashes stored records and hashes artifact blobs. `--full` uses
SQLite's exhaustive index audit and can be slow on large vaults. These checks
verify stored data; they don't establish complete acquisition of the sources.

## Source selection

Without source flags, `sync` uses existing default directories:

| Source | Default |
| --- | --- |
| Codex | `~/.codex` |
| Claude Code | `~/.claude` |
| Claude Desktop code sessions, macOS | `~/Library/Application Support/Claude/claude-code-sessions`, then `local-agent-mode-sessions` |
| Claude Desktop code sessions, Windows | `%APPDATA%\Claude\claude-code-sessions`, then `local-agent-mode-sessions` |
| Claude code-session metadata, other platforms | `~/.config/Claude/local-agent-mode-sessions` |
| OpenCode | `$XDG_DATA_HOME/opencode` or `~/.local/share/opencode` |
| OpenChamber | `$OPENCHAMBER_DATA_DIR` or `~/.config/openchamber` |

OpenChamber chat workspaces under its data directory's `chats/` and an optional
`$OPENCHAMBER_CHATS_DIR` are also discovered. `OPENCODE_DB` can select a source
database within the chosen OpenCode root. Native Windows and WSL have separate
homes and source databases.

Codex surfaces that write to the same history root, including the CLI, Desktop,
IDE extension and `codex exec`, are read together. Recorded originator labels
are retained in `runs.surface`. Coverage depends on the on-disk format; new
harness versions may introduce records the current readers don't recognize.

For copied histories, choose explicit roots and a stable source-machine ID:

```sh
./cairn sync ~/vault.sqlite \
  --host-id build-mac-01 \
  --codex-home /archives/build-mac-01/.codex \
  --claude-home /archives/build-mac-01/.claude \
  --claude-app-support /archives/build-mac-01/claude-code-sessions \
  --opencode-data /archives/build-mac-01/opencode \
  --openchamber-data /archives/build-mac-01/openchamber
```

Use only the flags for roots you have. Once any explicit root is supplied,
`--host-id` is required, unnamed providers are disabled, and local project-folder
discovery is disabled. Missing explicit directories are errors. The default
host ID for automatic discovery is the machine hostname. Explicit selection
doesn't sandbox the reader: recorded working directories can still be examined
for local Git identity, and selected roots can contain sensitive content.

For a closed backup whose SQLite files report `database is locked` (for example,
an APFS backup mounted on Windows), `--snapshot-source-sqlite` reads temporary
local copies of source databases and their WAL/journal files. Copied SHM files
are excluded so SQLite can rebuild them. Use this only for backups no longer
being written, with temporary space for the largest database and WAL. Keep the
destination vault on a local filesystem supporting SQLite locking.

## Size limits and incremental behavior

Defaults are 64 MiB per copied artifact and 256 MiB per JSONL record:

```sh
./cairn sync ~/vault.sqlite \
  --artifact-max-bytes 134217728 \
  --max-record-bytes 536870912
```

An oversized artifact retains metadata and SHA-256 without its bytes. An oversized
JSONL record stops that source at its previous checkpoint so you can retry with
a higher limit. A live tail without a trailing newline waits for a later sync.
Malformed complete JSONL records retain their bytes and produce warnings.

The initial free-space estimate accounts for source records and capped artifacts.
`--ignore-space-check` bypasses that estimate; it doesn't reduce storage needs.
Later syncs use saved offsets and detect source rewrites. Archived record revisions
remain stored while normalized events reflect active revisions. Missing source
files are tracked without discarding their archived content.

Sync may migrate schema version 1 to 2 and rebuild affected derived projections
when reader versions change. This changes the destination vault in place. Back
up a closed vault before upgrading; don't expect derived rows to be append-only.

## Source coverage

### Codex

Active and archived rollout JSONL, session index, thread/turn state, spawn edges,
subagent threads, plans, goals and thread-linked artifacts are supported. Generated
files in `.codex/memories` and standalone snapshots of `memories_*.sqlite` are
versioned artifacts. Memory snapshots include committed WAL data and all native
tables, so their contents can be broader than normalized session data.

A forked subagent rollout can replay its parent's history. The replayed prefix
is kept as child context without counting its turns and tokens again.
Per-response `token_usage_record` entries and realtime voice transcripts are
kept as events. Turn usage is derived from successive cumulative snapshots;
the snapshots themselves are never summed.

### Claude Code

Project transcripts, nested subagent transcripts/metadata, Desktop code-session
links, plans, tasks, teams, externalized tool results and referenced files within
the uploads directory are supported. Files under
`.claude/projects/<project>/memory` or `memories`, including nested files, are
versioned byte-for-byte as shared project context without a single run attribution.
The artifact cap applies to memory files and database snapshots too.

Response usage is counted once per request when thinking, tool and text blocks
repeat the same usage object. Thinking blocks and signatures are kept if exposed
in the transcript. Ordinary consumer Claude chats aren't imported.

### OpenCode

V1 session/message/part projections and V2 `session_v2` with inline
`session_message` blocks are supported. V2 includes synthetic/system messages,
compactions, idle outcomes and agent/model/location switches. A V2 turn extends
from one user message to the next.

Both readers cover parent/fork relationships, text/reasoning, tool calls/results,
response tokens and cost, todos, queued/pending inputs, context epochs,
instruction state/blobs, workspace/worktree metadata and background-job records.
Externalized `tool-output` and background `shell` output are archived. Files are
linked to their producing run/turn when the source supplies the link; unlinked
shell output stays under its project folder.

The OpenCode table allowlist excludes the `event` replay journal, credential,
account and permission tables, and cached model catalogs in `kv`. It selects
`job.%` entries from `kv`. File-snapshot Git stores, worktree contents and logs
aren't a machine backup.

### OpenChamber

OpenChamber sessions are read from OpenCode, with `OpenChamber` surface labels
where recorded. Supported `session.create` tool-call records supply `dispatch`
edges. Its sessions-archive list sets archive status.

Selected content includes session goals, folders, archive/metadata, message
queues, pending dispatch results, project tasks/notes/todos/plans, agent memory,
extension state, walkthroughs and archived spaces. Project-less chat working
folders exclude `.git`, `node_modules`, virtualenvs and other pruned output trees.
This selection still includes arbitrary content written into those workspaces.

For automatic local discovery, project paths come from OpenChamber's project
list and recorded OpenCode directories. Cairn reads `settings.json` to obtain
that list; it doesn't archive settings/preferences as artifacts. Within projects,
it archives `.openchamber/project.json`, shared plans, agent screenshots and
`.agents/loops` definitions. A configured shared plans directory is also supported.

Dedicated password/token/key/pairing/push credentials, `environment.json`, caches,
logs, runtime state and Electron LevelDB draft stores aren't selected for archive.
Auth/configuration, skills, rules, hooks, automations, debug logs, shell snapshots
and queues outside the supported content readers are also excluded. Exclusions
can't redact copies of those values inside a selected transcript or artifact.

Across providers, exposed reasoning, summaries, encrypted payloads and usage
counts are retained as recorded. Hidden provider reasoning can't be recovered;
encrypted payloads aren't decrypted. Token categories stay separate by provider
because their billing semantics can differ.

## SQL and export formats

`src/cairn/db.py` defines schema version 2. Main tables are `projects`, `workloads`,
`runs`, `run_edges`, `turns`, `events`, `usage_records`, `usage_sources`,
`raw_records`, `stream_records`, `artifacts`, `blobs`, `source_instances`,
`source_files`, `sync_runs` and `ingest_errors`. Views are `run_summary`,
`workload_summary` and `daily_usage`. Event content is in `payload_json`;
`run_edges` has `parent_run_id`, `child_run_id` and `relation` columns.

Unified JSONL emits envelopes with `schema_version`, `record_type` and `data`.
Record types are `project`, `workload`, `run`, `run_edge`, `turn`, `raw_record`,
`event`, `usage`, `artifact` and `ingest_error`. Provider payloads are retained;
unparseable raw bytes and stored artifact bytes can be base64-encoded. File output
must be new; `--output -` writes to stdout and may expose data in terminal logs.

Native export requires a new or empty directory. It reconstructs active stored
streams under `sources/`, artifact versions under `artifact_versions/`, and a
`manifest.json` mapping files to original source roots/paths. SQLite projection
streams export as JSONL rows. They don't recreate working harness databases. Stored memory
database snapshots are separate artifacts. Oversized artifacts without bytes
can't be reconstructed from metadata alone.

## Privacy and side effects

Cairn performs no redaction or encryption. Vaults, manifests, paths, exports,
memories and screenshots can contain personal information, source code and secrets.
Don't attach them to public issues. Share a redacted error category, relevant
Python/SQLite/harness versions and a minimal synthetic reproduction instead.

Source SQLite connections request `mode=ro`; source histories aren't intentionally
modified. SQLite sidecar handling depends on the filesystem. Snapshot mode uses
temporary local database copies. Sync writes the destination database/schema,
WAL/SHM, checkpoints and archived blobs, and can rebuild derived rows.

Project identity discovery can run local `git rev-parse --show-toplevel` and
`git config --get remote.origin.url` for existing recorded working directories.
Remote URL userinfo is stripped from normalized project identity. Raw records
can still contain the original URLs. There is no model or HTTP client, network
listener or paid inference path in the shipped runtime.

Use one writer per vault. To combine machines, move a closed vault to the next
machine's local disk, sync with appropriate source identity, verify it and copy
it back while closed. Don't concurrently write it over SMB, OneDrive or another
network/cloud-synced filesystem. POSIX vault/sidecar/export files are created
owner-only; Windows permissions require a private directory and suitable NTFS
ACLs. Use disk encryption where needed.

## Development and packaging

Python 3.11+ with standard-library SQLite is required. The source launcher needs
no package installation. For an editable install into your chosen virtualenv:

```sh
python3 -m pip install -e .
```

The build backend requires `setuptools>=68`; build isolation may download it.
There are no declared runtime dependencies. The POSIX launcher resolves symlinks
to find its adjacent `src`; a symlink in an existing directory on `PATH` can expose
it elsewhere. The Windows wrapper uses adjacent `cairn.pyz` if present, otherwise
the checkout's source.

```sh
python3 build_zipapp.py
```

This creates `cairn.pyz`. Copy it with the MIT [LICENSE](../LICENSE) when
redistributing: the recipe packages source but doesn't embed that root notice.
With Python 3.11+ on Windows, invoke `py -3 cairn.pyz` followed by a command and
arguments. Third-party and captured-content terms remain separate; see
[RIGHTS.md](../RIGHTS.md).

The unittest suite constructs synthetic source histories, SQLite fixtures and
vaults. Some cases exercise migration, filesystem discovery and export. Inspect
containment before executing it: use an isolated HOME/XDG/APPDATA/TMPDIR and
source roots, deny network/process effects and mock Git metadata discovery.
The usual development command is:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Running tests or CLI commands against real history roots collects sensitive
data. A CLI help invocation also imports runtime modules; inspect source when
execution isn't authorized.
