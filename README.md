<img src="docs/cairn.svg" alt="Cairn stacked-stone logo" width="72" align="left" hspace="12">

# cairn

Cairn keeps a local archive of coding-agent work across Codex, Claude Code,
OpenCode and OpenChamber. It reads their on-disk histories into one SQLite vault,
keeping raw records alongside queryable runs, turns, tool calls and token usage.

Use it when you want to find the work behind a decision, follow a run into its
subagents, or keep a history after the original session files disappear. You can
query the vault with SQL and export its records without opening each harness.

<br clear="left">

## Why keep a separate archive?

Harness memory and project notes hold context you want an agent to use again.
Cairn preserves the recorded conversation and tool output behind that context,
along with supported plans, memories and other artifacts. Keeping both lets you
retain a short project decision and still look up how the agent reached it.

- Find a past implementation discussion across supported harnesses with a SQL
  text query, then inspect the matching run's messages and tool results.
- Review a parent run and its recorded child runs together. Run edges preserve
  spawn, fork and supported OpenChamber dispatch relationships.
- Compare recorded token usage by day and provider, with cached input and
  reasoning categories kept separate. Provider accounting semantics still differ.
- Keep versioned copies of supported memory files and artifacts, even after they
  change at the source.

Cairn has a CLI, SQL views and exports. It doesn't provide a GUI, semantic search,
an MCP server or automatic memory injection into a harness. It needs no model,
API key or backend service; the runtime uses Python's standard library.

## Start from source

You need Python 3.11 or newer with SQLite support. On POSIX systems:

```sh
git clone https://github.com/batteryshark/cairn.git
cd cairn
./cairn sync ~/vault.sqlite
./cairn status ~/vault.sqlite
```

`sync` with no source flags discovers supported histories in your home directory
and selected project files. **It performs no redaction:** the vault can contain
private conversations, credentials in tool output, source code and screenshots.
Choose a private destination with enough free space before syncing.

The first sync archives available supported records. Later syncs resume from
checkpoints. `status` reports workloads, turns, tool calls, artifacts, warnings
and usage totals. Review warnings for unreadable files or unknown record types;
a successful command doesn't establish that every source format was understood.

On Windows, use `cairn.cmd` from the checkout with the same arguments; it requires
the `py -3` launcher and Python 3.11+. See the [reference](docs/reference.md) for
source paths, copied histories, optional installation and zipapp packaging.

## Find and recover past work

Open the vault read-only in a SQLite client. For example, with the separately
installed `sqlite3` shell:

```sh
sqlite3 -readonly ~/vault.sqlite
```

```sql
-- Find recorded messages mentioning a topic. Substitute your own search text.
SELECT run_id, timestamp, role, payload_json
FROM events
WHERE kind = 'message' AND payload_json LIKE '%database migration%'
ORDER BY timestamp;

-- Inspect workload totals, including their child runs.
SELECT title, provider, run_count, tool_calls, total_tokens
FROM workload_summary ORDER BY updated_at DESC;

SELECT * FROM daily_usage ORDER BY day, provider;
```

Text matching searches the stored JSON payload, so results include its field
names as well as message text. For a matched run, use its `run_id` to filter
`events`; `run_edges` relates parent and child IDs. The [reference](docs/reference.md)
describes the schema and export formats.

```sh
./cairn verify ~/vault.sqlite
./cairn export ~/vault.sqlite --format unified-jsonl --output vault.jsonl
./cairn export ~/vault.sqlite --format native --output native-export
```

Unified JSONL carries versioned records for downstream analysis. Native export
reconstructs stored JSONL streams and artifact versions under an empty directory,
with a manifest of original paths. Database projections export as records;
Cairn doesn't restore a working harness installation or resume a session.

## Coverage and storage

Cairn reads Codex active/archived rollouts (plain, and `.jsonl.zst` with Python 3.14
or a `zstd` binary on PATH), Claude Code transcripts and subagents,
OpenCode V1/V2 database projections, and selected OpenChamber session/project
content, and Orchestra run logs (`--orchestra-root`). Desktop code-session metadata is supported; ordinary consumer Claude
chats are excluded. The [coverage reference](docs/reference.md#source-coverage)
lists artifacts, exclusions and accounting behavior.

Complete ingested JSONL records are compressed and retained byte-for-byte.
Incomplete tails wait for a later sync. Size caps and inaccessible files can
leave content unstored; exposed reasoning is preserved only as recorded, and
encrypted payloads aren't decrypted.

Use one writer per vault on a local filesystem with SQLite locking. Sync can
migrate an older vault and rebuild derived tables; back up a closed vault before
upgrading. Keep the writer database off network shares and cloud-sync folders.

## Privacy and rights

Treat the vault and every export as sensitive as the source histories. Source
databases are opened in SQLite read-only mode; sync writes the destination vault
and its sidecars. Local project identification can invoke Git metadata commands.
Default discovery reaches selected project notes, memories and chat workspaces,
so review the [discovery and security details](docs/reference.md#privacy-and-side-effects)
before collecting someone else's histories or importing a copied archive.

POSIX files are created owner-only. Windows needs a private directory with
appropriate ACLs. Cairn doesn't encrypt the vault. Credential-file exclusions
don't remove secrets embedded in messages or artifacts.

Cairn's source and documentation use the [MIT license](LICENSE). Its existing
name and stacked-stone logo are retained. Captured content keeps its own rights
and confidentiality obligations; see [RIGHTS.md](RIGHTS.md).

## Development

The package is version 0.1.0. Source lives in `src/cairn/`; the generated-fixture
unittest suite is in `tests/`. See [development and packaging](docs/reference.md#development-and-packaging)
before running tests or distributing a standalone launcher.
