# pi-archive

> Minimum viable personal agent: save every message to a SQLite DB.

A [Pi](https://github.com/nichochar/pi-coding-agent) coding agent extension that creates a searchable SQLite archive of every Pi session ever. Every message (user, assistant, tool results, bash executions) is saved locally and indexed for full-text search.

The agent can search its own history via the `search_archive` tool, and you can search manually with the `/archive` command.

## Install

```sh
pi package add @gordonb/pi-archive
```

## How it works

Pi stores sessions as JSONL files on disk. This extension syncs those files into a SQLite database at `.pi/archive.db` in your project directory.

- **On session start**: all past session files for the project are synced into the database.
- **After each agent turn**: the current session file is incrementally synced.
- **On shutdown**: the database connection is closed cleanly.

Sync is idempotent — re-syncing the same file is a no-op. Appended entries are picked up incrementally by comparing file sizes.

### What's stored

Every JSONL entry is parsed into structured columns:

| Column | Description |
|---|---|
| `role` | `user`, `assistant`, `toolResult`, `bashExecution`, `custom`, … |
| `tool_name` | Tool name for tool result entries |
| `model` | Model ID (e.g. `claude-sonnet-4-5`) |
| `provider` | Provider (e.g. `anthropic`) |
| `text_content` | Extracted searchable text |
| `raw_json` | Full original JSONL line |

The tree structure of conversations is preserved via `parent_id`, so you can walk branches. Session metadata (cwd, timestamps, parent session for forks) is tracked in a separate `sessions` table.

Text content is indexed using SQLite FTS5 for fast full-text search.

## Usage

### `/archive` command

Search your archive or view stats from the Pi prompt:

```
/archive authentication JWT
/archive stats
```

### `search_archive` tool

The agent can search past conversations automatically. It's guided to use the tool when you reference past work. You can also ask it directly:

> Search our past conversations for when we discussed the database schema.

The tool accepts [FTS5 query syntax](https://www.sqlite.org/fts5.html#full_text_query_syntax):

- Phrase search: `"REST API"`
- Boolean: `authentication AND JWT`
- Prefix: `Graph*`
- Negation: `REST NOT GraphQL`

Results are ordered by timestamp (newest first) and include highlighted snippets.

### Direct SQL

The database is just a SQLite file. You can query it directly:

```sh
sqlite3 .pi/archive.db "SELECT text_content FROM entries WHERE role = 'user' ORDER BY timestamp DESC LIMIT 10"
```

## Development

Requires Node.js 22+ (uses `node:sqlite`).

```sh
npm install
npm test
```

## License

MIT
