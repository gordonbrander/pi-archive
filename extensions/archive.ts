import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type, type Static } from "@sinclair/typebox";
import { StringEnum } from "@mariozechner/pi-ai";
import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";

// --- Types ---

type EntryRow = {
  rowid: number;
  session_id: string;
  entry_id: string;
  parent_id: string | null;
  entry_type: string;
  timestamp: string;
  role: string | null;
  tool_name: string | null;
  tool_call_id: string | null;
  provider: string | null;
  model: string | null;
  stop_reason: string | null;
  is_error: number | null;
  model_change_provider: string | null;
  model_change_model_id: string | null;
  thinking_level: string | null;
  compaction_summary: string | null;
  compaction_first_kept_entry_id: string | null;
  compaction_tokens_before: number | null;
  branch_summary: string | null;
  branch_from_id: string | null;
  custom_type: string | null;
  label_target_id: string | null;
  label: string | null;
  session_name: string | null;
  text_content: string | null;
  raw_json: string;
};

// --- Schema ---

export const SCHEMA = `
CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  session_file TEXT NOT NULL,
  version INTEGER NOT NULL,
  cwd TEXT,
  created_at TEXT NOT NULL,
  parent_session TEXT,
  entry_count INTEGER DEFAULT 0,
  file_size INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS entries (
  rowid INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(session_id),
  entry_id TEXT NOT NULL,
  parent_id TEXT,
  entry_type TEXT NOT NULL,
  timestamp TEXT NOT NULL,

  role TEXT,
  tool_name TEXT,
  tool_call_id TEXT,
  provider TEXT,
  model TEXT,
  stop_reason TEXT,
  is_error INTEGER,

  model_change_provider TEXT,
  model_change_model_id TEXT,

  thinking_level TEXT,

  compaction_summary TEXT,
  compaction_first_kept_entry_id TEXT,
  compaction_tokens_before INTEGER,

  branch_summary TEXT,
  branch_from_id TEXT,

  custom_type TEXT,

  label_target_id TEXT,
  label TEXT,

  session_name TEXT,

  text_content TEXT,

  raw_json TEXT NOT NULL,

  UNIQUE(session_id, entry_id)
);

CREATE INDEX IF NOT EXISTS idx_entries_session ON entries(session_id);
CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(session_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(entry_type);
CREATE INDEX IF NOT EXISTS idx_entries_role ON entries(role);
CREATE INDEX IF NOT EXISTS idx_entries_timestamp ON entries(timestamp);

CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
  text_content
);
`;

// --- Pure functions ---

/** Extract searchable text content from a parsed JSONL entry. */
export const extractTextContent = (entry: Record<string, unknown>): string | null => {
  const type = entry.type as string;

  if (type === "message") {
    const msg = entry.message as Record<string, unknown>;
    if (!msg) return null;
    const role = msg.role as string;

    if (role === "user") {
      return extractFromContent(msg.content);
    }
    if (role === "assistant") {
      return extractFromAssistantContent(
        msg.content as Array<Record<string, unknown>>,
      );
    }
    if (role === "toolResult") {
      return extractFromContent(msg.content);
    }
    if (role === "bashExecution") {
      const command = (msg.command as string) ?? "";
      const output = (msg.output as string) ?? "";
      return [command, output].filter(Boolean).join("\n") || null;
    }
    if (role === "custom" || role === "branchSummary") {
      const content = extractFromContent(msg.content);
      const summary = msg.summary as string | undefined;
      return content ?? summary ?? null;
    }
    if (role === "compactionSummary") {
      return (msg.summary as string) ?? null;
    }
    return extractFromContent(msg.content);
  }

  if (type === "compaction") {
    return (entry.summary as string) ?? null;
  }

  if (type === "branch_summary") {
    return (entry.summary as string) ?? null;
  }

  if (type === "custom_message") {
    return extractFromContent(entry.content);
  }

  return null;
};

/** Extract text from a content field that may be a string or content block array. */
export const extractFromContent = (content: unknown): string | null => {
  if (typeof content === "string") return content || null;
  if (!Array.isArray(content)) return null;

  const texts = content
    .filter(
      (block: Record<string, unknown>) =>
        block.type === "text" && typeof block.text === "string",
    )
    .map((block: Record<string, unknown>) => block.text as string);

  return texts.length > 0 ? texts.join("\n") : null;
};

/** Extract text from assistant content blocks (skip thinking and tool calls). */
export const extractFromAssistantContent = (
  content: Array<Record<string, unknown>> | undefined,
): string | null => {
  if (!Array.isArray(content)) return null;

  const texts = content
    .filter(
      (block) => block.type === "text" && typeof block.text === "string",
    )
    .map((block) => block.text as string);

  return texts.length > 0 ? texts.join("\n") : null;
};

/** Build column values from a parsed entry for insertion. */
export const entryToRow = (
  sessionId: string,
  entry: Record<string, unknown>,
): Record<string, unknown> => {
  const type = entry.type as string;
  const base = {
    session_id: sessionId,
    entry_id: entry.id as string,
    parent_id: (entry.parentId as string) ?? null,
    entry_type: type,
    timestamp: entry.timestamp as string,
    text_content: extractTextContent(entry),
    raw_json: JSON.stringify(entry),
  };

  if (type === "message") {
    const msg = entry.message as Record<string, unknown>;
    if (!msg) return base;
    return {
      ...base,
      role: (msg.role as string) ?? null,
      tool_name: (msg.toolName as string) ?? null,
      tool_call_id: (msg.toolCallId as string) ?? null,
      provider: (msg.provider as string) ?? null,
      model: (msg.model as string) ?? null,
      stop_reason: (msg.stopReason as string) ?? null,
      is_error: msg.isError === true ? 1 : msg.isError === false ? 0 : null,
      custom_type: (msg.customType as string) ?? null,
    };
  }

  if (type === "model_change") {
    return {
      ...base,
      model_change_provider: (entry.provider as string) ?? null,
      model_change_model_id: (entry.modelId as string) ?? null,
    };
  }

  if (type === "thinking_level_change") {
    return {
      ...base,
      thinking_level: (entry.thinkingLevel as string) ?? null,
    };
  }

  if (type === "compaction") {
    return {
      ...base,
      compaction_summary: (entry.summary as string) ?? null,
      compaction_first_kept_entry_id:
        (entry.firstKeptEntryId as string) ?? null,
      compaction_tokens_before: (entry.tokensBefore as number) ?? null,
    };
  }

  if (type === "branch_summary") {
    return {
      ...base,
      branch_summary: (entry.summary as string) ?? null,
      branch_from_id: (entry.fromId as string) ?? null,
    };
  }

  if (type === "custom") {
    return {
      ...base,
      custom_type: (entry.customType as string) ?? null,
    };
  }

  if (type === "custom_message") {
    return {
      ...base,
      custom_type: (entry.customType as string) ?? null,
    };
  }

  if (type === "label") {
    return {
      ...base,
      label_target_id: (entry.targetId as string) ?? null,
      label: (entry.label as string) ?? null,
    };
  }

  if (type === "session_info") {
    return {
      ...base,
      session_name: (entry.name as string) ?? null,
    };
  }

  return base;
};

// --- Database operations ---

export const openDb = (dbPath: string): DatabaseSync => {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA foreign_keys = ON");
  db.exec(SCHEMA);
  return db;
};

export const INSERT_ENTRY_SQL = `
  INSERT OR IGNORE INTO entries (
    session_id, entry_id, parent_id, entry_type, timestamp,
    role, tool_name, tool_call_id, provider, model, stop_reason, is_error,
    model_change_provider, model_change_model_id,
    thinking_level,
    compaction_summary, compaction_first_kept_entry_id, compaction_tokens_before,
    branch_summary, branch_from_id,
    custom_type,
    label_target_id, label,
    session_name,
    text_content,
    raw_json
  ) VALUES (
    :session_id, :entry_id, :parent_id, :entry_type, :timestamp,
    :role, :tool_name, :tool_call_id, :provider, :model, :stop_reason, :is_error,
    :model_change_provider, :model_change_model_id,
    :thinking_level,
    :compaction_summary, :compaction_first_kept_entry_id, :compaction_tokens_before,
    :branch_summary, :branch_from_id,
    :custom_type,
    :label_target_id, :label,
    :session_name,
    :text_content,
    :raw_json
  )
`;

export const INSERT_FTS_SQL = `
  INSERT INTO entries_fts(rowid, text_content)
  VALUES (:rowid, :text_content)
`;

/** Fill in nulls for all optional columns so named params don't error. */
export const padRow = (row: Record<string, unknown>): Record<string, unknown> => ({
  role: null,
  tool_name: null,
  tool_call_id: null,
  provider: null,
  model: null,
  stop_reason: null,
  is_error: null,
  model_change_provider: null,
  model_change_model_id: null,
  thinking_level: null,
  compaction_summary: null,
  compaction_first_kept_entry_id: null,
  compaction_tokens_before: null,
  branch_summary: null,
  branch_from_id: null,
  custom_type: null,
  label_target_id: null,
  label: null,
  session_name: null,
  ...row,
});

/** Sync a single session file into the database. Returns count of new entries. */
export const syncSessionFile = (
  db: DatabaseSync,
  sessionFile: string,
): number => {
  const stat = fs.statSync(sessionFile);
  const fileSize = stat.size;

  // Check if we've seen this file and at what size
  const existingRow = db
    .prepare("SELECT session_id, file_size FROM sessions WHERE session_file = ?")
    .get(sessionFile) as { session_id: string; file_size: number } | undefined;

  if (existingRow && existingRow.file_size >= fileSize) {
    return 0; // No new data
  }

  const previousSize = existingRow?.file_size ?? 0;

  // Read the file content
  const content = fs.readFileSync(sessionFile, "utf-8");
  const lines = content.split("\n").filter((line) => line.trim().length > 0);

  if (lines.length === 0) return 0;

  // Parse the header (first line)
  const header = JSON.parse(lines[0]) as Record<string, unknown>;
  if (header.type !== "session") return 0;

  const sessionId = header.id as string;

  // Upsert session record
  if (!existingRow) {
    db.prepare(
      `INSERT OR IGNORE INTO sessions
        (session_id, session_file, version, cwd, created_at, parent_session, entry_count, file_size)
       VALUES (?, ?, ?, ?, ?, ?, 0, 0)`,
    ).run(
      sessionId,
      sessionFile,
      (header.version as number) ?? 1,
      (header.cwd as string) ?? null,
      header.timestamp as string,
      (header.parentSession as string) ?? null,
    );
  }

  // If we have previous data, figure out how many lines to skip.
  // We re-parse all lines but use INSERT OR IGNORE to skip duplicates.
  // For very large files we could seek, but this is simpler and correct.
  const insertEntry = db.prepare(INSERT_ENTRY_SQL);
  const insertFts = db.prepare(INSERT_FTS_SQL);

  let newCount = 0;

  // Skip header line, process all entry lines
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    let entry: Record<string, unknown>;
    try {
      entry = JSON.parse(line);
    } catch {
      continue; // Skip malformed lines
    }

    // Skip non-entry types (like session header duplicates)
    if (!entry.id || !entry.type) continue;

    const row = padRow(entryToRow(sessionId, entry));

    const result = insertEntry.run(row);
    if (result.changes > 0) {
      newCount++;
      // Sync FTS for this entry if it has text content
      if (row.text_content != null) {
        insertFts.run({
          rowid: result.lastInsertRowid,
          text_content: row.text_content,
        });
      }
    }
  }

  // Update session metadata
  db.prepare(
    `UPDATE sessions SET
      file_size = ?,
      entry_count = (SELECT COUNT(*) FROM entries WHERE session_id = ?)
     WHERE session_id = ?`,
  ).run(fileSize, sessionId, sessionId);

  return newCount;
};

/** Find the session directory for the given cwd. */
export const getSessionDir = (cwd: string): string => {
  const encoded = cwd.replace(/\//g, "-");
  return path.join(
    process.env.HOME ?? "~",
    ".pi",
    "agent",
    "sessions",
    `--${encoded}--`,
  );
};

/** Sync all session files for the project's cwd. */
export const syncAllSessions = (db: DatabaseSync, cwd: string): number => {
  const sessionDir = getSessionDir(cwd);

  if (!fs.existsSync(sessionDir)) return 0;

  const files = fs
    .readdirSync(sessionDir)
    .filter((f) => f.endsWith(".jsonl"))
    .map((f) => path.join(sessionDir, f));

  let totalNew = 0;
  for (const file of files) {
    totalNew += syncSessionFile(db, file);
  }
  return totalNew;
};

// --- Search ---

export type SearchOptions = {
  query: string;
  role?: string;
  limit?: number;
};

export type SearchResult = {
  session_id: string;
  entry_id: string;
  entry_type: string;
  role: string | null;
  tool_name: string | null;
  model: string | null;
  timestamp: string;
  text_content: string;
  snippet: string;
  session_cwd: string | null;
  session_created_at: string;
};

export const searchArchive = (
  db: DatabaseSync,
  options: SearchOptions,
): SearchResult[] => {
  const { query, role, limit = 20 } = options;

  let sql = `
    SELECT
      e.session_id,
      e.entry_id,
      e.entry_type,
      e.role,
      e.tool_name,
      e.model,
      e.timestamp,
      e.text_content,
      snippet(entries_fts, 0, '>>>', '<<<', '...', 48) AS snippet,
      s.cwd AS session_cwd,
      s.created_at AS session_created_at
    FROM entries_fts f
    JOIN entries e ON e.rowid = f.rowid
    JOIN sessions s ON s.session_id = e.session_id
    WHERE entries_fts MATCH :query
  `;

  const params: Record<string, unknown> = { query };

  if (role) {
    sql += " AND e.role = :role";
    params.role = role;
  }

  sql += " ORDER BY e.timestamp DESC LIMIT :limit";
  params.limit = limit;

  return db.prepare(sql).all(params) as SearchResult[];
};

// --- Stats ---

export type ArchiveStats = {
  sessionCount: number;
  entryCount: number;
  messageCount: number;
  oldestSession: string | null;
  newestSession: string | null;
};

export const getStats = (db: DatabaseSync): ArchiveStats => {
  const sessionCount = (
    db.prepare("SELECT COUNT(*) AS c FROM sessions").get() as { c: number }
  ).c;
  const entryCount = (
    db.prepare("SELECT COUNT(*) AS c FROM entries").get() as { c: number }
  ).c;
  const messageCount = (
    db.prepare(
      "SELECT COUNT(*) AS c FROM entries WHERE entry_type = 'message'",
    ).get() as { c: number }
  ).c;
  const oldest = db
    .prepare("SELECT MIN(created_at) AS t FROM sessions")
    .get() as { t: string | null };
  const newest = db
    .prepare("SELECT MAX(created_at) AS t FROM sessions")
    .get() as { t: string | null };

  return {
    sessionCount,
    entryCount,
    messageCount,
    oldestSession: oldest.t,
    newestSession: newest.t,
  };
};

// --- Format helpers ---

export const formatSearchResults = (results: SearchResult[]): string => {
  if (results.length === 0) return "No results found.";

  return results
    .map((r, i) => {
      const parts = [
        `[${i + 1}] ${r.role ?? r.entry_type} — ${r.timestamp}`,
      ];
      if (r.model) parts.push(`    model: ${r.model}`);
      if (r.tool_name) parts.push(`    tool: ${r.tool_name}`);
      parts.push(`    session: ${r.session_id.slice(0, 8)}...`);
      parts.push(`    ${r.snippet}`);
      return parts.join("\n");
    })
    .join("\n\n");
};

export const formatStats = (stats: ArchiveStats): string => {
  const lines = [
    `Archive stats:`,
    `  Sessions: ${stats.sessionCount}`,
    `  Entries:  ${stats.entryCount}`,
    `  Messages: ${stats.messageCount}`,
  ];
  if (stats.oldestSession)
    lines.push(`  Oldest:   ${stats.oldestSession}`);
  if (stats.newestSession)
    lines.push(`  Newest:   ${stats.newestSession}`);
  return lines.join("\n");
};

// --- Tool input type ---

const SearchToolParams = Type.Object({
  query: Type.String({
    description:
      "Full-text search query (FTS5 syntax: phrases in quotes, AND/OR/NOT, prefix*)",
  }),
  role: Type.Optional(
    StringEnum([
      "user",
      "assistant",
      "toolResult",
      "bashExecution",
      "custom",
    ] as const),
  ),
  limit: Type.Optional(
    Type.Number({ description: "Max results (default 20)" }),
  ),
});

export type SearchToolInput = Static<typeof SearchToolParams>;

// --- Extension ---

export default function (pi: ExtensionAPI) {
  let db: DatabaseSync | undefined;

  const getDbPath = (cwd: string): string =>
    path.join(cwd, ".pi", "archive.db");

  const ensureDb = (cwd: string): DatabaseSync => {
    if (!db) {
      db = openDb(getDbPath(cwd));
    }
    return db;
  };

  // Sync on session start
  pi.on("session_start", async (_event, ctx) => {
    const database = ensureDb(ctx.cwd);
    const newEntries = syncAllSessions(database, ctx.cwd);
    if (newEntries > 0) {
      ctx.ui.notify(`Archive: synced ${newEntries} new entries`, "info");
    }
  });

  // Incremental sync after each agent turn
  pi.on("agent_end", async (_event, ctx) => {
    const database = ensureDb(ctx.cwd);
    const sessionFile = ctx.sessionManager.getSessionFile();
    if (sessionFile) {
      syncSessionFile(database, sessionFile);
    }
  });

  // Close DB on shutdown
  pi.on("session_shutdown", async () => {
    if (db) {
      db.close();
      db = undefined;
    }
  });

  // Search tool for the LLM
  pi.registerTool({
    name: "search_archive",
    label: "Search Archive",
    description:
      "Search the archive of all past pi conversations in this project. " +
      "Use FTS5 query syntax: quoted phrases, AND/OR/NOT, prefix*.",
    promptSnippet:
      "Search past pi conversations by keyword or phrase",
    promptGuidelines: [
      "Use search_archive to find relevant context from previous sessions when the user references past work.",
      "Prefer specific quoted phrases over broad single-word queries.",
    ],
    parameters: SearchToolParams,

    async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
      const database = ensureDb(ctx.cwd);

      // Sync before searching to pick up latest
      syncAllSessions(database, ctx.cwd);

      const results = searchArchive(database, {
        query: params.query,
        role: params.role,
        limit: params.limit,
      });

      const text = formatSearchResults(results);

      return {
        content: [{ type: "text", text }],
        details: { resultCount: results.length },
      };
    },
  });

  // /archive command
  pi.registerCommand("archive", {
    description:
      "Search the archive or show stats. Usage: /archive [query] or /archive stats",
    handler: async (args, ctx) => {
      const database = ensureDb(ctx.cwd);

      // Sync first
      syncAllSessions(database, ctx.cwd);

      if (!args || args.trim() === "stats") {
        const stats = getStats(database);
        ctx.ui.notify(formatStats(stats), "info");
        return;
      }

      const results = searchArchive(database, { query: args.trim() });
      ctx.ui.notify(
        results.length > 0
          ? formatSearchResults(results)
          : "No results found.",
        "info",
      );
    },
  });
}
