import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert";
import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import {
  SCHEMA,
  INSERT_ENTRY_SQL,
  INSERT_FTS_SQL,
  extractTextContent,
  extractFromContent,
  extractFromAssistantContent,
  entryToRow,
  padRow,
  searchArchive,
  getStats,
  formatSearchResults,
  formatStats,
  syncSessionFile,
  syncAllSessions,
  getSessionDir,
} from "./archive.ts";

// --- Helpers ---

/** Create an in-memory DB with the archive schema applied. */
const createTestDb = (): DatabaseSync => {
  const db = new DatabaseSync(":memory:");
  db.exec("PRAGMA foreign_keys = ON");
  db.exec(SCHEMA);
  return db;
};

/** Insert a session row. */
const insertSession = (
  db: DatabaseSync,
  sessionId: string,
  opts: { file?: string; cwd?: string; createdAt?: string } = {},
): void => {
  db.prepare(
    `INSERT INTO sessions (session_id, session_file, version, cwd, created_at)
     VALUES (?, ?, 3, ?, ?)`,
  ).run(
    sessionId,
    opts.file ?? "/tmp/test.jsonl",
    opts.cwd ?? "/test",
    opts.createdAt ?? "2026-01-01T00:00:00.000Z",
  );
};

/** Insert an entry row and sync its FTS. */
const insertEntry = (
  db: DatabaseSync,
  sessionId: string,
  entry: Record<string, unknown>,
): void => {
  const row = padRow(entryToRow(sessionId, entry));
  const result = db.prepare(INSERT_ENTRY_SQL).run(row);
  if (row.text_content != null) {
    db.prepare(INSERT_FTS_SQL).run({
      rowid: result.lastInsertRowid,
      text_content: row.text_content,
    });
  }
};

// --- Test fixtures ---

const userEntry = (id: string, parentId: string | null, text: string) => ({
  type: "message",
  id,
  parentId,
  timestamp: "2026-01-01T00:00:01.000Z",
  message: {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: 1735689601000,
  },
});

const assistantEntry = (
  id: string,
  parentId: string,
  text: string,
  opts: { model?: string; provider?: string; stopReason?: string } = {},
) => ({
  type: "message",
  id,
  parentId,
  timestamp: "2026-01-01T00:00:02.000Z",
  message: {
    role: "assistant",
    content: [
      { type: "thinking", thinking: "Let me think about this..." },
      { type: "text", text },
      {
        type: "toolCall",
        id: "call_123",
        name: "bash",
        arguments: { command: "ls" },
      },
    ],
    provider: opts.provider ?? "anthropic",
    model: opts.model ?? "claude-sonnet-4-5",
    stopReason: opts.stopReason ?? "stop",
    usage: {
      input: 100,
      output: 50,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 150,
      cost: {
        input: 0.001,
        output: 0.0005,
        cacheRead: 0,
        cacheWrite: 0,
        total: 0.0015,
      },
    },
    timestamp: 1735689602000,
  },
});

const toolResultEntry = (
  id: string,
  parentId: string,
  toolName: string,
  text: string,
  isError = false,
) => ({
  type: "message",
  id,
  parentId,
  timestamp: "2026-01-01T00:00:03.000Z",
  message: {
    role: "toolResult",
    toolCallId: "call_123",
    toolName,
    content: [{ type: "text", text }],
    isError,
    timestamp: 1735689603000,
  },
});

// --- Tests ---

describe("extractFromContent", () => {
  it("extracts from a plain string", () => {
    assert.strictEqual(extractFromContent("hello"), "hello");
  });

  it("returns null for empty string", () => {
    assert.strictEqual(extractFromContent(""), null);
  });

  it("extracts from text content blocks", () => {
    const blocks = [
      { type: "text", text: "hello" },
      { type: "text", text: "world" },
    ];
    assert.strictEqual(extractFromContent(blocks), "hello\nworld");
  });

  it("skips non-text blocks", () => {
    const blocks = [
      { type: "text", text: "hello" },
      { type: "image", data: "abc", mimeType: "image/png" },
    ];
    assert.strictEqual(extractFromContent(blocks), "hello");
  });

  it("returns null for empty array", () => {
    assert.strictEqual(extractFromContent([]), null);
  });

  it("returns null for non-string non-array", () => {
    assert.strictEqual(extractFromContent(42), null);
    assert.strictEqual(extractFromContent(null), null);
    assert.strictEqual(extractFromContent(undefined), null);
  });
});

describe("extractFromAssistantContent", () => {
  it("extracts only text blocks, skipping thinking and tool calls", () => {
    const content = [
      { type: "thinking", thinking: "Let me think..." },
      { type: "text", text: "Here is my answer" },
      { type: "toolCall", id: "c1", name: "bash", arguments: {} },
      { type: "text", text: "and more" },
    ];
    assert.strictEqual(
      extractFromAssistantContent(content),
      "Here is my answer\nand more",
    );
  });

  it("returns null for undefined", () => {
    assert.strictEqual(extractFromAssistantContent(undefined), null);
  });

  it("returns null for content with only thinking/tool calls", () => {
    const content = [
      { type: "thinking", thinking: "hmm" },
      { type: "toolCall", id: "c1", name: "bash", arguments: {} },
    ];
    assert.strictEqual(extractFromAssistantContent(content), null);
  });
});

describe("extractTextContent", () => {
  it("extracts from user message with content blocks", () => {
    const entry = userEntry("a1", null, "How do I test?");
    assert.strictEqual(extractTextContent(entry), "How do I test?");
  });

  it("extracts from user message with string content", () => {
    const entry = {
      type: "message",
      id: "a1",
      parentId: null,
      timestamp: "2026-01-01T00:00:00.000Z",
      message: { role: "user", content: "plain string", timestamp: 0 },
    };
    assert.strictEqual(extractTextContent(entry), "plain string");
  });

  it("extracts text from assistant, skipping thinking and tool calls", () => {
    const entry = assistantEntry("b1", "a1", "The answer is 42");
    assert.strictEqual(extractTextContent(entry), "The answer is 42");
  });

  it("extracts from toolResult", () => {
    const entry = toolResultEntry("c1", "b1", "bash", "file.txt\nREADME.md");
    assert.strictEqual(extractTextContent(entry), "file.txt\nREADME.md");
  });

  it("extracts from bashExecution message", () => {
    const entry = {
      type: "message",
      id: "d1",
      parentId: "c1",
      timestamp: "2026-01-01T00:00:00.000Z",
      message: {
        role: "bashExecution",
        command: "ls -la",
        output: "total 0\ndrwxr-xr-x",
        exitCode: 0,
        cancelled: false,
        truncated: false,
        timestamp: 0,
      },
    };
    assert.strictEqual(
      extractTextContent(entry),
      "ls -la\ntotal 0\ndrwxr-xr-x",
    );
  });

  it("extracts from compaction entry", () => {
    const entry = {
      type: "compaction",
      id: "e1",
      parentId: "d1",
      timestamp: "2026-01-01T00:00:00.000Z",
      summary: "User discussed testing strategies",
      firstKeptEntryId: "c1",
      tokensBefore: 50000,
    };
    assert.strictEqual(
      extractTextContent(entry),
      "User discussed testing strategies",
    );
  });

  it("extracts from branch_summary entry", () => {
    const entry = {
      type: "branch_summary",
      id: "f1",
      parentId: "a1",
      timestamp: "2026-01-01T00:00:00.000Z",
      summary: "Branch explored approach A",
      fromId: "e1",
    };
    assert.strictEqual(extractTextContent(entry), "Branch explored approach A");
  });

  it("extracts from custom_message entry", () => {
    const entry = {
      type: "custom_message",
      id: "g1",
      parentId: "f1",
      timestamp: "2026-01-01T00:00:00.000Z",
      customType: "my-ext",
      content: "Injected context",
      display: true,
    };
    assert.strictEqual(extractTextContent(entry), "Injected context");
  });

  it("extracts from custom message role (inside message entry)", () => {
    const entry = {
      type: "message",
      id: "h1",
      parentId: "g1",
      timestamp: "2026-01-01T00:00:00.000Z",
      message: {
        role: "custom",
        customType: "my-ext",
        content: "Custom role content",
        display: true,
        timestamp: 0,
      },
    };
    assert.strictEqual(extractTextContent(entry), "Custom role content");
  });

  it("extracts from compactionSummary message role", () => {
    const entry = {
      type: "message",
      id: "i1",
      parentId: "h1",
      timestamp: "2026-01-01T00:00:00.000Z",
      message: {
        role: "compactionSummary",
        summary: "Summary of compacted messages",
        tokensBefore: 30000,
        timestamp: 0,
      },
    };
    assert.strictEqual(
      extractTextContent(entry),
      "Summary of compacted messages",
    );
  });

  it("returns null for model_change entry", () => {
    const entry = {
      type: "model_change",
      id: "j1",
      parentId: "i1",
      timestamp: "2026-01-01T00:00:00.000Z",
      provider: "anthropic",
      modelId: "claude-sonnet-4-5",
    };
    assert.strictEqual(extractTextContent(entry), null);
  });

  it("returns null for thinking_level_change entry", () => {
    const entry = {
      type: "thinking_level_change",
      id: "k1",
      parentId: "j1",
      timestamp: "2026-01-01T00:00:00.000Z",
      thinkingLevel: "high",
    };
    assert.strictEqual(extractTextContent(entry), null);
  });

  it("returns null for label entry", () => {
    const entry = {
      type: "label",
      id: "l1",
      parentId: "k1",
      timestamp: "2026-01-01T00:00:00.000Z",
      targetId: "a1",
      label: "checkpoint",
    };
    assert.strictEqual(extractTextContent(entry), null);
  });

  it("returns null for message entry with no message", () => {
    const entry = {
      type: "message",
      id: "m1",
      parentId: null,
      timestamp: "2026-01-01T00:00:00.000Z",
      message: undefined,
    };
    assert.strictEqual(extractTextContent(entry), null);
  });
});

describe("entryToRow", () => {
  it("maps user message entry", () => {
    const entry = userEntry("a1", null, "Hello world");
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.session_id, "session-1");
    assert.strictEqual(row.entry_id, "a1");
    assert.strictEqual(row.parent_id, null);
    assert.strictEqual(row.entry_type, "message");
    assert.strictEqual(row.role, "user");
    assert.strictEqual(row.text_content, "Hello world");
    assert.strictEqual(row.tool_name, null);
  });

  it("maps assistant message entry with model metadata", () => {
    const entry = assistantEntry("b1", "a1", "Answer", {
      model: "claude-opus-4-6",
      provider: "anthropic",
      stopReason: "toolUse",
    });
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.role, "assistant");
    assert.strictEqual(row.model, "claude-opus-4-6");
    assert.strictEqual(row.provider, "anthropic");
    assert.strictEqual(row.stop_reason, "toolUse");
  });

  it("maps toolResult entry with isError", () => {
    const entry = toolResultEntry("c1", "b1", "bash", "output", true);
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.role, "toolResult");
    assert.strictEqual(row.tool_name, "bash");
    assert.strictEqual(row.tool_call_id, "call_123");
    assert.strictEqual(row.is_error, 1);
  });

  it("maps toolResult entry with isError=false", () => {
    const entry = toolResultEntry("c1", "b1", "read", "content", false);
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.is_error, 0);
  });

  it("maps model_change entry", () => {
    const entry = {
      type: "model_change",
      id: "d1",
      parentId: "c1",
      timestamp: "2026-01-01T00:00:00.000Z",
      provider: "openai",
      modelId: "gpt-4o",
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.entry_type, "model_change");
    assert.strictEqual(row.model_change_provider, "openai");
    assert.strictEqual(row.model_change_model_id, "gpt-4o");
  });

  it("maps thinking_level_change entry", () => {
    const entry = {
      type: "thinking_level_change",
      id: "e1",
      parentId: "d1",
      timestamp: "2026-01-01T00:00:00.000Z",
      thinkingLevel: "high",
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.thinking_level, "high");
  });

  it("maps compaction entry", () => {
    const entry = {
      type: "compaction",
      id: "f1",
      parentId: "e1",
      timestamp: "2026-01-01T00:00:00.000Z",
      summary: "Discussed testing",
      firstKeptEntryId: "c1",
      tokensBefore: 50000,
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.compaction_summary, "Discussed testing");
    assert.strictEqual(row.compaction_first_kept_entry_id, "c1");
    assert.strictEqual(row.compaction_tokens_before, 50000);
  });

  it("maps branch_summary entry", () => {
    const entry = {
      type: "branch_summary",
      id: "g1",
      parentId: "a1",
      timestamp: "2026-01-01T00:00:00.000Z",
      summary: "Branch explored approach A",
      fromId: "f1",
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.branch_summary, "Branch explored approach A");
    assert.strictEqual(row.branch_from_id, "f1");
  });

  it("maps custom entry", () => {
    const entry = {
      type: "custom",
      id: "h1",
      parentId: "g1",
      timestamp: "2026-01-01T00:00:00.000Z",
      customType: "my-ext",
      data: { count: 42 },
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.custom_type, "my-ext");
  });

  it("maps custom_message entry", () => {
    const entry = {
      type: "custom_message",
      id: "i1",
      parentId: "h1",
      timestamp: "2026-01-01T00:00:00.000Z",
      customType: "my-ext",
      content: "Injected",
      display: true,
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.custom_type, "my-ext");
    assert.strictEqual(row.text_content, "Injected");
  });

  it("maps label entry", () => {
    const entry = {
      type: "label",
      id: "j1",
      parentId: "i1",
      timestamp: "2026-01-01T00:00:00.000Z",
      targetId: "a1",
      label: "checkpoint-1",
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.label_target_id, "a1");
    assert.strictEqual(row.label, "checkpoint-1");
  });

  it("maps session_info entry", () => {
    const entry = {
      type: "session_info",
      id: "k1",
      parentId: "j1",
      timestamp: "2026-01-01T00:00:00.000Z",
      name: "Refactor auth",
    };
    const row = entryToRow("session-1", entry);
    assert.strictEqual(row.session_name, "Refactor auth");
  });

  it("preserves raw_json for all entry types", () => {
    const entry = userEntry("a1", null, "Hello");
    const row = entryToRow("session-1", entry);
    const parsed = JSON.parse(row.raw_json as string);
    assert.strictEqual(parsed.id, "a1");
    assert.strictEqual(parsed.message.role, "user");
  });
});

describe("padRow", () => {
  it("fills in null defaults for missing keys", () => {
    const row = padRow({ session_id: "s1", entry_id: "e1" });
    assert.strictEqual(row.session_id, "s1");
    assert.strictEqual(row.entry_id, "e1");
    assert.strictEqual(row.role, null);
    assert.strictEqual(row.tool_name, null);
    assert.strictEqual(row.thinking_level, null);
    assert.strictEqual(row.compaction_summary, null);
  });

  it("does not overwrite provided values", () => {
    const row = padRow({ role: "user", model: "gpt-4o" });
    assert.strictEqual(row.role, "user");
    assert.strictEqual(row.model, "gpt-4o");
    assert.strictEqual(row.tool_name, null);
  });
});

describe("database schema", () => {
  it("creates all tables", () => {
    const db = createTestDb();
    const tables = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'entries_fts%' AND name != 'sqlite_sequence' ORDER BY name",
      )
      .all() as Array<{ name: string }>;
    const names = tables.map((t) => t.name);
    assert.deepStrictEqual(names, ["entries", "sessions"]);
    db.close();
  });

  it("creates FTS virtual table", () => {
    const db = createTestDb();
    const tables = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = 'entries_fts'",
      )
      .all();
    assert.strictEqual(tables.length, 1);
    db.close();
  });

  it("enforces unique session_id + entry_id", () => {
    const db = createTestDb();
    insertSession(db, "s1");
    insertEntry(db, "s1", userEntry("a1", null, "Hello"));

    // Second insert with same session_id + entry_id should be ignored (OR IGNORE)
    // We use the raw INSERT OR IGNORE here to verify the constraint
    const row = padRow(entryToRow("s1", userEntry("a1", null, "Duplicate")));
    const result = db.prepare(INSERT_ENTRY_SQL).run(row);
    assert.strictEqual(result.changes, 0); // No row inserted

    const count = (
      db.prepare("SELECT COUNT(*) AS c FROM entries").get() as { c: number }
    ).c;
    assert.strictEqual(count, 1);
    db.close();
  });

  it("is idempotent (running schema twice is safe)", () => {
    const db = createTestDb();
    // Run schema again — should not throw
    db.exec(SCHEMA);
    db.close();
  });
});

describe("insert and FTS sync", () => {
  let db: DatabaseSync;

  beforeEach(() => {
    db = createTestDb();
    insertSession(db, "s1");
  });

  afterEach(() => {
    db.close();
  });

  it("inserts a user message and indexes it in FTS", () => {
    insertEntry(db, "s1", userEntry("a1", null, "How do I write tests?"));

    const results = db
      .prepare(
        "SELECT rowid, * FROM entries_fts WHERE entries_fts MATCH 'tests'",
      )
      .all() as Array<{ text_content: string }>;

    assert.strictEqual(results.length, 1);
    assert.ok(results[0].text_content.includes("tests"));
  });

  it("does not index entries without text content", () => {
    const entry = {
      type: "model_change",
      id: "m1",
      parentId: null,
      timestamp: "2026-01-01T00:00:00.000Z",
      provider: "anthropic",
      modelId: "claude-sonnet-4-5",
    };
    insertEntry(db, "s1", entry);

    const ftsCount = (
      db.prepare("SELECT COUNT(*) AS c FROM entries_fts").get() as {
        c: number;
      }
    ).c;
    assert.strictEqual(ftsCount, 0);

    // But the entry itself is stored
    const entryCount = (
      db.prepare("SELECT COUNT(*) AS c FROM entries").get() as { c: number }
    ).c;
    assert.strictEqual(entryCount, 1);
  });

  it("preserves tree structure via parent_id", () => {
    insertEntry(db, "s1", userEntry("a1", null, "Question"));
    insertEntry(db, "s1", assistantEntry("b1", "a1", "Answer"));
    insertEntry(db, "s1", toolResultEntry("c1", "b1", "bash", "output"));

    const entries = db
      .prepare("SELECT entry_id, parent_id FROM entries ORDER BY rowid")
      .all() as Array<{ entry_id: string; parent_id: string | null }>;

    assert.strictEqual(entries[0].entry_id, "a1");
    assert.strictEqual(entries[0].parent_id, null);
    assert.strictEqual(entries[1].entry_id, "b1");
    assert.strictEqual(entries[1].parent_id, "a1");
    assert.strictEqual(entries[2].entry_id, "c1");
    assert.strictEqual(entries[2].parent_id, "b1");
  });
});

describe("searchArchive", () => {
  let db: DatabaseSync;

  beforeEach(() => {
    db = createTestDb();
    insertSession(db, "s1", {
      cwd: "/projects/myapp",
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    insertEntry(
      db,
      "s1",
      userEntry("a1", null, "How do I implement authentication?"),
    );
    insertEntry(
      db,
      "s1",
      assistantEntry("b1", "a1", "You can use JWT tokens for authentication"),
    );
    insertEntry(
      db,
      "s1",
      toolResultEntry("c1", "b1", "bash", "npm install jsonwebtoken"),
    );
    insertEntry(
      db,
      "s1",
      userEntry("d1", "c1", "What about database migrations?"),
    );
  });

  afterEach(() => {
    db.close();
  });

  it("finds entries matching a query", () => {
    const results = searchArchive(db, { query: "authentication" });
    assert.ok(results.length >= 1);
    assert.ok(results.some((r) => r.text_content.includes("authentication")));
  });

  it("returns snippets with highlights", () => {
    const results = searchArchive(db, { query: "JWT" });
    assert.ok(results.length >= 1);
    assert.ok(results[0].snippet.includes(">>>JWT<<<"));
  });

  it("filters by role", () => {
    const results = searchArchive(db, {
      query: "authentication",
      role: "assistant",
    });
    assert.ok(results.length >= 1);
    assert.ok(results.every((r) => r.role === "assistant"));
  });

  it("returns empty array for no matches", () => {
    const results = searchArchive(db, {
      query: "nonexistenttermxyz",
    });
    assert.strictEqual(results.length, 0);
  });

  it("respects limit", () => {
    const results = searchArchive(db, {
      query: "authentication OR migrations OR jsonwebtoken",
      limit: 2,
    });
    assert.ok(results.length <= 2);
  });

  it("includes session metadata in results", () => {
    const results = searchArchive(db, { query: "authentication" });
    assert.ok(results.length >= 1);
    assert.strictEqual(results[0].session_cwd, "/projects/myapp");
    assert.strictEqual(
      results[0].session_created_at,
      "2026-01-01T00:00:00.000Z",
    );
  });

  it("orders results by timestamp descending", () => {
    // Insert entries with distinct timestamps
    insertSession(db, "s2", {
      cwd: "/projects/other",
      createdAt: "2026-02-01T00:00:00.000Z",
    });
    insertEntry(db, "s2", {
      ...userEntry("x1", null, "authentication question again"),
      timestamp: "2026-02-01T00:00:01.000Z",
    });

    const results = searchArchive(db, { query: "authentication" });
    assert.ok(results.length >= 2);
    // Most recent first
    assert.ok(results[0].timestamp >= results[1].timestamp);
  });
});

describe("getStats", () => {
  it("returns correct counts for empty database", () => {
    const db = createTestDb();
    const stats = getStats(db);
    assert.strictEqual(stats.sessionCount, 0);
    assert.strictEqual(stats.entryCount, 0);
    assert.strictEqual(stats.messageCount, 0);
    assert.strictEqual(stats.oldestSession, null);
    assert.strictEqual(stats.newestSession, null);
    db.close();
  });

  it("returns correct counts for populated database", () => {
    const db = createTestDb();
    insertSession(db, "s1", { createdAt: "2026-01-01T00:00:00.000Z" });
    insertSession(db, "s2", {
      file: "/tmp/test2.jsonl",
      createdAt: "2026-06-15T00:00:00.000Z",
    });
    insertEntry(db, "s1", userEntry("a1", null, "Hello"));
    insertEntry(db, "s1", assistantEntry("b1", "a1", "Hi"));
    insertEntry(db, "s2", userEntry("c1", null, "World"));
    // Non-message entry
    insertEntry(db, "s1", {
      type: "model_change",
      id: "d1",
      parentId: "b1",
      timestamp: "2026-01-01T00:00:00.000Z",
      provider: "anthropic",
      modelId: "claude-sonnet-4-5",
    });

    const stats = getStats(db);
    assert.strictEqual(stats.sessionCount, 2);
    assert.strictEqual(stats.entryCount, 4);
    assert.strictEqual(stats.messageCount, 3);
    assert.strictEqual(stats.oldestSession, "2026-01-01T00:00:00.000Z");
    assert.strictEqual(stats.newestSession, "2026-06-15T00:00:00.000Z");
    db.close();
  });
});

describe("formatSearchResults", () => {
  it("returns 'No results found.' for empty array", () => {
    assert.strictEqual(formatSearchResults([]), "No results found.");
  });

  it("formats results with role, timestamp, and snippet", () => {
    const results: Array<{
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
    }> = [
      {
        session_id: "abcdef12-3456-7890-abcd-ef1234567890",
        entry_id: "a1b2c3d4",
        entry_type: "message",
        role: "user",
        tool_name: null,
        model: null,
        timestamp: "2026-01-01T00:00:01.000Z",
        text_content: "How do I test?",
        snippet: "How do I >>>test<<<?",
        session_cwd: "/test",
        session_created_at: "2026-01-01T00:00:00.000Z",
      },
    ];

    const output = formatSearchResults(results);
    assert.ok(output.includes("[1] user"));
    assert.ok(output.includes("2026-01-01T00:00:01.000Z"));
    assert.ok(output.includes("abcdef12..."));
    assert.ok(output.includes(">>>test<<<"));
  });

  it("includes model and tool_name when present", () => {
    const results = [
      {
        session_id: "abcdef12-3456-7890-abcd-ef1234567890",
        entry_id: "b1",
        entry_type: "message",
        role: "toolResult",
        tool_name: "bash",
        model: "claude-sonnet-4-5",
        timestamp: "2026-01-01T00:00:02.000Z",
        text_content: "output",
        snippet: ">>>output<<<",
        session_cwd: "/test",
        session_created_at: "2026-01-01T00:00:00.000Z",
      },
    ];

    const output = formatSearchResults(results);
    assert.ok(output.includes("model: claude-sonnet-4-5"));
    assert.ok(output.includes("tool: bash"));
  });
});

describe("formatStats", () => {
  it("formats stats correctly", () => {
    const output = formatStats({
      sessionCount: 5,
      entryCount: 100,
      messageCount: 80,
      oldestSession: "2026-01-01T00:00:00.000Z",
      newestSession: "2026-06-15T00:00:00.000Z",
    });
    assert.ok(output.includes("Sessions: 5"));
    assert.ok(output.includes("Entries:  100"));
    assert.ok(output.includes("Messages: 80"));
    assert.ok(output.includes("Oldest:   2026-01-01"));
    assert.ok(output.includes("Newest:   2026-06-15"));
  });

  it("omits oldest/newest when null", () => {
    const output = formatStats({
      sessionCount: 0,
      entryCount: 0,
      messageCount: 0,
      oldestSession: null,
      newestSession: null,
    });
    assert.ok(!output.includes("Oldest"));
    assert.ok(!output.includes("Newest"));
  });
});

describe("getSessionDir", () => {
  it("encodes cwd path correctly", () => {
    const dir = getSessionDir("/Users/gordonb/Dev/pi-archive");
    assert.ok(dir.includes("--Users-gordonb-Dev-pi-archive--"));
    assert.ok(dir.includes(".pi/agent/sessions"));
  });
});

describe("syncSessionFile", () => {
  let db: DatabaseSync;
  let tmpDir: string;

  beforeEach(() => {
    db = createTestDb();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "archive-test-"));
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  const writeSessionFile = (
    filename: string,
    lines: Array<Record<string, unknown>>,
  ): string => {
    const filePath = path.join(tmpDir, filename);
    fs.writeFileSync(
      filePath,
      lines.map((l) => JSON.stringify(l)).join("\n") + "\n",
    );
    return filePath;
  };

  it("ingests a session file with header and entries", () => {
    const filePath = writeSessionFile("session.jsonl", [
      {
        type: "session",
        version: 3,
        id: "sess-001",
        timestamp: "2026-01-01T00:00:00.000Z",
        cwd: "/projects/myapp",
      },
      {
        type: "model_change",
        id: "mc01",
        parentId: null,
        timestamp: "2026-01-01T00:00:00.000Z",
        provider: "anthropic",
        modelId: "claude-sonnet-4-5",
      },
      userEntry("a1", "mc01", "Hello world"),
      assistantEntry("b1", "a1", "Hi there!"),
    ]);

    const count = syncSessionFile(db, filePath);
    assert.strictEqual(count, 3);

    // Verify session row
    const session = db
      .prepare("SELECT * FROM sessions WHERE session_id = ?")
      .get("sess-001") as Record<string, unknown>;
    assert.strictEqual(session.cwd, "/projects/myapp");
    assert.strictEqual(session.version, 3);
    assert.strictEqual(session.entry_count, 3);

    // Verify entries
    const entries = db
      .prepare("SELECT entry_id, entry_type, role FROM entries ORDER BY rowid")
      .all() as Array<Record<string, unknown>>;
    assert.strictEqual(entries.length, 3);
    assert.strictEqual(entries[0].entry_type, "model_change");
    assert.strictEqual(entries[1].role, "user");
    assert.strictEqual(entries[2].role, "assistant");
  });

  it("is idempotent (re-syncing same file adds nothing)", () => {
    const filePath = writeSessionFile("session.jsonl", [
      {
        type: "session",
        version: 3,
        id: "sess-002",
        timestamp: "2026-01-01T00:00:00.000Z",
        cwd: "/test",
      },
      userEntry("a1", null, "Hello"),
    ]);

    const count1 = syncSessionFile(db, filePath);
    assert.strictEqual(count1, 1);

    const count2 = syncSessionFile(db, filePath);
    assert.strictEqual(count2, 0);
  });

  it("incrementally syncs appended entries", () => {
    const filePath = path.join(tmpDir, "session.jsonl");

    // Initial write
    fs.writeFileSync(
      filePath,
      [
        JSON.stringify({
          type: "session",
          version: 3,
          id: "sess-003",
          timestamp: "2026-01-01T00:00:00.000Z",
          cwd: "/test",
        }),
        JSON.stringify(userEntry("a1", null, "First message")),
      ].join("\n") + "\n",
    );

    const count1 = syncSessionFile(db, filePath);
    assert.strictEqual(count1, 1);

    // Append more entries (simulating pi appending to JSONL)
    fs.appendFileSync(
      filePath,
      JSON.stringify(assistantEntry("b1", "a1", "First reply")) +
        "\n" +
        JSON.stringify(userEntry("c1", "b1", "Second message")) +
        "\n",
    );

    const count2 = syncSessionFile(db, filePath);
    assert.strictEqual(count2, 2);

    const total = (
      db.prepare("SELECT COUNT(*) AS c FROM entries").get() as { c: number }
    ).c;
    assert.strictEqual(total, 3);
  });

  it("indexes text content in FTS during sync", () => {
    const filePath = writeSessionFile("session.jsonl", [
      {
        type: "session",
        version: 3,
        id: "sess-004",
        timestamp: "2026-01-01T00:00:00.000Z",
        cwd: "/test",
      },
      userEntry("a1", null, "How do I implement authentication?"),
      assistantEntry("b1", "a1", "Use JWT tokens for secure authentication"),
    ]);

    syncSessionFile(db, filePath);

    const results = searchArchive(db, { query: "authentication" });
    assert.strictEqual(results.length, 2);
  });

  it("skips files without a session header", () => {
    const filePath = writeSessionFile("bad.jsonl", [
      userEntry("a1", null, "No header"),
    ]);

    const count = syncSessionFile(db, filePath);
    assert.strictEqual(count, 0);
  });

  it("skips malformed lines gracefully", () => {
    const filePath = path.join(tmpDir, "session.jsonl");
    fs.writeFileSync(
      filePath,
      [
        JSON.stringify({
          type: "session",
          version: 3,
          id: "sess-005",
          timestamp: "2026-01-01T00:00:00.000Z",
          cwd: "/test",
        }),
        "this is not json",
        JSON.stringify(userEntry("a1", null, "Valid entry")),
        "{bad json",
        JSON.stringify(assistantEntry("b1", "a1", "Also valid")),
      ].join("\n") + "\n",
    );

    const count = syncSessionFile(db, filePath);
    assert.strictEqual(count, 2);
  });

  it("handles session with parentSession (fork)", () => {
    const filePath = writeSessionFile("fork.jsonl", [
      {
        type: "session",
        version: 3,
        id: "sess-fork",
        timestamp: "2026-01-01T00:00:00.000Z",
        cwd: "/test",
        parentSession: "/path/to/original.jsonl",
      },
      userEntry("a1", null, "Forked session"),
    ]);

    syncSessionFile(db, filePath);

    const session = db
      .prepare("SELECT * FROM sessions WHERE session_id = ?")
      .get("sess-fork") as Record<string, unknown>;
    assert.strictEqual(session.parent_session, "/path/to/original.jsonl");
  });
});

describe("syncAllSessions", () => {
  let db: DatabaseSync;
  let tmpDir: string;

  beforeEach(() => {
    db = createTestDb();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "archive-test-sessions-"));
  });

  afterEach(() => {
    db.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns 0 for nonexistent session directory", () => {
    // syncAllSessions uses getSessionDir which hardcodes ~/.pi/agent/sessions/
    // We test the 0-return path for a cwd whose session dir doesn't exist
    const count = syncAllSessions(db, "/nonexistent/path/that/wont/match");
    assert.strictEqual(count, 0);
  });
});

describe("FTS search features", () => {
  let db: DatabaseSync;

  beforeEach(() => {
    db = createTestDb();
    insertSession(db, "s1");
    insertEntry(
      db,
      "s1",
      userEntry("a1", null, "How do I implement a REST API with Express?"),
    );
    insertEntry(
      db,
      "s1",
      assistantEntry(
        "b1",
        "a1",
        "You can create a REST API using Express.js framework",
      ),
    );
    insertEntry(
      db,
      "s1",
      userEntry("c1", "b1", "What about GraphQL instead of REST?"),
    );
    insertEntry(
      db,
      "s1",
      assistantEntry(
        "d1",
        "c1",
        "GraphQL provides a more flexible query language",
      ),
    );
  });

  afterEach(() => {
    db.close();
  });

  it("supports phrase search", () => {
    const results = searchArchive(db, { query: '"REST API"' });
    assert.ok(results.length >= 1);
    assert.ok(results.every((r) => r.text_content.includes("REST API")));
  });

  it("supports OR queries", () => {
    const results = searchArchive(db, { query: "Express OR GraphQL" });
    assert.ok(results.length >= 2);
  });

  it("supports prefix queries", () => {
    const results = searchArchive(db, { query: "Graph*" });
    assert.ok(results.length >= 1);
    assert.ok(results.some((r) => r.text_content.includes("GraphQL")));
  });

  it("supports NOT queries", () => {
    const results = searchArchive(db, { query: "REST NOT GraphQL" });
    assert.ok(results.length >= 1);
    assert.ok(results.every((r) => !r.text_content.includes("GraphQL")));
  });
});

describe("tree structure queries", () => {
  let db: DatabaseSync;

  beforeEach(() => {
    db = createTestDb();
    insertSession(db, "s1");

    // Build a branching tree:
    //   a1 (user) -> b1 (assistant) -> c1 (user) -> d1 (assistant)
    //                                └> e1 (branch_summary) -> f1 (user)
    insertEntry(db, "s1", userEntry("a1", null, "Start"));
    insertEntry(db, "s1", assistantEntry("b1", "a1", "Response 1"));
    insertEntry(db, "s1", userEntry("c1", "b1", "Branch A follow-up"));
    insertEntry(db, "s1", assistantEntry("d1", "c1", "Branch A response"));
    insertEntry(db, "s1", {
      type: "branch_summary",
      id: "e1",
      parentId: "b1",
      timestamp: "2026-01-01T00:00:05.000Z",
      summary: "Explored Branch A approach",
      fromId: "d1",
    });
    insertEntry(db, "s1", userEntry("f1", "e1", "Branch B follow-up"));
  });

  afterEach(() => {
    db.close();
  });

  it("can walk from leaf to root via parent_id", () => {
    // Walk from f1 back to root
    const walkBranch = (entryId: string): string[] => {
      const ids: string[] = [];
      let current: string | null = entryId;
      while (current) {
        ids.push(current);
        const entry = db
          .prepare(
            "SELECT parent_id FROM entries WHERE session_id = ? AND entry_id = ?",
          )
          .get("s1", current) as { parent_id: string | null } | undefined;
        current = entry?.parent_id ?? null;
      }
      return ids;
    };

    const branchB = walkBranch("f1");
    assert.deepStrictEqual(branchB, ["f1", "e1", "b1", "a1"]);

    const branchA = walkBranch("d1");
    assert.deepStrictEqual(branchA, ["d1", "c1", "b1", "a1"]);
  });

  it("can find children of a node", () => {
    const children = db
      .prepare(
        "SELECT entry_id FROM entries WHERE session_id = ? AND parent_id = ? ORDER BY timestamp",
      )
      .all("s1", "b1") as Array<{ entry_id: string }>;

    const childIds = children.map((c) => c.entry_id);
    assert.ok(childIds.includes("c1")); // Branch A
    assert.ok(childIds.includes("e1")); // Branch B (via branch_summary)
  });

  it("can find branch_summary entries", () => {
    const summaries = db
      .prepare(
        "SELECT entry_id, branch_summary, branch_from_id FROM entries WHERE session_id = ? AND entry_type = 'branch_summary'",
      )
      .all("s1") as Array<{
      entry_id: string;
      branch_summary: string;
      branch_from_id: string;
    }>;

    assert.strictEqual(summaries.length, 1);
    assert.strictEqual(
      summaries[0].branch_summary,
      "Explored Branch A approach",
    );
    assert.strictEqual(summaries[0].branch_from_id, "d1");
  });
});
