import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import Database from "better-sqlite3";
import cron from "node-cron";
import {
  Client, GatewayIntentBits, Events, ChannelType,
  REST, Routes, SlashCommandBuilder,
  ActionRowBuilder, ButtonBuilder, ButtonStyle,
  StringSelectMenuBuilder, StringSelectMenuOptionBuilder,
  type Message, type Collection, type Snowflake,
  type TextChannel, type AnyThreadChannel,
} from "discord.js";

// --- ThreadMap (SQLite-backed) ---

type ThreadEntry = {
  sessionId: string;
  cwd: string;
  model: string;
  createdAt: number;
  started: boolean;
  lastBotMessageId?: string;
  isLocalResume?: boolean;
};

type ThreadMap = Record<string, ThreadEntry>;

const DB_PATH = path.join(import.meta.dirname, "..", "threads.db");
const JSON_PATH = path.join(import.meta.dirname, "..", "thread-map.json");

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");

db.exec(`CREATE TABLE IF NOT EXISTS threads (
  threadId TEXT PRIMARY KEY,
  sessionId TEXT NOT NULL,
  cwd TEXT NOT NULL,
  model TEXT NOT NULL,
  createdAt INTEGER NOT NULL,
  started INTEGER NOT NULL DEFAULT 0,
  lastBotMessageId TEXT,
  isLocalResume INTEGER NOT NULL DEFAULT 0
)`);

// Migration: add isLocalResume column if missing
try { db.exec("ALTER TABLE threads ADD COLUMN isLocalResume INTEGER NOT NULL DEFAULT 0"); } catch { /* already exists */ }

// One-time migration from JSON → SQLite
if (fs.existsSync(JSON_PATH)) {
  try {
    const old: ThreadMap = JSON.parse(fs.readFileSync(JSON_PATH, "utf8"));
    const insert = db.prepare(
      `INSERT OR IGNORE INTO threads (threadId, sessionId, cwd, model, createdAt, started, lastBotMessageId)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    const migrate = db.transaction(() => {
      for (const [tid, e] of Object.entries(old)) {
        insert.run(tid, e.sessionId, e.cwd, e.model, e.createdAt, e.started ? 1 : 0, e.lastBotMessageId ?? null);
      }
    });
    migrate();
    fs.renameSync(JSON_PATH, JSON_PATH + ".bak");
    console.log(`[discord-cc-bot] migrated ${Object.keys(old).length} threads from JSON → SQLite`);
  } catch (err) {
    console.error("[discord-cc-bot] JSON migration failed:", err);
  }
}

// Prepared statements
const stmtGet = db.prepare("SELECT * FROM threads WHERE threadId = ?");
const stmtUpsert = db.prepare(
  `INSERT OR REPLACE INTO threads (threadId, sessionId, cwd, model, createdAt, started, lastBotMessageId, isLocalResume)
   VALUES (@threadId, @sessionId, @cwd, @model, @createdAt, @started, @lastBotMessageId, @isLocalResume)`,
);
const stmtAll = db.prepare("SELECT * FROM threads");

function rowToEntry(row: any): ThreadEntry {
  return {
    sessionId: row.sessionId,
    cwd: row.cwd,
    model: row.model,
    createdAt: row.createdAt,
    started: !!row.started,
    lastBotMessageId: row.lastBotMessageId ?? undefined,
    isLocalResume: !!row.isLocalResume,
  };
}

function loadMap(): ThreadMap {
  const map: ThreadMap = {};
  for (const row of stmtAll.all() as any[]) {
    map[row.threadId] = rowToEntry(row);
  }
  return map;
}

function saveEntry(threadId: string, entry: ThreadEntry): void {
  stmtUpsert.run({
    threadId,
    sessionId: entry.sessionId,
    cwd: entry.cwd,
    model: entry.model,
    createdAt: entry.createdAt,
    started: entry.started ? 1 : 0,
    lastBotMessageId: entry.lastBotMessageId ?? null,
    isLocalResume: entry.isLocalResume ? 1 : 0,
  });
}

function getOrCreate(map: ThreadMap, threadId: string, defaultCwd: string): ThreadEntry {
  if (!map[threadId]) {
    const row = stmtGet.get(threadId) as any;
    if (row) {
      map[threadId] = rowToEntry(row);
    } else {
      map[threadId] = {
        sessionId: crypto.randomUUID(),
        cwd: defaultCwd,
        model: "opus",
        createdAt: Date.now(),
        started: false,
      };
      saveEntry(threadId, map[threadId]);
    }
  }
  return map[threadId];
}

// --- Local Session Discovery ---

const CLAUDE_HOME = path.join(process.env.HOME ?? "", ".claude");

// --- Auth / child environment ---

// CLAUDE_CODE_OAUTH_TOKEN short-circuits ~/.claude/.credentials.json inside the
// CLI's credential resolver, and a token minted by `claude setup-token` declares
// only `user:inference`. So an inherited token silently strips the scopes a
// `claude /login` credential carries — `user:mcp_servers` among them, which is
// what gates claude.ai connectors — and nothing errors: the tool is simply
// absent. Prefer the stored credential when one exists, and blank the variable
// for the child so a value from a systemd EnvironmentFile cannot ride in on the
// `...process.env` spread.
const CREDENTIALS_PATH = path.join(CLAUDE_HOME, ".credentials.json");

function hasStoredCredentials(): boolean {
  try {
    return fs.statSync(CREDENTIALS_PATH).isFile();
  } catch {
    return false;
  }
}

// Checked per spawn rather than cached, so a `claude /login` run after the
// process started takes effect without a restart.
function childEnv(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { ...process.env, CLAUDECODE: undefined };
  if (hasStoredCredentials()) env.CLAUDE_CODE_OAUTH_TOKEN = "";
  return env;
}

/**
 * Patch session file so CC's /resume picker can discover it.
 * CC 2.1.90+ filters out sessions with entrypoint:"sdk-cli" from the picker.
 * Rewriting to "cli" makes bot sessions visible alongside interactive ones.
 */
function patchSessionEntrypoint(sessionId: string, cwd: string): void {
  try {
    const sanitized = cwd.replace(/[^a-zA-Z0-9]/g, "-");
    const projectDir = path.join(CLAUDE_HOME, "projects", sanitized);
    const sessionFile = path.join(projectDir, `${sessionId}.jsonl`);
    if (!fs.existsSync(sessionFile)) return;
    const content = fs.readFileSync(sessionFile, "utf8");
    if (!content.includes('"entrypoint":"sdk-cli"')) return;
    fs.writeFileSync(sessionFile, content.replaceAll('"entrypoint":"sdk-cli"', '"entrypoint":"cli"'));
  } catch { /* best-effort */ }
}

type LocalSession = {
  sessionId: string;
  cwd: string;
  pid?: number;
  alive: boolean;
  startedAt?: number;
  mtime: number;
  lastPrompt?: string;
};

function isProcessAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

function discoverLocalSessions(): LocalSession[] {
  const sessions: LocalSession[] = [];

  // Active sessions from PID files — these have reliable CWD
  const sessionsDir = path.join(CLAUDE_HOME, "sessions");
  if (fs.existsSync(sessionsDir)) {
    for (const file of fs.readdirSync(sessionsDir)) {
      if (!file.endsWith(".json")) continue;
      try {
        const data = JSON.parse(fs.readFileSync(path.join(sessionsDir, file), "utf8"));
        const alive = isProcessAlive(data.pid);
        // Skip stale PID files (process dead)
        if (!alive) continue;
        sessions.push({
          sessionId: data.sessionId,
          cwd: data.cwd,
          pid: data.pid,
          alive: true,
          startedAt: data.startedAt,
          mtime: data.startedAt ?? 0,
        });
      } catch { /* skip malformed */ }
    }
  }

  // Also check history.jsonl for recent sessions + last prompt per session
  const historyPath = path.join(CLAUDE_HOME, "history.jsonl");
  const promptBySession = new Map<string, string>(); // sessionId → last prompt
  if (fs.existsSync(historyPath)) {
    const seen = new Set(sessions.map(s => s.sessionId));
    try {
      const lines = fs.readFileSync(historyPath, "utf8").trim().split("\n");
      // Read last 50 lines to get enough prompt context
      const recent = lines.slice(-50);
      const bySession = new Map<string, { cwd: string; mtime: number }>();
      for (const line of recent) {
        try {
          const entry = JSON.parse(line);
          const cwd = entry.cwd || entry.project;
          if (entry.sessionId) {
            if (entry.display) {
              promptBySession.set(entry.sessionId, entry.display);
            }
            if (cwd) {
              bySession.set(entry.sessionId, {
                cwd,
                mtime: typeof entry.timestamp === "number" ? entry.timestamp : 0,
              });
            }
          }
        } catch { /* skip */ }
      }
      // Add top 5 unseen sessions
      const candidates = [...bySession.entries()]
        .filter(([sid]) => !seen.has(sid))
        .sort((a, b) => b[1].mtime - a[1].mtime)
        .slice(0, 5);
      for (const [sid, info] of candidates) {
        sessions.push({
          sessionId: sid,
          cwd: info.cwd,
          alive: false,
          mtime: info.mtime,
          lastPrompt: promptBySession.get(sid),
        });
      }
    } catch { /* skip */ }
  }

  // Attach lastPrompt to PID-discovered sessions too
  for (const s of sessions) {
    if (!s.lastPrompt && promptBySession.has(s.sessionId)) {
      s.lastPrompt = promptBySession.get(s.sessionId);
    }
  }

  // Sort: alive first, then by mtime descending
  sessions.sort((a, b) => {
    if (a.alive !== b.alive) return a.alive ? -1 : 1;
    return b.mtime - a.mtime;
  });

  return sessions;
}

// --- Thread History ---

const SYSTEM_PROMPT = [
  "You are a Discord bot running inside a thread.",
  "Multiple users may be talking in the same thread.",
  "When thread history is provided, use it as context to understand the conversation so far.",
  "Reply naturally as a participant in the group conversation.",
  "IMPORTANT: Do NOT output any session handoff summaries, session recaps, bullet-point preambles, or any meta-commentary about previous sessions at the start of your reply.",
  "Respond directly and immediately to the user's message.",
  "",
  "FORMATTING: Discord does NOT render markdown tables. Never use markdown table syntax (| col | col |).",
  "When comparing items, use bold label + slash-separated attributes on one line per item, e.g.:",
  "**Opus** — Speed: Slow / Quality: Best / Price: $$$",
  "**Sonnet** — Speed: Fast / Quality: Good / Price: $$",
  "**Haiku** — Speed: Fastest / Quality: OK / Price: $",
].join("\n");

const RESUME_SYSTEM_PROMPT = [
  "User is continuing this conversation from Discord (mobile).",
  "This is the same session, just a different interface — respond normally.",
  "IMPORTANT: Do NOT output any session handoff summaries, session recaps, bullet-point preambles, or any meta-commentary about previous sessions at the start of your reply.",
  "Respond directly and immediately to the user's message.",
  "",
  "FORMATTING: Discord does NOT render markdown tables. Never use markdown table syntax (| col | col |).",
  "When comparing items, use bold label + slash-separated attributes on one line per item, e.g.:",
  "**Opus** — Speed: Slow / Quality: Best / Price: $$$",
  "**Sonnet** — Speed: Fast / Quality: Good / Price: $$",
  "**Haiku** — Speed: Fastest / Quality: OK / Price: $",
].join("\n");

const HISTORY_FETCH_LIMIT = 30;

async function fetchThreadHistory(
  channel: Message["channel"],
  entry: ThreadEntry,
  botUserId: string,
  currentMessageId?: string,
): Promise<string> {
  const fetchOpts: { limit: number; after?: string } = { limit: HISTORY_FETCH_LIMIT };
  if (entry.started && entry.lastBotMessageId) {
    fetchOpts.after = entry.lastBotMessageId;
  }

  let messages: Collection<Snowflake, Message>;
  try {
    messages = await channel.messages.fetch(fetchOpts);
  } catch {
    return "";
  }

  if (messages.size === 0) return "";

  const sorted = [...messages.values()]
    .sort((a, b) => a.createdTimestamp - b.createdTimestamp)
    .filter((m) => m.author.id !== botUserId && m.id !== currentMessageId);

  if (sorted.length === 0) return "";

  const lines = sorted.map((m) => {
    const name = m.member?.displayName ?? m.author.displayName ?? m.author.username;
    const text = m.content.replace(/<@!?\d+>/g, "").trim();
    return text ? `[${name}] ${text}` : null;
  }).filter(Boolean);

  if (lines.length === 0) return "";

  const label = entry.started
    ? "Messages from other users since your last reply"
    : "Recent thread history for context";

  return `[${label}]\n${lines.join("\n")}\n[End]\n\n`;
}

// --- Streaming Runner ---

const running = new Map<string, ChildProcess>();

type StreamCallbacks = {
  onText?: (fullText: string) => void;
  onToolUse?: (toolName: string) => void;
};

type AskQuestion = {
  question: string;
  header: string;
  options: { label: string; description: string }[];
  multiSelect: boolean;
};

type PermissionDenial = {
  tool_name: string;
  tool_use_id: string;
  tool_input: { questions: AskQuestion[] };
};

type RunResult = { text: string; exitCode: number; costUsd?: number; is_error?: boolean; permissionDenials?: PermissionDenial[] };

function runClaudeStreaming(opts: {
  sessionId: string;
  prompt: string;
  cwd: string;
  model: string;
  claudeBin: string;
  resume: boolean;
  systemPrompt?: string;
  systemPromptMode?: SystemPromptMode;
  mcpConfig?: string;
  timeoutMs?: number;
  callbacks?: StreamCallbacks;
}): Promise<RunResult> {
  return new Promise((resolve, reject) => {
    const args = [
      "-p", opts.prompt,
      ...(opts.resume ? ["--resume", opts.sessionId] : ["--session-id", opts.sessionId]),
      "--model", opts.model,
      "--dangerously-skip-permissions",
      "--output-format", "stream-json",
      "--verbose",
      "--include-partial-messages",
      // `--system-prompt` REPLACES Claude Code's own system prompt; `--append-`
      // adds to it. Replacing drops the CLI's tool-use guidance along with
      // everything else, so append is the default and replacing is opt-in.
      ...(opts.systemPrompt
        ? [opts.systemPromptMode === "replace" ? "--system-prompt" : "--append-system-prompt", opts.systemPrompt]
        : []),
      ...(opts.mcpConfig ? ["--mcp-config", opts.mcpConfig] : []),
    ];

    const child = spawn(opts.claudeBin, args, {
      cwd: opts.cwd,
      env: childEnv(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    running.set(opts.sessionId, child);

    let buffer = "";
    let stderrBuf = "";
    let lastSeenText = "";
    let resultText = "";
    let costUsd: number | undefined;
    let isError = false;
    let permissionDenials: PermissionDenial[] | undefined;
    let settled = false;

    child.stderr.on("data", (chunk: Buffer) => {
      stderrBuf += chunk.toString();
    });

    child.stdout.on("data", (chunk: Buffer) => {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop()!;

      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const event = JSON.parse(line);

          if (event.type === "assistant" && event.message?.content) {
            let messageText = "";
            for (const block of event.message.content) {
              if (block.type === "text") {
                messageText += block.text || "";
              } else if (block.type === "tool_use" && block.name) {
                opts.callbacks?.onToolUse?.(block.name);
              }
            }
            if (messageText && messageText !== lastSeenText) {
              lastSeenText = messageText;
              opts.callbacks?.onText?.(messageText);
            }
          }

          if (event.type === "result") {
            resultText = event.result || "";
            costUsd = event.total_cost_usd;
            if (event.is_error) isError = true;
            // Capture error messages from the errors array (e.g. "No conversation found...")
            if (!resultText && event.errors?.length) {
              resultText = event.errors.join("\n");
            }
            if (event.permission_denials?.length > 0) {
              permissionDenials = event.permission_denials;
            }
          }
        } catch {
          // Not valid JSON, skip
        }
      }
    });

    const timeout = opts.timeoutMs ?? 5_400_000;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      running.delete(opts.sessionId);
      child.kill("SIGTERM");
      const partial = resultText || lastSeenText || "";
      if (partial) {
        resolve({
          text: partial + "\n\n⚠️ *Task timed out after 90 min — partial result above.*",
          exitCode: 124,
          costUsd,
        });
      } else {
        reject(new Error(`Timeout after ${timeout}ms with no output`));
      }
    }, timeout);

    child.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      running.delete(opts.sessionId);
      reject(err);
    });

    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      running.delete(opts.sessionId);
      const errHint = stderrBuf.trim() ? `\n\n⚠️ stderr: ${stderrBuf.trim().slice(0, 500)}` : "";
      const text = resultText || lastSeenText || `(no output)${errHint}`;
      resolve({ text, exitCode: code ?? 1, costUsd, is_error: isError, permissionDenials });
    });
  });
}

// Generate a short, intuitive thread title from a user message using Claude Haiku.
// Returns null on failure — caller should fall back to the placeholder name.
function generateThreadTitle(opts: {
  message: string;
  claudeBin: string;
  cwd: string;
  timeoutMs?: number;
}): Promise<string | null> {
  return new Promise((resolve) => {
    const prompt =
      "Generate a concise Discord thread title (3-6 words, max 60 chars) summarizing this user message. " +
      "Use Title Case. No quotes, no trailing punctuation, no emoji, no prefix like 'Title:'. " +
      "Output ONLY the title text.\n\nMessage:\n" + opts.message;

    const args = [
      "-p", prompt,
      "--model", "haiku",
      "--dangerously-skip-permissions",
      "--output-format", "text",
    ];

    const child = spawn(opts.claudeBin, args, {
      cwd: opts.cwd,
      env: childEnv(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    let out = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { child.kill("SIGTERM"); } catch { /* ignore */ }
      resolve(null);
    }, opts.timeoutMs ?? 20_000);

    child.stdout.on("data", (chunk: Buffer) => { out += chunk.toString(); });
    child.on("error", () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(null);
    });
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) return resolve(null);
      // Take first non-empty line, strip wrapping quotes/punctuation, truncate to 95 chars.
      const firstLine = out.split("\n").map(l => l.trim()).find(l => l.length > 0) ?? "";
      const cleaned = firstLine
        .replace(/^["'`]|["'`]$/g, "")
        .replace(/^(title|thread title)\s*[:\-]\s*/i, "")
        .replace(/[.!?]+$/, "")
        .trim();
      resolve(cleaned.length > 0 ? cleaned.slice(0, 95) : null);
    });
  });
}

// --- AskUserQuestion button rendering ---

async function sendAskButtons(
  channel: { send: (opts: any) => Promise<Message> },
  threadId: string,
  entry: ThreadEntry,
  denial: PermissionDenial,
): Promise<void> {
  for (const q of denial.tool_input.questions) {
    const row = new ActionRowBuilder<ButtonBuilder>();
    for (const opt of q.options.slice(0, 4)) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`ask_${entry.sessionId}_${opt.label}`.slice(0, 100))
          .setLabel(opt.label)
          .setStyle(ButtonStyle.Primary),
      );
    }
    if (q.options.length <= 3) {
      row.addComponents(
        new ButtonBuilder()
          .setCustomId(`ask_${entry.sessionId}_OTHER`)
          .setLabel("Other...")
          .setStyle(ButtonStyle.Secondary),
      );
    }
    const botReply = await channel.send({
      content: `❓ **${q.question}**`,
      components: [row],
    });
    entry.lastBotMessageId = botReply.id;
  }
  saveEntry(threadId, entry);
}

// --- Streaming tool-use callback ---

function createToolUseHandler(ps: PreviewState): (toolName: string) => void {
  return (toolName) => {
    console.log(`[discord-cc-bot] tool: ${toolName}`);
    ps.toolsUsed.push(toolName);
    if (ps.msg) {
      const text = (ps.pendingText || "").slice(0, PREVIEW_MAX_LEN);
      ps.msg.edit(text + buildStatusLine(ps)).catch(() => {});
    }
  };
}

// --- Attachment handling ---

const ATTACH_TMP_DIR = path.join(import.meta.dirname, "..", "tmp-attachments");
const ATTACH_MAX_BYTES = 10 * 1024 * 1024; // 10 MB per file

async function downloadAttachment(url: string, filepath: string): Promise<void> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.mkdirSync(path.dirname(filepath), { recursive: true });
  fs.writeFileSync(filepath, buf);
}

// --- Skill install (vault -> ~/.claude/skills/) ---
//
// Claude Code has a built-in "sensitive file" guard on ~/.claude/ paths that
// blocks Write/Edit tool calls from within a Claude Code session, even with
// --dangerously-skip-permissions. This bot process isn't subject to that
// guard, so we expose a direct slash command to copy a skill's source copy
// into the Claude Code skills dir where it gets auto-loaded.
//
// Set VAULT_SKILLS_DIR in your .env to point at the directory that holds your
// skill markdown files (e.g. an Obsidian vault's Skills/ folder). Defaults to
// ~/obsidian-vault/Skills. If the directory doesn't exist, /install-skill is
// effectively a no-op.

const VAULT_SKILLS_DIR =
  process.env.VAULT_SKILLS_DIR ??
  path.join(process.env.HOME ?? "", "obsidian-vault", "Skills");
const CLAUDE_SKILLS_DIR = path.join(process.env.HOME ?? "", ".claude", "skills");

// Frontmatter keys that are meaningful to Claude Code skill loading.
// Anything else (vault-specific metadata like `type`, `tags`, `related`,
// `skill_version`, `trigger_conditions`, `technologies`, `source`) is stripped.
const SKILL_FRONTMATTER_KEYS = new Set(["name", "description", "author", "version", "date"]);

function stripVaultFrontmatter(raw: string): string {
  if (!raw.startsWith("---\n")) return raw;
  const end = raw.indexOf("\n---\n", 4);
  if (end === -1) return raw;
  const fmRaw = raw.slice(4, end);
  const body = raw.slice(end + 5);

  const outLines: string[] = [];
  const lines = fmRaw.split("\n");
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$/);
    if (!m) {
      // continuation of previous key (block scalar content or list item) — include if we kept the parent
      if (outLines.length > 0 && (line.startsWith("  ") || line.startsWith("-"))) {
        outLines.push(line);
      }
      i++;
      continue;
    }
    const key = m[1];
    const rest = m[2];
    const keep = SKILL_FRONTMATTER_KEYS.has(key);
    if (keep) outLines.push(line);
    // Consume continuation lines (block scalars `|` / `>` or indented list items)
    if (rest === "|" || rest === ">" || rest === "") {
      i++;
      while (i < lines.length && (lines[i].startsWith("  ") || lines[i].startsWith("- ") || lines[i] === "")) {
        if (keep) outLines.push(lines[i]);
        i++;
      }
    } else {
      i++;
    }
  }
  return `---\n${outLines.join("\n").replace(/\n+$/, "")}\n---\n${body}`;
}

function listVaultSkills(): string[] {
  if (!fs.existsSync(VAULT_SKILLS_DIR)) return [];
  return fs.readdirSync(VAULT_SKILLS_DIR)
    .filter(f => f.endsWith(".md"))
    .map(f => f.replace(/\.md$/, ""))
    .sort();
}

function isSkillInstalled(name: string): boolean {
  return fs.existsSync(path.join(CLAUDE_SKILLS_DIR, name, "SKILL.md"));
}

function installSkillFromVault(name: string): { ok: true; path: string } | { ok: false; error: string } {
  // Validate name: kebab-case, no path traversal
  if (!/^[a-z0-9][a-z0-9-]*[a-z0-9]$/.test(name)) {
    return { ok: false, error: "Skill name must be kebab-case (lowercase, digits, hyphens)." };
  }
  const src = path.join(VAULT_SKILLS_DIR, `${name}.md`);
  if (!fs.existsSync(src)) {
    return { ok: false, error: `No vault skill at \`${src}\`.` };
  }
  const raw = fs.readFileSync(src, "utf8");
  const cleaned = stripVaultFrontmatter(raw);
  const destDir = path.join(CLAUDE_SKILLS_DIR, name);
  const dest = path.join(destDir, "SKILL.md");
  fs.mkdirSync(destDir, { recursive: true });
  fs.writeFileSync(dest, cleaned, "utf8");
  return { ok: true, path: dest };
}

// --- Chunked message sending ---

const DISCORD_MAX_LEN = 2000;
const CHUNK_LEN = 1900;

function splitMessage(text: string): string[] {
  if (text.length <= CHUNK_LEN) return [text];

  // Split text into atomic segments: complete code blocks + surrounding text.
  // Code blocks are never broken across chunks.
  const segments: string[] = [];
  const blockRegex = /^(`{3,})\w*\n[\s\S]*?^\1\s*$/gm;
  let lastEnd = 0;

  for (const match of text.matchAll(blockRegex)) {
    if (match.index! > lastEnd) {
      segments.push(text.slice(lastEnd, match.index!));
    }
    segments.push(match[0]);
    lastEnd = match.index! + match[0].length;
  }
  if (lastEnd < text.length) {
    segments.push(text.slice(lastEnd));
  }

  // Pack segments into chunks, splitting only at segment boundaries
  const chunks: string[] = [];
  let current = "";

  for (const seg of segments) {
    const combined = current + seg;
    if (combined.length <= CHUNK_LEN) {
      current = combined;
      continue;
    }

    // Won't fit — flush current chunk, start new one
    if (current) chunks.push(current);

    if (seg.length <= CHUNK_LEN) {
      current = seg;
    } else {
      // Oversized segment — detect if it's a code block
      const fenceMatch = seg.match(/^(`{3,})(\w*)\n/);
      if (fenceMatch) {
        // It's a code block — strip outer fences, split inner content, re-wrap each piece
        const fence = fenceMatch[1];
        const lang = fenceMatch[2];
        const header = fence + lang + "\n";
        const footer = "\n" + fence;
        const closeIdx = seg.lastIndexOf("\n" + fence);
        const body = seg.slice(header.length, closeIdx === -1 ? seg.length : closeIdx);
        const maxBody = CHUNK_LEN - header.length - footer.length;
        let rem = body;
        while (rem.length > maxBody) {
          let splitAt = rem.lastIndexOf("\n", maxBody);
          if (splitAt < maxBody / 2) splitAt = maxBody;
          chunks.push(header + rem.slice(0, splitAt) + footer);
          rem = rem.slice(splitAt + (rem[splitAt] === "\n" ? 1 : 0));
        }
        current = header + rem + footer;
      } else {
        // Plain text oversized — split at newlines
        let rem = seg;
        while (rem.length > CHUNK_LEN) {
          let splitAt = rem.lastIndexOf("\n", CHUNK_LEN);
          if (splitAt < CHUNK_LEN / 2) splitAt = CHUNK_LEN;
          chunks.push(rem.slice(0, splitAt));
          rem = rem.slice(splitAt + (rem[splitAt] === "\n" ? 1 : 0));
        }
        current = rem;
      }
    }
  }

  if (current) chunks.push(current);
  return chunks;
}

async function sendChunked(
  channel: { send: (content: string) => Promise<Message> },
  text: string,
  replyTo?: Message,
): Promise<Message> {
  const chunks = splitMessage(text);

  let firstMsg: Message | undefined;
  for (let i = 0; i < chunks.length; i++) {
    if (i === 0 && replyTo) {
      firstMsg = await replyTo.reply(chunks[i]);
    } else {
      const msg = await channel.send(chunks[i]);
      if (i === 0) firstMsg = msg;
    }
  }
  return firstMsg!;
}

// --- Streaming preview throttle ---

const STREAM_THROTTLE_MS = 1500;
const STREAM_MIN_DELTA = 40;
const PREVIEW_MAX_LEN = 1900;

type PreviewState = {
  msg: Message | null;
  lastText: string;
  lastEditTime: number;
  timer: NodeJS.Timeout | null;
  pendingText: string;
  startTime: number;
  toolsUsed: string[];
};

function createPreviewState(): PreviewState {
  return { msg: null, lastText: "", lastEditTime: 0, timer: null, pendingText: "", startTime: Date.now(), toolsUsed: [] };
}

function formatElapsed(startTime: number): string {
  const elapsed = Math.round((Date.now() - startTime) / 1000);
  const mins = Math.floor(elapsed / 60);
  const secs = elapsed % 60;
  return mins > 0 ? `${mins}m${secs}s` : `${secs}s`;
}

function buildStatusLine(ps: PreviewState): string {
  const time = formatElapsed(ps.startTime);
  const toolLine = ps.toolsUsed.length > 0
    ? `🔧 ${ps.toolsUsed.slice(-3).join(" → ")}\n` : "";
  return `\n\n${toolLine}⏳ *working... (${time})*`;
}

function flushPreview(ps: PreviewState): void {
  if (!ps.msg || !ps.pendingText) return;
  const display = ps.pendingText.slice(0, PREVIEW_MAX_LEN) + buildStatusLine(ps);
  ps.msg.edit(display).catch(() => {});
  ps.lastText = ps.pendingText;
  ps.lastEditTime = Date.now();
}

function handleStreamText(ps: PreviewState, fullText: string): void {
  ps.pendingText = fullText;

  const delta = fullText.length - ps.lastText.length;
  const elapsed = Date.now() - ps.lastEditTime;

  // Not enough new content
  if (delta < STREAM_MIN_DELTA && ps.lastEditTime > 0) {
    if (!ps.timer) {
      ps.timer = setTimeout(() => {
        ps.timer = null;
        flushPreview(ps);
      }, STREAM_THROTTLE_MS);
    }
    return;
  }

  // Too soon since last edit
  if (elapsed < STREAM_THROTTLE_MS && ps.lastEditTime > 0) {
    if (!ps.timer) {
      ps.timer = setTimeout(() => {
        ps.timer = null;
        flushPreview(ps);
      }, STREAM_THROTTLE_MS - elapsed);
    }
    return;
  }

  // Enough content & time — flush immediately
  if (ps.timer) clearTimeout(ps.timer);
  flushPreview(ps);
}

// --- Channel Config (agent routing) ---

type ChannelSchedule = {
  cron: string;
  prompt: string;
  timezone?: string;
  useThread?: boolean;
};

type SystemPromptMode = "append" | "replace";

type ChannelConfig = {
  name: string;
  sessionId: string;
  systemPrompt?: string;
  systemPromptMode?: SystemPromptMode;
  workingDirectory?: string;
  model?: string;
  schedule?: ChannelSchedule;
  replyInThread?: boolean;
  contextFile?: string;
  /** Answer only when mentioned. Default false — a configured channel replies to everything. */
  requireMention?: boolean;
  /** Extra case-insensitive regexes counted as a mention, so a bare name in plain
   *  text routes as well as a real Discord ping does. */
  mentionPatterns?: string[];
  /** Admit messages from other bots. Default false. */
  allowBots?: boolean;
  /** Consecutive bot-triggered turns allowed before this channel goes quiet
   *  until a human speaks. Default DEFAULT_BOT_TURN_BUDGET. */
  botTurnBudget?: number;
  /** Prepend recent channel messages to the prompt. Default true. */
  fetchHistory?: boolean;
};

type ChannelConfigFile = {
  channels: Record<string, ChannelConfig>;
  defaults?: {
    model?: string;
    systemPrompt?: string;
    systemPromptMode?: SystemPromptMode;
    workingDirectory?: string;
  };
};

const CHANNEL_CONFIG_PATH = path.join(import.meta.dirname, "..", "channel-config.json");

function loadChannelConfig(): ChannelConfigFile {
  try {
    if (fs.existsSync(CHANNEL_CONFIG_PATH)) {
      return JSON.parse(fs.readFileSync(CHANNEL_CONFIG_PATH, "utf8"));
    }
  } catch (err) {
    console.error("[discord-cc-bot] failed to load channel-config.json:", err);
  }
  return { channels: {} };
}

let channelConfig = loadChannelConfig();

// --- Context File Cache ---

const contextFileCache = new Map<string, string>();

function loadContextFile(filePath: string): string {
  const absPath = path.resolve(path.join(import.meta.dirname, "..", filePath));
  try {
    const content = fs.readFileSync(absPath, "utf8").trim();
    contextFileCache.set(filePath, content);
    console.log(`[discord-cc-bot] loaded context file: ${filePath}`);
    return content;
  } catch (err) {
    console.error(`[discord-cc-bot] failed to load context file "${filePath}":`, (err as Error).message);
    return "";
  }
}

function getContextForChannel(config: ChannelConfig): string {
  if (!config.contextFile) return "";
  if (contextFileCache.has(config.contextFile)) {
    return contextFileCache.get(config.contextFile)!;
  }
  return loadContextFile(config.contextFile);
}

function preloadContextFiles(): void {
  contextFileCache.clear();
  for (const cfg of Object.values(channelConfig.channels)) {
    if (cfg.contextFile) {
      loadContextFile(cfg.contextFile);
    }
  }
}

preloadContextFiles();

// Watch for config changes and reload
fs.watchFile(CHANNEL_CONFIG_PATH, { interval: 5000 }, () => {
  console.log("[discord-cc-bot] reloading channel-config.json");
  channelConfig = loadChannelConfig();
  preloadContextFiles();
});

function getChannelAgent(channelId: string): ChannelConfig | null {
  return channelConfig.channels[channelId] ?? null;
}

function resolveSystemPromptMode(agent: ChannelConfig | null): SystemPromptMode {
  return agent?.systemPromptMode ?? channelConfig.defaults?.systemPromptMode ?? "append";
}

// A bare name in message text should route as well as a real Discord ping does,
// because a bot posting plain "@name" does not produce a ping — only the
// `<@id>` form does, and text written by a model is posted verbatim.
function matchesMentionPatterns(content: string, agent: ChannelConfig | null): boolean {
  const patterns = agent?.mentionPatterns;
  if (!patterns?.length) return false;
  return patterns.some((pattern) => {
    try {
      return new RegExp(pattern, "i").test(content);
    } catch (err) {
      console.error(`[discord-cc-bot] bad mentionPattern ${JSON.stringify(pattern)}:`, (err as Error).message);
      return false;
    }
  });
}

// --- Bot-turn budget ---

// Admitting other bots is what lets two of them hold a conversation, and
// therefore also what lets them answer each other until something kills the
// process. Instruction alone has been observed to fail at this — the runaway
// case is two agents being polite, not two agents misbehaving — so the ceiling
// is mechanical: N consecutive bot-triggered turns per channel, cleared by any
// message from a human.
const DEFAULT_BOT_TURN_BUDGET = 6;
const botTurnsUsed = new Map<string, number>();

function consumeBotTurnBudget(channelId: string, fromBot: boolean, agent: ChannelConfig | null): boolean {
  if (!fromBot) {
    botTurnsUsed.delete(channelId);
    return true;
  }
  const budget = agent?.botTurnBudget ?? DEFAULT_BOT_TURN_BUDGET;
  const used = (botTurnsUsed.get(channelId) ?? 0) + 1;
  botTurnsUsed.set(channelId, used);
  if (used > budget) {
    console.log(`[discord-cc-bot] bot-turn budget (${budget}) spent in ${channelId} — quiet until a human speaks`);
    return false;
  }
  return true;
}

// --- Turn serialization ---

// A message arriving mid-turn queues instead of being rejected: with several
// speakers in one channel, mid-turn arrival is the normal case rather than the
// exception.
const MAX_QUEUE_DEPTH = 4;

type Lane = { busy: boolean; waiting: (() => void)[] };
const lanes = new Map<string, Lane>();

/** Returns a release function, or null if this key's queue is already full. */
function acquireTurn(key: string): Promise<(() => void) | null> {
  let lane = lanes.get(key);
  if (!lane) {
    lane = { busy: false, waiting: [] };
    lanes.set(key, lane);
  }
  const held = lane;

  if (held.busy && held.waiting.length >= MAX_QUEUE_DEPTH) {
    return Promise.resolve(null);
  }

  const makeRelease = (): (() => void) => {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = held.waiting.shift();
      if (next) {
        next();
      } else {
        held.busy = false;
        if (lanes.get(key) === held) lanes.delete(key);
      }
    };
  };

  if (!held.busy) {
    held.busy = true;
    return Promise.resolve(makeRelease());
  }
  return new Promise((resolve) => {
    held.waiting.push(() => resolve(makeRelease()));
  });
}

// --- Silent turns ---

// An explicit way for the model to decline to post. In a channel where bots hear
// each other, not posting is what ends an exchange, so the graceful exit needs to
// be something the model can choose rather than something it has to be stopped
// from doing.
const SILENT_TOKEN = process.env.SILENT_TOKEN ?? "NO_RESPONSE";

// --- Discord ---

const DISCORD_TOKEN = process.env.DISCORD_TOKEN!;
const DEFAULT_CWD = process.env.DEFAULT_CWD ?? process.cwd();
const CLAUDE_BIN = process.env.CLAUDE_BIN ?? "claude";
const GUILD_ID = process.env.GUILD_ID;

const slashCommands = [
  new SlashCommandBuilder().setName("help").setDescription("Show available commands"),
  new SlashCommandBuilder().setName("new").setDescription("Clear context — start a new conversation"),
  new SlashCommandBuilder().setName("model").setDescription("Switch Claude model")
    .addStringOption(o => o.setName("name").setDescription("Model name (e.g. sonnet, opus, haiku)").setRequired(true)),
  new SlashCommandBuilder().setName("cd").setDescription("Switch working directory")
    .addStringOption(o => o.setName("path").setDescription("Absolute path to directory").setRequired(true)),
  new SlashCommandBuilder().setName("stop").setDescription("Kill running Claude process"),
  new SlashCommandBuilder().setName("sessions").setDescription("List all active sessions"),
  new SlashCommandBuilder().setName("channels").setDescription("List configured channel agents"),
  new SlashCommandBuilder().setName("reload-config").setDescription("Reload channel-config.json"),
  new SlashCommandBuilder().setName("resume-local").setDescription("Resume a local terminal Claude Code session")
    .addStringOption(o => o.setName("session").setDescription("Session ID (auto-detect if omitted)").setRequired(false)),
  new SlashCommandBuilder().setName("handback").setDescription("Hand session back to terminal"),
  new SlashCommandBuilder().setName("install-skill").setDescription("Copy a skill from the Obsidian vault into ~/.claude/skills/")
    .addStringOption(o => o.setName("name").setDescription("Skill name or number from the list (omit to list)").setRequired(false)),
];

if (!DISCORD_TOKEN) {
  console.error("Missing DISCORD_TOKEN");
  process.exit(1);
}

const threadMap = loadMap();

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

client.once(Events.ClientReady, async (c) => {
  console.log(`[discord-cc-bot] ready as ${c.user.tag}`);
  console.log(
    `[discord-cc-bot] auth: ${hasStoredCredentials()
      ? `stored credentials (${CREDENTIALS_PATH}); CLAUDE_CODE_OAUTH_TOKEN blanked for children`
      : process.env.CLAUDE_CODE_OAUTH_TOKEN
        ? "CLAUDE_CODE_OAUTH_TOKEN from the environment"
        : "none found — the CLI will resolve its own"}`,
  );
  const rest = new REST().setToken(DISCORD_TOKEN);
  try {
    const route = GUILD_ID
      ? Routes.applicationGuildCommands(c.user.id, GUILD_ID)
      : Routes.applicationCommands(c.user.id);
    await rest.put(route, {
      body: slashCommands.map(cmd => cmd.toJSON()),
    });
    console.log(`[discord-cc-bot] registered ${slashCommands.length} slash commands`);
  } catch (err) {
    console.error("[discord-cc-bot] failed to register commands:", err);
  }

  // --- Scheduled Jobs ---
  startScheduledJobs(c);
});

const scheduledTasks: cron.ScheduledTask[] = [];

function startScheduledJobs(c: Client<true>): void {
  // Clear any existing tasks (in case of config reload)
  for (const task of scheduledTasks) task.stop();
  scheduledTasks.length = 0;

  for (const [channelId, cfg] of Object.entries(channelConfig.channels)) {
    if (!cfg.schedule) continue;

    const { cron: cronExpr, prompt, timezone } = cfg.schedule;
    console.log(`[discord-cc-bot] scheduling "${cfg.name}" → "${cronExpr}"${timezone ? ` (${timezone})` : ""}`);

    const task = cron.schedule(cronExpr, async () => {
      console.log(`[discord-cc-bot] cron fired for #${cfg.name}`);
      let release: (() => void) | null = null;
      try {
        const channel = await c.channels.fetch(channelId);
        if (!channel || !("send" in channel)) {
          console.error(`[discord-cc-bot] cron: cannot send to channel ${channelId}`);
          return;
        }

        const agentCwd = cfg.workingDirectory ?? channelConfig.defaults?.workingDirectory ?? DEFAULT_CWD;
        const agentModel = cfg.model ?? channelConfig.defaults?.model ?? "opus";
        const baseSchedulePrompt = cfg.systemPrompt ?? channelConfig.defaults?.systemPrompt ?? "";
        const scheduleContext = getContextForChannel(cfg);
        const agentSystemPrompt = scheduleContext
          ? `Channel context:\n${scheduleContext}\n\n${baseSchedulePrompt}`
          : baseSchedulePrompt;

        // When useThread is enabled, create a dated thread and post inside it
        const useThread = cfg.schedule?.useThread ?? false;
        let target: { send: (typeof channel)["send"] } = channel;
        let threadId: string | undefined;

        if (useThread && "threads" in channel) {
          const dateStr = new Date().toLocaleDateString("en-US", {
            weekday: "long",
            year: "numeric",
            month: "long",
            day: "numeric",
            timeZone: cfg.schedule?.timezone ?? "America/Chicago",
          });
          const newThread = await (channel as TextChannel).threads.create({
            name: `📧 ${dateStr}`,
            autoArchiveDuration: 1440, // 24 hours
            type: ChannelType.PublicThread,
          });
          target = newThread;
          threadId = newThread.id;
          console.log(`[discord-cc-bot] cron: created thread "${dateStr}" for #${cfg.name}`);
        }

        const targetId = threadId ?? channelId;
        const entry = getOrCreate(threadMap, targetId, agentCwd);
        entry.cwd = agentCwd;
        entry.model = agentModel;

        release = await acquireTurn(entry.sessionId);
        if (!release) {
          console.log(`[discord-cc-bot] cron: #${cfg.name} session busy, skipping this firing`);
          return;
        }

        const statusMsg = await target.send("⏳ *Running scheduled task...*");

        const previewState = createPreviewState();
        previewState.msg = statusMsg;

        const result = await runClaudeStreaming({
          sessionId: entry.sessionId,
          prompt,
          cwd: entry.cwd,
          model: entry.model,
          claudeBin: CLAUDE_BIN,
          resume: entry.started,
          systemPrompt: agentSystemPrompt,
          systemPromptMode: resolveSystemPromptMode(cfg),
          callbacks: {
            onText: (fullText) => handleStreamText(previewState, fullText),
            onToolUse: createToolUseHandler(previewState),
          },
        });

        if (previewState.timer) clearTimeout(previewState.timer);

        entry.started = true;
        await statusMsg.delete().catch(() => {});

        let botReply: Message;
        if (result.text.length <= DISCORD_MAX_LEN) {
          botReply = await target.send(result.text);
        } else {
          botReply = await sendChunked(target, result.text);
        }

        entry.lastBotMessageId = botReply.id;
        patchSessionEntrypoint(entry.sessionId, entry.cwd);
        saveEntry(targetId, entry);

        console.log(`[discord-cc-bot] cron: #${cfg.name} completed`);
      } catch (err) {
        console.error(`[discord-cc-bot] cron error for #${cfg.name}:`, (err as Error).message);
      } finally {
        release?.();
      }
    }, { timezone: timezone ?? undefined });

    scheduledTasks.push(task);
  }

  if (scheduledTasks.length > 0) {
    console.log(`[discord-cc-bot] started ${scheduledTasks.length} scheduled job(s)`);
  }
}

client.on(Events.InteractionCreate, async (interaction) => {
  // --- Button handler ---
  if (interaction.isButton()) {
    const id = interaction.customId;
    console.log(`[discord-cc-bot] button: ${id} by ${interaction.user.username}`);

    // AskUserQuestion buttons: ask_<sessionId>_<answer>
    if (id.startsWith("ask_")) {
      const parts = id.split("_");
      const sessionId = parts[1];
      const answer = parts.slice(2).join("_");
      const threadId = interaction.channelId;
      const entry = threadMap[threadId];

      if (answer === "OTHER") {
        await interaction.reply({ content: "Type your answer as a regular message:", ephemeral: true });
        return;
      }

      await interaction.update({ content: `✅ **${answer}**`, components: [] });

      // Resume claude with the answer
      if (entry && !running.has(entry.sessionId)) {
        const ch = interaction.channel!;
        if (!("send" in ch)) return;
        const previewState = createPreviewState();
        previewState.msg = await ch.send("⏳ *Continuing...*");

        try {
          const result = await runClaudeStreaming({
            sessionId: entry.sessionId,
            prompt: `I choose: ${answer}`,
            cwd: entry.cwd,
            model: entry.model,
            claudeBin: CLAUDE_BIN,
            resume: true,
            systemPrompt: SYSTEM_PROMPT,
            callbacks: {
              onText: (fullText) => handleStreamText(previewState, fullText),
              onToolUse: createToolUseHandler(previewState),
            },
          });

          if (previewState.timer) clearTimeout(previewState.timer);

          // Check for AskUserQuestion denials — render as Discord buttons
          const askDenial = result.permissionDenials?.find(d => d.tool_name === "AskUserQuestion");
          if (askDenial) {
            await previewState.msg!.delete().catch(() => {});
            await sendAskButtons(ch, threadId, entry, askDenial);
            return;
          }

          await previewState.msg!.delete().catch(() => {});
          let botReply: Message;
          if (result.text.length <= DISCORD_MAX_LEN) {
            botReply = await ch.send(result.text);
          } else {
            botReply = await sendChunked(ch, result.text);
          }
          entry.lastBotMessageId = botReply.id;
          patchSessionEntrypoint(entry.sessionId, entry.cwd);
          saveEntry(threadId, entry);
        } catch (err) {
          if (previewState.timer) clearTimeout(previewState.timer);
          if (previewState.msg) await previewState.msg.edit(`Error: ${(err as Error).message}`).catch(() => {});
        }
      }
      return;
    }

    // Test buttons (temporary)
    if (id.startsWith("test_")) {
      const choice = id.replace("test_", "");
      console.log(`[discord-cc-bot] test button: ${choice}`);
      await interaction.update({
        content: `✅ **你選了：${choice}**\n\nBot 收到了你的選擇！按鈕互動成功。`,
        components: [],
      });
      return;
    }

    return;
  }

  // --- Select menu handler (resume-local session picker) ---
  if (interaction.isStringSelectMenu() && interaction.customId === "resume_local_select") {
    const sessionId = interaction.values[0];
    const threadId = interaction.channelId;
    const locals = discoverLocalSessions();
    const picked = locals.find(s => s.sessionId === sessionId);
    const cwd = picked?.cwd ?? DEFAULT_CWD;
    const alive = picked?.alive ?? false;

    const entry = getOrCreate(threadMap, threadId, cwd);
    entry.sessionId = sessionId;
    entry.cwd = cwd;
    entry.isLocalResume = true;
    entry.started = true;
    saveEntry(threadId, entry);

    const status = alive ? "🟢 terminal still running" : "🔵 inactive";
    await interaction.update({
      content:
        `📱 已接手本地 session \`${sessionId.slice(0, 8)}…\` (${status})\n` +
        `cwd: \`${cwd}\`\n\n` +
        (alive ? `> ⚠️ Terminal CC 還在跑。建議先在 terminal 輸入 \`/quit\`。\n` : "") +
        `> 💻 回到 terminal 後：\`/quit\` → \`claude --continue\``,
      components: [],
    });
    return;
  }

  if (!interaction.isChatInputCommand()) return;

  const { commandName } = interaction;

  try {
    if (commandName === "help") {
      await interaction.reply({
        content: [
          "`/new` — clear context, start new conversation",
          "`/model <name>` — switch model (e.g. sonnet, opus, haiku)",
          "`/cd <path>` — switch working directory",
          "`/stop` — kill running task",
          "`/sessions` — list all sessions",
          "`/channels` — list configured channel agents",
          "`/reload-config` — reload channel-config.json",
          "`/resume-local [session]` — resume a local terminal CC session",
          "`/handback` — hand session back to terminal",
          "`/install-skill [name]` — install a vault skill into `~/.claude/skills/` (no arg = list)",
        ].join("\n"),
        ephemeral: true,
      });
      return;
    }

    if (commandName === "new") {
      const channelId = interaction.channelId;
      const agent = getChannelAgent(channelId);
      if (!interaction.channel?.isThread() && !agent) {
        await interaction.reply({ content: "This command only works in threads or configured channels.", ephemeral: true });
        return;
      }
      const threadId = channelId;
      const agentCwd = agent?.workingDirectory ?? channelConfig.defaults?.workingDirectory ?? DEFAULT_CWD;
      const entry = getOrCreate(threadMap, threadId, agentCwd);
      // For configured channels, reset to a new session but keep the agent prefix
      entry.sessionId = agent ? `${agent.sessionId}-${Date.now()}` : crypto.randomUUID();
      entry.started = false;
      saveEntry(threadId, entry);
      const label = agent ? `**${agent.name}** agent` : "conversation";
      await interaction.reply({ content: `Context cleared. Next message starts a new ${label}.`, ephemeral: true });
      return;
    }

    if (commandName === "model") {
      const name = interaction.options.getString("name", true);
      if (!interaction.channel?.isThread()) {
        await interaction.reply({ content: "This command only works in threads.", ephemeral: true });
        return;
      }
      const threadId = interaction.channelId;
      const entry = getOrCreate(threadMap, threadId, DEFAULT_CWD);
      entry.model = name;
      saveEntry(threadId, entry);
      await interaction.reply({ content: `Model -> \`${name}\``, ephemeral: true });
      return;
    }

    if (commandName === "cd") {
      const dir = interaction.options.getString("path", true);
      if (!fs.existsSync(dir)) {
        await interaction.reply({ content: `Path not found: \`${dir}\``, ephemeral: true });
        return;
      }
      if (!interaction.channel?.isThread()) {
        await interaction.reply({ content: "This command only works in threads.", ephemeral: true });
        return;
      }
      const threadId = interaction.channelId;
      const entry = getOrCreate(threadMap, threadId, DEFAULT_CWD);
      entry.cwd = dir;
      saveEntry(threadId, entry);
      await interaction.reply({ content: `cwd -> \`${dir}\``, ephemeral: true });
      return;
    }

    if (commandName === "stop") {
      const channelId = interaction.channelId;
      const agent = getChannelAgent(channelId);
      if (!interaction.channel?.isThread() && !agent) {
        await interaction.reply({ content: "This command only works in threads or configured channels.", ephemeral: true });
        return;
      }
      const threadId = channelId;
      const entry = threadMap[threadId];
      if (entry && running.has(entry.sessionId)) {
        running.get(entry.sessionId)!.kill("SIGTERM");
        await interaction.reply({ content: "Stopped.", ephemeral: true });
      } else {
        await interaction.reply({ content: "Nothing running.", ephemeral: true });
      }
      return;
    }

    if (commandName === "channels") {
      const entries = Object.entries(channelConfig.channels);
      if (entries.length === 0) {
        await interaction.reply({ content: "No channels configured. Edit `channel-config.json`.", ephemeral: true });
        return;
      }
      const lines = entries.map(([id, cfg]) =>
        `<#${id}> — **${cfg.name}** | \`${cfg.workingDirectory ?? DEFAULT_CWD}\` | ${cfg.model ?? "opus"}`
      );
      await interaction.reply({ content: lines.join("\n"), ephemeral: true });
      return;
    }

    if (commandName === "reload-config") {
      channelConfig = loadChannelConfig();
      preloadContextFiles();
      const count = Object.keys(channelConfig.channels).length;
      await interaction.reply({ content: `Reloaded config: ${count} channel(s) configured.`, ephemeral: true });
      return;
    }

    if (commandName === "sessions") {
      const lines = Object.entries(threadMap).map(
        ([tid, e]) => `<#${tid}> | ${e.model} | \`${e.cwd}\`${e.isLocalResume ? " 📱" : ""}`,
      );
      await interaction.reply({
        content: lines.length ? lines.join("\n") : "No sessions.",
        ephemeral: true,
      });
      return;
    }

    if (commandName === "resume-local") {
      if (!interaction.channel?.isThread()) {
        await interaction.reply({ content: "This command only works in threads.", ephemeral: true });
        return;
      }
      const threadId = interaction.channelId;
      const explicitId = interaction.options.getString("session");

      if (explicitId) {
        // Direct resume with explicit session ID
        const entry = getOrCreate(threadMap, threadId, DEFAULT_CWD);
        entry.sessionId = explicitId;
        entry.isLocalResume = true;
        entry.started = true; // always resume mode for local sessions
        saveEntry(threadId, entry);
        await interaction.reply(
          `📱 已接手本地 session \`${explicitId.slice(0, 8)}…\`\ncwd: \`${entry.cwd}\`\n\n` +
          `> 💻 回到 terminal 後：\`/quit\` → \`claude --continue\``,
        );
        return;
      }

      // Auto-discover local sessions
      const locals = discoverLocalSessions();
      if (locals.length === 0) {
        await interaction.reply({ content: "No local sessions found.", ephemeral: true });
        return;
      }

      // Filter: only sessions that are NOT alive (can't resume a running session)
      const resumable = locals.filter(s => !s.alive);
      const aliveCount = locals.length - resumable.length;

      if (resumable.length === 0) {
        const hint = aliveCount > 0
          ? `Found ${aliveCount} active session(s), but can't resume a running CC.\n` +
            `請先在 terminal 輸入 \`/quit\` 退出，再回來 \`/resume-local\`。`
          : "No local sessions found.";
        await interaction.reply({ content: hint, ephemeral: true });
        return;
      }

      if (resumable.length === 1) {
        const s = resumable[0];
        const entry = getOrCreate(threadMap, threadId, s.cwd);
        entry.sessionId = s.sessionId;
        entry.cwd = s.cwd;
        entry.isLocalResume = true;
        entry.started = true;
        saveEntry(threadId, entry);
        const promptHint = s.lastPrompt ? `\nLast prompt: *${s.lastPrompt.slice(0, 100)}*\n` : "\n";
        await interaction.reply(
          `📱 已接手本地 session \`${s.sessionId.slice(0, 8)}…\`` +
          promptHint +
          `cwd: \`${s.cwd}\`\n\n` +
          (aliveCount > 0 ? `> ⚠️ 另有 ${aliveCount} 個活躍 session 無法 resume（需先在 terminal \`/quit\`）\n` : "") +
          `> 💻 回到 terminal 後：\`claude --continue\``,
        );
        return;
      }

      // Multiple resumable sessions — show select menu
      const menu = new StringSelectMenuBuilder()
        .setCustomId("resume_local_select")
        .setPlaceholder("Pick a session to resume")
        .addOptions(
          resumable.slice(0, 10).map((s) => {
            const ago = Math.round((Date.now() - s.mtime) / 60000);
            const timeStr = ago < 60 ? `${ago}m` : `${Math.round(ago / 60)}h`;
            const prompt = s.lastPrompt ?? "(no prompt)";
            // Label: last prompt (max 100 chars, Discord limit)
            const label = `${prompt.slice(0, 95)}`;
            // Description: time ago + cwd
            const project = s.cwd.split("/").slice(-2).join("/");
            const desc = `${timeStr} ago · ${project}`;
            return new StringSelectMenuOptionBuilder()
              .setLabel(label.slice(0, 100) || "(empty)")
              .setDescription(desc.slice(0, 100))
              .setValue(s.sessionId);
          }),
        );

      const row = new ActionRowBuilder<StringSelectMenuBuilder>().addComponents(menu);
      await interaction.reply({ content: "Select a local session to resume:", components: [row], ephemeral: true });
      return;
    }

    if (commandName === "install-skill") {
      const input = interaction.options.getString("name", false);
      const skills = listVaultSkills();
      if (skills.length === 0) {
        await interaction.reply({ content: `No skills found in \`${VAULT_SKILLS_DIR}\`.`, ephemeral: true });
        return;
      }
      if (!input) {
        // List mode: numbered list with install status
        const lines = skills.map((s, i) => `**${i + 1}.** ${isSkillInstalled(s) ? "✅" : "⬜"} \`${s}\``);
        await interaction.reply({
          content: `**Vault skills** (✅ = installed in \`~/.claude/skills/\`)\n${lines.join("\n")}\n\nRun \`/install-skill <number>\` or \`/install-skill <name>\` to install one.`,
          ephemeral: true,
        });
        return;
      }
      // Resolve input: number (1-indexed) or exact name
      let name = input.trim();
      if (/^\d+$/.test(name)) {
        const idx = parseInt(name, 10) - 1;
        if (idx < 0 || idx >= skills.length) {
          await interaction.reply({ content: `❌ Number out of range. There are ${skills.length} vault skills (1–${skills.length}).`, ephemeral: true });
          return;
        }
        name = skills[idx];
      }
      const result = installSkillFromVault(name);
      if (!result.ok) {
        await interaction.reply({ content: `❌ ${result.error}`, ephemeral: true });
        return;
      }
      await interaction.reply({
        content: `✅ Installed \`${name}\` → \`${result.path}\`\nNew Claude Code sessions will auto-load it.`,
        ephemeral: true,
      });
      return;
    }

    if (commandName === "handback") {
      if (!interaction.channel?.isThread()) {
        await interaction.reply({ content: "This command only works in threads.", ephemeral: true });
        return;
      }
      const threadId = interaction.channelId;
      const entry = threadMap[threadId];
      if (!entry?.isLocalResume) {
        await interaction.reply({ content: "This thread is not a resumed local session.", ephemeral: true });
        return;
      }
      // Kill any running process
      if (running.has(entry.sessionId)) {
        running.get(entry.sessionId)!.kill("SIGTERM");
      }
      // Reset to a fresh bot session
      entry.isLocalResume = false;
      entry.sessionId = crypto.randomUUID();
      entry.started = false;
      saveEntry(threadId, entry);
      await interaction.reply(
        `💻 已交還 session。回到 terminal 輸入 \`claude --continue\` 即可看到完整對話。\n` +
        `此 thread 已重置，下次訊息會開始新的 bot session。`,
      );
      return;
    }
  } catch (err) {
    console.error("[discord-cc-bot] interaction error:", (err as Error).message);
    if (!interaction.replied) {
      await interaction.reply({ content: "An error occurred.", ephemeral: true }).catch(() => {});
    }
  }
});

client.on(Events.MessageCreate, async (message) => {
  let releaseTurn: (() => void) | null = null;
  try {
    // Never react to our own output, whatever the bot policy is.
    if (message.author.id === client.user!.id) return;

    // Check if this is a configured channel (agent routing)
    // Also check parent channel for threads inside configured channels
    const agent = getChannelAgent(message.channelId)
      ?? (message.channel.isThread() ? getChannelAgent(message.channel.parentId!) : null);

    const fromBot = message.author.bot;
    if (fromBot && !(agent?.allowBots ?? false)) return;

    const isMentioned = message.mentions.has(client.user!.id)
      || matchesMentionPatterns(message.content, agent);

    if (agent) {
      // Configured channel (or thread inside one) — everything, unless gated
      if ((agent.requireMention ?? false) && !isMentioned) return;
    } else if (isMentioned) {
      // @mentioned anywhere — respond with defaults
    } else {
      return;
    }

    if (!consumeBotTurnBudget(message.channelId, fromBot, agent)) return;

    const content = message.content.replace(/<@!?\d+>/g, "").trim();
    const attachments = [...message.attachments.values()];
    if (!content && attachments.length === 0) return;

    // Thread-first: create a thread from the user's message in configured channels
    const shouldThread = agent?.replyInThread === true && !message.channel.isThread();
    let thread: AnyThreadChannel | null = null;
    if (shouldThread) {
      const threadName = content.length > 97
        ? content.slice(0, 97) + "..."
        : content || "Thread";
      thread = await message.startThread({
        name: threadName,
        autoArchiveDuration: 1440,
      });
      // Fire-and-forget: generate a prettier title with Haiku and rename the thread.
      // Runs in parallel with the main Claude response so it doesn't add latency.
      const createdThread = thread;
      (async () => {
        try {
          const title = await generateThreadTitle({
            message: content,
            claudeBin: CLAUDE_BIN,
            cwd: agent?.workingDirectory ?? channelConfig.defaults?.workingDirectory ?? DEFAULT_CWD,
          });
          if (title && title !== createdThread.name) {
            await createdThread.setName(title);
          }
        } catch (err) {
          console.error("[discord-cc-bot] thread rename failed:", err);
        }
      })();
    }

    const threadId = thread?.id ?? message.channelId;

    // For configured channels, use the channel's config for cwd/model/sessionId
    const agentCwd = agent?.workingDirectory ?? channelConfig.defaults?.workingDirectory ?? DEFAULT_CWD;
    const agentModel = agent?.model ?? channelConfig.defaults?.model ?? "opus";
    const baseSystemPrompt = agent?.systemPrompt ?? channelConfig.defaults?.systemPrompt ?? SYSTEM_PROMPT;
    const channelContext = agent ? getContextForChannel(agent) : "";
    const agentSystemPrompt = channelContext
      ? `Channel context:\n${channelContext}\n\n${baseSystemPrompt}`
      : baseSystemPrompt;

    let entry: ThreadEntry;
    if (agent) {
      entry = getOrCreate(threadMap, threadId, agentCwd);
      entry.cwd = agentCwd;
      entry.model = agentModel;
      // Thread-first threads get their own session (new UUID from getOrCreate).
      // Only the parent channel itself gets the stable config sessionId.
      if (!thread) {
        const existingRow = stmtGet.get(threadId) as any;
        if (!existingRow) {
          entry.sessionId = agent.sessionId;
          saveEntry(threadId, entry);
        }
      }
    } else {
      entry = getOrCreate(threadMap, threadId, DEFAULT_CWD);
    }

    releaseTurn = await acquireTurn(entry.sessionId);
    if (!releaseTurn) {
      console.log(`[discord-cc-bot] queue full for ${entry.sessionId} — dropping message ${message.id}`);
      return;
    }

    // Send initial preview message (in thread if thread-first)
    const previewState = createPreviewState();
    previewState.msg = thread
      ? await thread.send("⏳ *Thinking...*")
      : await message.reply("⏳ *Thinking...*");

    // Download all attachments — let Claude Code handle them via Read tool
    const filePaths: string[] = [];
    for (const att of attachments) {
      if (att.size > ATTACH_MAX_BYTES) {
        console.log(`[discord-cc-bot] skipping oversized attachment: ${att.name} (${att.size} bytes)`);
        continue;
      }
      const ext = att.name?.split(".").pop() ?? "bin";
      const filepath = path.join(ATTACH_TMP_DIR, `${message.id}_${att.id}.${ext}`);
      try {
        await downloadAttachment(att.url, filepath);
        filePaths.push(filepath);
      } catch (err) {
        console.error(`[discord-cc-bot] attachment download failed: ${(err as Error).message}`);
      }
    }

    try {
      const historyChannel = thread ?? message.channel;
      // fetchThreadHistory filters only this bot's own messages, so in a channel
      // shared with sibling bots it pulls their traffic in whether or not this
      // agent was addressed. Off is the right default for such a channel.
      const history = (agent?.fetchHistory ?? true)
        ? await fetchThreadHistory(historyChannel, entry, client.user!.id, message.id)
        : "";
      // Once several speakers share a channel the model has no other way to tell
      // who is talking, and who is talking is the whole of the routing.
      const speaker = message.member?.displayName ?? message.author.displayName ?? message.author.username;
      const body = (agent?.allowBots ?? false) ? `[${speaker}] ${content}` : content;
      let userMessage = body;
      if (filePaths.length === 1) {
        userMessage = `${body}\n\nThe user attached a file: ${filePaths[0]}`.trim();
      } else if (filePaths.length > 1) {
        userMessage = `${body}\n\nThe user attached files:\n${filePaths.map((p) => `- ${p}`).join("\n")}`.trim();
      }
      const prompt = history ? `${history}${userMessage}` : userMessage;

      const systemPrompt = agent
        ? agentSystemPrompt
        : entry.isLocalResume ? RESUME_SYSTEM_PROMPT : SYSTEM_PROMPT;

      let result = await runClaudeStreaming({
        sessionId: entry.sessionId,
        prompt,
        cwd: entry.cwd,
        model: entry.model,
        claudeBin: CLAUDE_BIN,
        resume: entry.started,
        systemPrompt,
        systemPromptMode: resolveSystemPromptMode(agent),
        callbacks: {
          onText: (fullText) => handleStreamText(previewState, fullText),
          onToolUse: createToolUseHandler(previewState),
        },
      });

      // If --resume failed because the session no longer exists, reset and retry fresh
      if (entry.started && result.is_error && /no conversation found/i.test(result.text)) {
        console.log(`[discord-cc-bot] session ${entry.sessionId} not found, starting fresh`);
        entry.sessionId = crypto.randomUUID();
        entry.started = false;
        saveEntry(threadId, entry);
        result = await runClaudeStreaming({
          sessionId: entry.sessionId,
          prompt,
          cwd: entry.cwd,
          model: entry.model,
          claudeBin: CLAUDE_BIN,
          resume: false,
          systemPrompt,
          systemPromptMode: resolveSystemPromptMode(agent),
          callbacks: {
            onText: (fullText) => handleStreamText(previewState, fullText),
            onToolUse: createToolUseHandler(previewState),
          },
        });
      }

      // Cancel any pending throttle timer
      if (previewState.timer) clearTimeout(previewState.timer);

      // Check for AskUserQuestion denials — render as Discord buttons
      const askDenial = result.permissionDenials?.find(d => d.tool_name === "AskUserQuestion");
      if (askDenial) {
        await previewState.msg!.delete().catch(() => {});
        entry.started = true;
        await sendAskButtons(thread ?? message.channel, threadId, entry, askDenial);
        return; // Wait for button click — handler will resume
      }

      // The model declined to speak. Post nothing — in a channel where bots hear
      // each other, silence is what ends the exchange.
      if (result.text.trim().startsWith(SILENT_TOKEN)) {
        await previewState.msg!.delete().catch(() => {});
        entry.started = true;
        patchSessionEntrypoint(entry.sessionId, entry.cwd);
        saveEntry(threadId, entry);
        console.log(`[discord-cc-bot] ${SILENT_TOKEN} in ${threadId} — nothing posted`);
        return;
      }

      const isFirstReply = !entry.started;
      if (isFirstReply) {
        entry.started = true;
      }

      const disclosure = (isFirstReply && !entry.isLocalResume)
        ? "*I'm Claude, an AI assistant by Anthropic.*\n\n"
        : "";
      const responseText = `${disclosure}${result.text}`;

      // Final delivery — always delete preview and send new message
      // so Discord sends a push notification for the completed reply.
      let botReply: Message;
      try {
        await previewState.msg!.delete().catch(() => {});
        if (thread) {
          // Thread-first: send inside the thread
          if (responseText.length <= DISCORD_MAX_LEN) {
            botReply = await thread.send(responseText);
          } else {
            botReply = await sendChunked(thread, responseText);
          }
        } else {
          // Normal: reply to the message directly
          if (responseText.length <= DISCORD_MAX_LEN) {
            botReply = await message.reply(responseText);
          } else {
            botReply = await sendChunked(message.channel, responseText, message);
          }
        }
      } catch (replyErr) {
        console.error("[discord-cc-bot] reply failed, trying fallback:", (replyErr as Error).message);
        botReply = await (thread ?? message.channel).send(responseText.slice(0, DISCORD_MAX_LEN));
      }

      entry.lastBotMessageId = botReply.id;
      patchSessionEntrypoint(entry.sessionId, entry.cwd);
      saveEntry(threadId, entry);
    } catch (err) {
      if (previewState.timer) clearTimeout(previewState.timer);
      if (previewState.msg) {
        await previewState.msg.edit(`Error: ${(err as Error).message}`).catch(() => {});
      } else {
        await message.reply(`Error: ${(err as Error).message}`);
      }
    } finally {
      for (const p of filePaths) fs.unlink(p, () => {});
    }
  } catch (err) {
    console.error("[discord-cc-bot] handler error:", (err as Error).message);
  } finally {
    releaseTurn?.();
  }
});

function shutdown() {
  for (const child of running.values()) child.kill("SIGTERM");
  db.close();
  client.destroy();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

client.login(DISCORD_TOKEN);
