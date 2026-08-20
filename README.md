# claudecord

A Discord bot that bridges Discord channels and threads to [Claude Code](https://docs.anthropic.com/en/docs/claude-code) CLI sessions. Single file TypeScript, minimal dependencies.

Drop claudecord into a Discord server and every channel can become its own Claude Code agent — own working directory, own model, own system prompt, own persistent session. You can @-mention it in any thread for one-off help, or configure dedicated channels where every message is a turn in a long-running Claude Code conversation. Schedule agents to post daily briefings on cron. Send file attachments. Stream live previews while Claude is thinking.

## Features

- **Channel routing** — map Discord channel IDs to Claude Code agents via `channel-config.json` (hot-reloaded). Each channel has its own cwd, model, system prompt, and context file.
- **Thread sessions** — @mention the bot in any thread for a spawn-on-demand agent with `--resume` persistence.
- **Scheduled jobs** — cron-based agents that post to a channel on a schedule (great for daily digests).
- **File attachments** — images, code, PDFs, or any file — auto-downloaded and passed to Claude Code's Read tool.
- **Streaming preview** — real-time response preview with tool-use status line while Claude is working.
- **Interactive permission buttons** — AskUserQuestion prompts rendered as Discord buttons.
- **Terminal takeover** — resume any bot session from your terminal via `claude --resume <id>`. Same brain, two entry points.
- **Reverse takeover** — `/resume-local` picks up a terminal CC session from Discord (mobile use case); `/handback` returns it.
- **SQLite storage** — crash-safe session persistence with WAL mode.
- **Slash commands** — `/new`, `/model`, `/cd`, `/stop`, `/channels`, `/reload-config`, `/sessions`, `/resume-local`, `/handback`, `/help`.

## Quick Start

```bash
git clone https://github.com/ecmulli/claudecord.git
cd claudecord
npm install

# Copy templates
cp .env.example .env
cp channel-config.example.json channel-config.json
cp contexts/general.example.md contexts/general.md

# Edit .env (add DISCORD_TOKEN) and channel-config.json (add real channel IDs)
# Then:
npm start
```

### Prerequisites

- Node.js 22+ (uses `--env-file`)
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated
- A Discord bot token ([Discord Developer Portal](https://discord.com/developers/applications))
- A C++ compiler toolchain for `better-sqlite3` (Xcode CLT on macOS, `build-essential` on Linux)

### Discord bot setup

1. Create an application in the Discord Developer Portal.
2. Go to **Bot** → enable **Message Content Intent**.
3. Go to **OAuth2 → URL Generator** → select `bot` + `applications.commands`.
4. Bot permissions: Send Messages, Read Message History, Attach Files, Use Slash Commands, Create Public Threads, Send Messages in Threads.
5. Use the generated URL to invite the bot to your server.

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `DISCORD_TOKEN` | Yes | Bot token from the Discord Developer Portal. |
| `GUILD_ID` | No | Server ID for instant slash command registration. Without it, global commands take up to 1 hour to propagate. |
| `DEFAULT_CWD` | No | Default working directory for Claude Code (defaults to `process.cwd()`). |
| `CLAUDE_BIN` | No | Path to Claude Code binary (defaults to `claude`). |

## Channel configuration

Two modes coexist:

1. **Thread mode** (default) — anywhere the bot is @mentioned in a thread, it spawns a fresh Claude Code session for that thread.
2. **Channel mode** — any channel listed in `channel-config.json` becomes a dedicated agent. Every message is a turn; the bot replies without needing a mention.

A minimal `channel-config.json`:

```json
{
  "channels": {
    "1234567890123456789": {
      "name": "general",
      "sessionId": "general-001",
      "systemPrompt": "You are a helpful assistant...",
      "workingDirectory": "/path/to/project",
      "model": "sonnet",
      "replyInThread": true,
      "contextFile": "contexts/general.md"
    }
  }
}
```

See `channel-config.example.json` for the full schema, including scheduled jobs.

### Channel options

| Key | Default | What |
|---|---|---|
| `systemPrompt` | — | Extra system prompt for this channel. |
| `systemPromptMode` | `append` | `append` adds to Claude Code's own system prompt (`--append-system-prompt`); `replace` discards it (`--system-prompt`). |
| `requireMention` | `false` | Answer only when mentioned, instead of on every message. |
| `mentionPatterns` | — | Case-insensitive regexes also counted as a mention. A bot posting plain `@name` produces no Discord ping — only the `<@id>` form does — so text-level matching is what makes name-based routing work between bots. |
| `allowBots` | `false` | Admit messages from other bots. Also switches on speaker labelling: each message reaches the model as `[Name] text`. |
| `botTurnBudget` | `6` | Consecutive bot-triggered turns allowed before the channel goes quiet until a human speaks. |
| `fetchHistory` | `true` | Prepend recent channel messages to the prompt. History filters only *this* bot's own messages, so in a channel shared with other bots it pulls their traffic in whether or not this agent was addressed — turn it off there. |
| `replyInThread` | `false` | Open a thread per message. Costs a second Claude call (Haiku) to title the thread. |

### Channels shared by several bots

`allowBots: true` is what lets two instances hold a conversation, and therefore
also what lets them answer each other indefinitely. Three things bound it:

- **`requireMention` + `mentionPatterns`** — an instance only wakes when named,
  so a turn that names nobody is the end of the exchange.
- **`NO_RESPONSE`** — a final message beginning with the token (configurable via
  `SILENT_TOKEN`) posts nothing at all. The graceful exit.
- **`botTurnBudget`** — the mechanical ceiling, because the first two are
  instructions and the observed runaway is two agents being *polite* at each
  other rather than either one misbehaving. Any human message clears the count.

Turns are serialized per session and queue rather than being rejected, up to
`MAX_QUEUE_DEPTH` (4); scheduled jobs take the same lane.

### Per-channel context files

`contexts/*.md` files get inlined into the system prompt for their channel. Use them to set a persona, list tools the agent should know about, or describe the channel's purpose. Context files are hot-reloaded. Templates live in `contexts/*.example.md`.

### Scheduled agents

Add a `schedule` block to any channel config to run it on a cron. See `contexts/email-triage.example.md` for a fully worked example: a daily 8am email-triage agent that scans, labels, and posts a briefing.

## Running with pm2 (recommended)

pm2 keeps the bot alive across crashes and reboots.

```bash
npm install -g pm2

pm2 start ecosystem.config.cjs
pm2 restart claudecord      # After code/config changes
pm2 stop claudecord
pm2 logs claudecord
pm2 status

# Survive reboots
pm2 startup                 # Follow the printed instructions
pm2 save
```

### Troubleshooting

```bash
# Node version mismatch after Node upgrade
npm rebuild better-sqlite3
pm2 restart claudecord

# Duplicate instances (double messages)
pkill -f "node --import=tsx src/index.ts"
pm2 restart claudecord

# Crash logs
pm2 logs claudecord --err --lines 20
```

## Architecture

```
Discord channel/thread     claudecord                 Claude Code CLI
─────────────────────     ──────────                 ───────────────
message  ───────────►     route by channel ID
                          fetch recent context
                          build prompt
                          spawn claude -p ─────────►  --session-id UUID
                                                      (or --resume UUID)
         ◄───────────     stream stdout back    ◄──── stream-json output
reply / .txt attachment
```

Single-file architecture: `src/index.ts` (~1700 LOC). Session state lives in a SQLite DB (`threads.db` with WAL mode) mapping Discord channel/thread IDs to Claude Code session UUIDs.

## Slash commands

| Command | Description |
|---|---|
| `/help` | Show available commands |
| `/new` | Clear context, start a new conversation |
| `/model <name>` | Switch Claude model (sonnet, opus, haiku) |
| `/cd <path>` | Switch working directory |
| `/stop` | Kill running Claude process |
| `/channels` | List configured channels |
| `/reload-config` | Hot-reload `channel-config.json` |
| `/sessions` | List all active sessions |
| `/resume-local [session]` | Resume a local terminal CC session |
| `/handback` | Hand a session back to the terminal |

## Advanced: terminal takeover

Each bot session is a standard Claude Code session. Resume any of them from the terminal:

```bash
# find the session ID
sqlite3 threads.db "SELECT sessionId FROM threads WHERE threadId = '<channel-or-thread-id>'"

# resume interactively
claude --resume <session-id>
```

Same "brain" — Claude remembers everything from Discord. You get the full interactive experience (diffs, permission confirmations, tool use) in the terminal while Discord stays as-is.

**Use case:** start a task from Discord on your phone → walk to your desk → finish in terminal → report back in Discord. One session, two entry points.

## Reverse takeover: resume terminal from Discord

```
Terminal: /quit           ← exit Claude Code
Discord:  /resume-local   ← bot discovers your session, shows a picker
Discord:  @bot message    ← continue from your phone
Discord:  /handback       ← when done, hand it back
Terminal: claude --continue
```

The bot auto-discovers sessions from `~/.claude/sessions/` (active PIDs) and `~/.claude/history.jsonl` (recent sessions). The select menu shows the last prompt for each session.

> You must `/quit` Claude Code in the terminal before resuming from Discord — Claude Code does not allow two processes to resume the same active session.

## Origin

Forked from [fredchu/discord-claude-code-bot](https://github.com/fredchu/discord-claude-code-bot) and extended with channel-based multi-agent routing, scheduled jobs, context files, file attachments, streaming previews, and hot-reloaded config.

## License

MIT
