# Claudecord

A Discord bot that wraps Claude Code CLI, routing different Discord channels to
different Claude Code agents with their own sessions, working directories, and
system prompts.

This file gives Claude Code (or any AI coding assistant working in this repo) a
project-level orientation. It's public-safe — no secrets, no personal paths.

## Project Overview

- **Stack:** TypeScript, Node.js, discord.js, better-sqlite3
- **Entry point:** `src/index.ts` (single-file architecture, ~1500 LOC)
- **Origin:** fork of [fredchu/discord-claude-code-bot](https://github.com/fredchu/discord-claude-code-bot),
  extended with channel-based multi-agent routing, scheduled jobs, file attachments,
  streaming previews, and hot-reloaded context files.

## Architecture

```
Discord message → Channel Router → claude -p "..." --resume <sessionId> → Discord reply
```

- **Thread mode (original):** Responds in threads when @mentioned. Each thread gets its own session.
- **Channel mode (added):** Configured channels in `channel-config.json` respond to all messages.
  Each channel maps to a stable Claude Code session with its own cwd, model, and system prompt.
  `requireMention` narrows a configured channel to mentions only without giving up its
  system prompt or context file.
- **Multi-bot channels:** `allowBots` admits other bots so several instances can talk in one
  channel. Routing is by name (`mentionPatterns`, since a bot posting plain `@name` produces
  no Discord ping), the graceful exit is a `NO_RESPONSE` reply, and `botTurnBudget` is the
  mechanical ceiling on a runaway exchange.

### Key Components (all in src/index.ts)

- **ThreadMap (SQLite):** Maps Discord thread/channel IDs → Claude Code session
  entries (sessionId, cwd, model, etc.).
- **Channel Config:** `channel-config.json` maps Discord channel IDs → agent configs.
  Hot-reloaded via `fs.watchFile`.
- **runClaudeStreaming():** Spawns `claude -p` with `--output-format stream-json`,
  parses streaming output, handles tool use callbacks.
- **Preview system:** Posts "thinking..." message, edits it with streaming partial
  results, then replaces with final chunked response.
- **AskUserQuestion:** Claude's permission denials are rendered as Discord buttons.
- **childEnv():** Builds the environment for every spawned `claude`. Prefers
  `~/.claude/.credentials.json` and blanks `CLAUDE_CODE_OAUTH_TOKEN`, because the env token
  short-circuits the credentials file in the CLI's resolver and declares only
  `user:inference` — an inherited token silently strips scopes with no error anywhere.
- **acquireTurn():** Per-session lane. Mid-turn messages queue (depth 4) rather than being
  rejected; scheduled jobs take the same lane, since `running` is only populated once a
  child has actually spawned.

### Session Management

- Sessions are identified by UUID (threads) or stable config ID (channels)
- `--resume <sessionId>` maintains context across messages
- `/new` resets the session (generates new ID for threads, appends timestamp for channels)
- Spend is capped per turn (`maxCostUsdPerTurn` → `--max-budget-usd`) and per UTC day
  (`maxCostUsdPerDay`, tracked in memory)

## Running the Bot (pm2)

The bot is designed to run under pm2 for auto-restart and persistence.

```bash
# Start
pm2 start ecosystem.config.cjs

# Common operations (replace "claudecord" with whatever you set in ecosystem.config.cjs)
pm2 restart claudecord      # Restart after code changes
pm2 stop claudecord         # Stop the bot
pm2 logs claudecord         # Watch live logs
pm2 logs claudecord --lines 50

# Status
pm2 status
pm2 monit

# Survive reboots
pm2 startup                 # Follow the printed instructions
pm2 save
```

### Without pm2 (development)

```
npm start                   # node --env-file=.env
npm run start:op            # same, but wraps with `op run` for 1Password secrets
npm run check               # TypeScript type-check
```

### Troubleshooting

```bash
# Crash logs
pm2 logs claudecord --err --lines 20

# Node version mismatch after upgrade
npm rebuild better-sqlite3
pm2 restart claudecord

# Duplicate instances (causes double messages)
pkill -f "node --import=tsx src/index.ts"
pm2 restart claudecord
```

## Configuration

- `.env` — `DISCORD_TOKEN`, `GUILD_ID` (optional), `DEFAULT_CWD` (optional), `CLAUDE_BIN` (optional),
  `SILENT_TOKEN` (optional, defaults to `NO_RESPONSE`)
- `channel-config.json` — Channel-to-agent routing. Copy from `channel-config.example.json`
  and fill in real channel IDs.
- `contexts/*.md` — Per-channel context files loaded into the system prompt.
  Copy templates from `contexts/*.example.md`.

## Slash Commands

`/new`, `/stop`, `/model`, `/cd`, `/channels`, `/reload-config`, `/sessions`, `/help`

## Code Style

- Single-file TypeScript with no build step (uses tsx)
- Minimal dependencies — discord.js, better-sqlite3, node-cron
- No Anthropic SDK dependency — spawns Claude Code CLI directly
- Functions are flat, not class-based
- Error handling: try/catch with console.error, graceful fallbacks to user
