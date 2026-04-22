# Email Triage Channel (Example)

An example of a scheduled-job channel. This channel is dedicated to email management
and triage, with an agent that runs daily at 8am local time and posts a briefing to
the channel as a thread.

This is a rich example — it pairs a context file with a `schedule` block in
`channel-config.json` to produce a fully automated daily agent.

## Setup assumptions

- You have a CLI tool that can query your email account (e.g. Gmail via the API,
  Apple Mail via `osascript`, Fastmail via JMAP, etc.). The example below uses a
  placeholder `your-gmail-cli` — replace with whatever tool you actually have.
- Your email account has labels/folders you want to auto-apply.

## Purpose
- Automated daily email briefing (runs at 8am via cron in `channel-config.json`)
- Ad-hoc email queries and inbox management from within the channel
- Label management and filter creation

## Tools
- Uses a CLI tool (`your-gmail-cli`) to interact with your email provider
- Always passes the account address via `-a user@example.com`

## Example labels (customize for your own inbox)
- GitHub (Label_30)
- News (Label_31)
- Job Search (Label_32)
- Bills & Finance (Label_33)
- Social/Noise (Label_34)
- Promotions/Marketing (Label_35)
- Personal (Label_1)
- Receipts (Label_2)
- Travel (Label_3)

## Daily Briefing Format
The scheduled job scans the inbox, auto-labels unlabeled emails, flags high-priority
items, marks low-priority as read, and cleans up old promotional/social/noise emails.
The briefing uses emoji-labeled sections for priority, new mail, worth-a-look, filter
updates, and cleanup stats.

## How to hook this up

1. Copy this file to `contexts/email-triage.md`
2. In `channel-config.json`, add a channel entry with:
   - `contextFile: "contexts/email-triage.md"`
   - A `schedule` block with your preferred cron expression
   - A `prompt` telling the agent to run the triage steps
3. Replace `your-gmail-cli` references with your actual email CLI
4. Restart the bot (`pm2 restart <name>`)

## Formatting notes
- Discord does NOT render markdown tables — never use `| col | col |` syntax
- Use bold labels with dash-separated attributes instead
- Keep the briefing concise but comprehensive
