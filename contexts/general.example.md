# General Channel

General-purpose chat and questions channel. Loose context — any topic is fair game.

## Purpose
- Quick questions, brainstorming, general discussion
- Anything that doesn't fit a more specific channel
- Ad-hoc coding help, research, or task automation

## Environment
- Working directory: /path/to/your/working/directory
- The bot has access to the local filesystem and can read/write files, run commands, and manage git repos

## Conversation Style
- Respond naturally and concisely
- Direct, technical answers preferred over excessive hedging

## Tips for writing context files

This file is loaded by the bot whenever a message lands in the associated channel.
Use it to:
- Set a persona or conversation style
- List tools, commands, or APIs the agent should know about
- Describe the channel's purpose so the agent knows what's in-scope
- Point at files, directories, or knowledge bases the agent should reference

Keep it concise — long context files burn tokens on every message.
