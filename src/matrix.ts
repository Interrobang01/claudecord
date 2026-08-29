// Matrix transport for claudecord.
//
// The point of this module is that the rest of the gateway does not know it
// exists. It presents the two shapes index.ts already used — a room you can
// `.send()` to, and a message you can `.edit()`, `.reply()` to and `.delete()`
// — backed by matrix-bot-sdk instead of discord.js, so the policy code above
// the transport (mention gating, budgets, turn queueing, session groups) ports
// with no changes at all.
//
// Four things genuinely differ from Discord and are handled here:
//   1. There is no bot flag in Matrix. Who is human is an explicit MXID list.
//   2. There are no buttons. AskUserQuestion renders as a numbered list.
//   3. Markdown is not rendered unless you send HTML yourself.
//   4. /sync replays history on reconnect. Events older than start are dropped.

import fs from "node:fs";
import path from "node:path";
import { MatrixClient, SimpleFsStorageProvider } from "matrix-bot-sdk";
import { marked } from "marked";

// Synapse's default max event size is 65536 bytes for the whole event; the body
// has to fit inside that with formatted_body beside it. 16k of markdown leaves
// generous room for the HTML rendering of the same text.
export const MATRIX_MAX_LEN = 16000;

export type Msg = {
  id: string;
  roomId: string;
  edit(text: string): Promise<void>;
  reply(text: string): Promise<Msg>;
  delete(): Promise<void>;
};

export type Room = {
  id: string;
  send(content: string | { content: string }): Promise<Msg>;
};

export type IncomingMessage = {
  id: string;
  roomId: string;
  sender: string;        // full MXID
  senderName: string;    // short label, used for the [Name] prefix
  content: string;
  isHuman: boolean;
  mentionsUs: boolean;
  attachments: { name: string; mxc: string; size: number }[];
};

// --- Markdown -> Matrix HTML ---

// Matrix clients sanitize incoming HTML against the spec's allowed-tag list
// themselves, so this does not need to be a security boundary; it needs to
// produce tags Element will actually keep. Tables survive here and did not on
// Discord, which is why the "never use markdown tables" prompt line is gone.
export function renderHtml(text: string): string {
  try {
    return marked.parse(text, { async: false, breaks: true, gfm: true }) as string;
  } catch {
    return escapeHtml(text).replace(/\n/g, "<br/>");
  }
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
}

function messageBody(text: string): Record<string, any> {
  return {
    msgtype: "m.text",
    body: text,
    format: "org.matrix.custom.html",
    formatted_body: renderHtml(text),
  };
}

// --- Transport ---

export class MatrixTransport {
  readonly client: MatrixClient;
  private userId = "";
  private displayName = "";
  private startTs = 0;
  private humansByRoom: Map<string, Set<string>> = new Map();
  private nameCache: Map<string, string> = new Map();

  constructor(homeserverUrl: string, accessToken: string, storagePath: string) {
    fs.mkdirSync(path.dirname(storagePath), { recursive: true });
    this.client = new MatrixClient(
      homeserverUrl,
      accessToken,
      new SimpleFsStorageProvider(storagePath),
    );
  }

  /** MXIDs that count as human in a given room. Everyone else is a sibling agent. */
  setHumans(roomId: string, mxids: string[]): void {
    this.humansByRoom.set(roomId, new Set(mxids));
  }

  async start(): Promise<{ userId: string; displayName: string }> {
    this.userId = await this.client.getUserId();
    try {
      const p = await this.client.getUserProfile(this.userId);
      this.displayName = p?.displayname || this.userId.split(":")[0].slice(1);
    } catch {
      this.displayName = this.userId.split(":")[0].slice(1);
    }
    // Everything before this instant is history. Matrix replays on reconnect
    // where Discord does not, and a two-hour outage must not wake N turns.
    this.startTs = Date.now();
    await this.client.start();
    return { userId: this.userId, displayName: this.displayName };
  }

  getUserId(): string { return this.userId; }

  async shortName(mxid: string): Promise<string> {
    const cached = this.nameCache.get(mxid);
    if (cached) return cached;
    let name = mxid.split(":")[0].replace(/^@/, "");
    try {
      const p = await this.client.getUserProfile(mxid);
      if (p?.displayname) name = p.displayname;
    } catch { /* localpart is a fine fallback */ }
    this.nameCache.set(mxid, name);
    return name;
  }

  /**
   * Register the message handler. Filters out our own echoes, anything from
   * before startup, and non-text events before the caller ever sees them.
   */
  onMessage(handler: (msg: IncomingMessage) => Promise<void>): void {
    this.client.on("room.message", async (roomId: string, event: any) => {
      try {
        if (!event?.content) return;
        if (event.sender === this.userId) return;              // our own output
        if ((event.origin_server_ts ?? 0) < this.startTs) return; // backfill
        if (event.content["m.relates_to"]?.rel_type === "m.replace") return; // an edit

        const msgtype = event.content.msgtype;
        const isFile = ["m.image", "m.file", "m.audio", "m.video"].includes(msgtype);
        if (msgtype !== "m.text" && !isFile) return;

        const body: string = event.content.body ?? "";
        const attachments = isFile && event.content.url
          ? [{
              name: body || "attachment",
              mxc: event.content.url as string,
              size: event.content.info?.size ?? 0,
            }]
          : [];

        const humans = this.humansByRoom.get(roomId);
        const senderName = await this.shortName(event.sender);

        handler({
          id: event.event_id,
          roomId,
          sender: event.sender,
          senderName,
          content: isFile ? "" : body,
          // No bot flag exists in Matrix. Absent an explicit list, treat every
          // sender as human — the fail-safe direction, since the bot-turn
          // budget only ever suppresses turns.
          isHuman: humans ? humans.has(event.sender) : true,
          mentionsUs: this.mentionsUs(event, body),
          attachments,
        }).catch((err) => console.error("[matrix-cc-bot] handler error:", err));
      } catch (err) {
        console.error("[matrix-cc-bot] room.message error:", err);
      }
    });
  }

  private mentionsUs(event: any, body: string): boolean {
    // The spec's intentional-mentions field, when the sender sets it.
    const m = event.content?.["m.mentions"]?.user_ids;
    if (Array.isArray(m) && m.includes(this.userId)) return true;
    // A pill in formatted_body is an <a href="https://matrix.to/#/@user:server">.
    const fb: string = event.content?.formatted_body ?? "";
    if (fb.includes(`/#/${this.userId}`)) return true;
    // Plain text, which is how one bot addresses another — no pill involved.
    if (this.displayName && new RegExp(`(^|\\s)@?${escapeRegex(this.displayName)}\\b`, "i").test(body)) return true;
    return new RegExp(`(^|\\s)@?${escapeRegex(this.userId.split(":")[0].slice(1))}\\b`, "i").test(body);
  }

  room(roomId: string): Room {
    return {
      id: roomId,
      send: async (content) => {
        const text = typeof content === "string" ? content : content.content;
        const eventId = await this.client.sendMessage(roomId, messageBody(text));
        return this.msg(roomId, eventId);
      },
    };
  }

  msg(roomId: string, eventId: string): Msg {
    return {
      id: eventId,
      roomId,
      edit: async (text) => {
        const inner = messageBody(text);
        await this.client.sendEvent(roomId, "m.room.message", {
          ...inner,
          body: "* " + inner.body,
          "m.new_content": inner,
          "m.relates_to": { rel_type: "m.replace", event_id: eventId },
        });
      },
      reply: async (text) => {
        const inner = messageBody(text);
        const id = await this.client.sendEvent(roomId, "m.room.message", {
          ...inner,
          "m.relates_to": { "m.in_reply_to": { event_id: eventId } },
        });
        return this.msg(roomId, id);
      },
      delete: async () => { await this.client.redactEvent(roomId, eventId); },
    };
  }

  async setTyping(roomId: string, typing: boolean): Promise<void> {
    await this.client.setTyping(roomId, typing, 30000).catch(() => {});
  }

  /** Authenticated media download. Discord CDN URLs were open; mxc:// is not. */
  async downloadMedia(mxc: string, filepath: string): Promise<void> {
    const res = await this.client.downloadContent(mxc);
    fs.mkdirSync(path.dirname(filepath), { recursive: true });
    fs.writeFileSync(filepath, res.data);
  }
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
