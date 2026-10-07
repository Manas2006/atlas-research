// End-to-end check of live docs: the real Atlas binary, the real browser sync
// client, real WebSockets, and a network that misbehaves on purpose.
//
//   node internal/atlas/uitests/e2e.js            (builds and starts Atlas itself)
//   CLIENTS=8 DURATION=20 node internal/atlas/uitests/e2e.js
//
// Several editors type into one document at once while their connections are
// delayed and cut at random and the server is killed with SIGKILL and brought
// back. The run passes only if every editor ends with the server's exact
// text, no editor was ever reset or rejected, suggestion anchors and carets
// agree everywhere, and a final restart serves the same document.
"use strict";
const { execFileSync, spawn } = require("node:child_process");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const OT = require("../ui/ot.js");
const { CollabClient } = require("../ui/collab.js");

const CLIENTS = Number(process.env.CLIENTS || 5);
const SECONDS = Number(process.env.DURATION || 8);
const SEED = Number(process.env.SEED || 20260907);
const root = path.join(__dirname, "../../..");

let seed = SEED;
const random = n => { seed = (Math.imul(seed, 1103515245) + 12345) & 0x7fffffff; return seed % n; };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const isLow = code => code >= 0xdc00 && code <= 0xdfff;
const WORDS = ["agent ", "eval ", "trace ", "reward ", "benchmark ", "é ", "😀 ", "\n", "- ", "reasoning ", "filter "];

const stats = { edits: 0, drops: 0, resets: 0, errors: [], kills: 0, offlineEdits: 0 };
const chaos = { drops: true, delay: 15 };

// ChaosSocket wraps a real WebSocket. It delays traffic in both directions
// while keeping order, and when it dies it takes whatever was still in
// flight with it, the way a dropped TCP connection does.
class ChaosSocket {
  constructor(url) {
    this.readyState = 0;
    this.dead = false;
    this.inbound = Promise.resolve();
    this.outbound = Promise.resolve();
    this.inner = new WebSocket(url);
    this.inner.onopen = () => { if (!this.dead) { this.readyState = 1; if (this.onopen) this.onopen(); } };
    this.inner.onmessage = event => {
      const data = event.data;
      this.inbound = this.inbound.then(() => sleep(random(chaos.delay + 1))).then(() => { if (!this.dead && this.onmessage) this.onmessage({ data }); });
    };
    // Node reports a refused connection with an error and only much later,
    // or never, with a close. Either one ends this socket.
    this.inner.onclose = () => this.kill();
    this.inner.onerror = () => this.kill();
  }
  send(data) {
    if (this.dead) return;
    this.outbound = this.outbound.then(() => sleep(random(chaos.delay + 1))).then(() => { if (!this.dead && this.inner.readyState === 1) this.inner.send(data); });
  }
  close() { this.kill(); }
  kill() {
    if (this.dead) return;
    this.dead = true;
    this.readyState = 3;
    try { this.inner.close(); } catch {}
    setTimeout(() => { if (this.onclose) this.onclose(); }, 0);
  }
}

function freePort() {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => { const { port } = probe.address(); probe.close(() => resolve(port)); });
  });
}

async function startServer(binary, port, dataDir) {
  const child = spawn(binary, ["-listen", `127.0.0.1:${port}`, "-data", dataDir], { stdio: ["ignore", "ignore", "inherit"] });
  for (let attempt = 0; attempt < 200; attempt++) {
    try { if ((await fetch(`http://127.0.0.1:${port}/api/health`)).ok) return child; } catch {}
    await sleep(25);
  }
  child.kill("SIGKILL");
  throw new Error("Atlas did not start");
}

function stopServer(child, signal) {
  return new Promise(resolve => { child.once("exit", resolve); child.kill(signal); });
}

function randomEdit(client) {
  const text = client.text;
  const down = p => { while (p > 0 && p < text.length && isLow(text.charCodeAt(p))) p--; return p; };
  const up = p => { while (p < text.length && isLow(text.charCodeAt(p))) p++; return p; };
  // The first line is left alone so the test has stable text to hang agent
  // suggestions on. Everything after it is fair game.
  const floor = text.indexOf("\n") + 1;
  const start = Math.max(floor, down(floor + random(text.length - floor + 1)));
  const roll = random(10);
  const shrink = text.length > 3000;
  let next, caret;
  if ((roll < 6 && !shrink) || text.length === 0) {
    const word = WORDS[random(WORDS.length)];
    next = text.slice(0, start) + word + text.slice(start);
    caret = start + word.length;
  } else if (roll < 9 || shrink) {
    const end = up(Math.min(text.length, start + 1 + random(shrink ? 40 : 6)));
    next = text.slice(0, start) + text.slice(end);
    caret = start;
  } else {
    const end = up(Math.min(text.length, start + 1 + random(6)));
    const word = WORDS[random(WORDS.length)];
    next = text.slice(0, start) + word + text.slice(end);
    caret = start + word.length;
  }
  if (next === text) return;
  client.applyLocal(OT.fromDiff(text, next, caret));
  client.setCursor(caret, caret);
  client.caret = caret;
  stats.edits++;
  if (!client.ready) stats.offlineEdits++;
}

function describe(client) {
  const socket = client.socket;
  return `${client.clientId} rev=${client.rev} synced=${client.isSynced()} ready=${client.ready} socket=${socket ? socket.readyState : "none"}` +
    ` pending=${client.outstanding ? client.outstanding.id : "-"}${client.buffer ? "+buffer" : ""} reset=${client.resetNext} attempt=${client.attempt}`;
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}

async function main() {
  const work = fs.mkdtempSync(path.join(os.tmpdir(), "atlas-e2e-"));
  const binary = path.join(work, "atlas");
  let base = process.env.ATLAS_URL;
  let server = null, port = 0;
  const dataDir = path.join(work, "data");
  if (!base) {
    execFileSync("go", ["build", "-o", binary, "./cmd/atlas"], { cwd: root, stdio: "inherit" });
    port = await freePort();
    server = await startServer(binary, port, dataDir);
    base = `http://127.0.0.1:${port}`;
  }
  const api = async (route, options) => {
    const response = await fetch(base + route, { headers: { "Content-Type": "application/json" }, ...options });
    const body = await response.json();
    if (!response.ok) throw new Error(`${route}: ${response.status} ${JSON.stringify(body)}`);
    return body;
  };

  try {
    for (const entry of [
      { title: "Agents' Last Exam", type: "Paper", body: "A benchmark for long-horizon agent evaluation with held-out tasks.", tags: ["agents", "evaluation"] },
      { title: "Filtered Reasoning Score", type: "Paper", body: "Ranks reasoning traces by confidence to evaluate reasoning quality.", tags: ["evaluation"] },
      { title: "Reward hacking notes", type: "Note", body: "Shaped rewards raise the strong hack rate in PPO code generation.", tags: ["rl"] }
    ]) await api("/api/entries", { method: "POST", body: JSON.stringify(entry) });

    const { doc } = await api("/api/docs", { method: "POST", body: JSON.stringify({ title: "E2E factoids", template: "factoid", user: { id: "u0", name: "Editor 0" } }) });
    const socketURL = `${base.replace(/^http/, "ws")}/api/docs/${doc.id}/ws`;

    const clients = Array.from({ length: CLIENTS }, (_, index) => {
      const client = new CollabClient({
        url: socketURL, clientId: `e2e${index}`, user: { id: `u${index}`, name: `Editor ${index}` },
        WebSocket: ChaosSocket, backoff: () => 40 + random(80), timeouts: { connect: 2000, ack: 4000 },
        handlers: {
          reset: info => { if (!info.first) stats.resets++; },
          error: reason => stats.errors.push(`${index}: ${reason}`)
        }
      });
      client.caret = 0;
      client.connect();
      return client;
    });
    while (clients.some(client => client.rev === null)) await sleep(10);

    // Everyone types at once. Connections drop at random throughout.
    let running = true;
    const typists = clients.map(async client => {
      while (running) {
        await sleep(random(30));
        randomEdit(client);
        if (chaos.drops && random(120) === 0 && client.socket) { stats.drops++; client.socket.kill(); }
      }
    });

    // Partway through, ask the librarian for related work three times. One
    // suggestion is accepted, one stays open on text nobody edits so its
    // anchor has to keep up with everything typed around it, and one sits
    // in the middle of the typing so it is bound to go stale.
    const invoke = async (client, pick) => {
      await client.whenSynced();
      const range = pick(client.text);
      if (!range) return null;
      const result = await api(`/api/docs/${doc.id}/invoke`, {
        method: "POST",
        body: JSON.stringify({ agent: "librarian", rev: client.rev, start: range[0], end: range[1], instruction: "agent evaluation benchmark", user: client.user })
      });
      return result.suggestion || null;
    };
    const heading = text => { const at = text.indexOf("Factoids"); return at < 0 ? null : [at, at + 8]; };
    const middle = text => { const at = Math.floor(text.length / 2); return /[\ud800-\udfff]/.test(text.slice(at - 1, at + 9)) ? null : [at, Math.min(text.length, at + 8)]; };
    await sleep(SECONDS * 250);
    const accepted = await invoke(clients[0], heading);
    check(accepted && accepted.status === "pending", "the first suggestion should be pending: " + JSON.stringify(accepted));
    clients[0].resolve(accepted.id, "accept");
    const open = await invoke(clients[1], heading);
    const doomed = await invoke(clients[2], middle);

    // Kill the server without warning while people are typing, then bring
    // it back on the same data. Edits continue offline in the meantime.
    if (server) {
      await sleep(SECONDS * 250);
      await stopServer(server, "SIGKILL");
      stats.kills++;
      await sleep(400);
      server = await startServer(binary, port, dataDir);
    }
    await sleep(SECONDS * 500);

    // Stop typing and stop cutting connections, then let everything drain.
    running = false;
    chaos.drops = false;
    await Promise.all(typists);
    let stable = 0, revision = -1;
    for (let waited = 0; stable < 10; waited++) {
      check(waited < 3000, "editors never settled: " + clients.map(describe).join("; ") + (stats.errors.length ? " | errors: " + stats.errors.join("; ") : ""));
      await sleep(20);
      const settled = clients.every(client => client.isSynced() && client.rev === clients[0].rev);
      stable = settled && clients[0].rev === revision ? stable + 1 : 0;
      revision = clients[0].rev;
    }
    // Nothing here moves a caret when someone else edits, as a real editor
    // would, so pin each caret to a valid position before comparing.
    for (const client of clients) {
      client.caret = Math.min(client.caret, client.text.length);
      client.setCursor(client.caret, client.caret);
    }
    await sleep(300);

    const truth = await api(`/api/docs/${doc.id}`);
    check(truth.doc.rev === revision, `server is at revision ${truth.doc.rev}, editors at ${revision}`);
    for (const client of clients) {
      check(client.text === truth.text, `${client.clientId} diverged from the server at revision ${client.rev}`);
    }
    check(stats.resets === 0, `${stats.resets} editors were reset, which means the server lost acknowledged edits`);
    check(stats.errors.length === 0, `editors reported errors: ${stats.errors.join("; ")}`);

    // Every editor must place every open suggestion exactly where the server does.
    for (const suggestion of truth.suggestions) {
      for (const client of clients) {
        const local = client.suggestions.get(suggestion.id);
        check(local && local.start === suggestion.start && local.end === suggestion.end && local.status === suggestion.status,
          `${client.clientId} has suggestion ${suggestion.id} at ${local && [local.start, local.end, local.status]}, server at ${[suggestion.start, suggestion.end, suggestion.status]}`);
      }
      if (suggestion.status === "pending") check(truth.text.slice(suggestion.start, suggestion.end) === suggestion.original, `pending suggestion ${suggestion.id} no longer anchors what the agent read`);
    }
    // Every editor must see every other editor's caret where it really is.
    for (const viewer of clients) {
      check(viewer.peers.size === CLIENTS - 1, `${viewer.clientId} sees ${viewer.peers.size} peers`);
      for (const other of clients) {
        if (other === viewer) continue;
        const seen = viewer.peers.get(other.connId);
        check(seen && seen.pos === other.caret, `${viewer.clientId} sees ${other.clientId} at ${seen && seen.pos}, really at ${other.caret}`);
      }
    }

    const provenance = await api(`/api/docs/${doc.id}/provenance`);
    const attributed = provenance.spans.reduce((sum, span) => sum + span.length, 0);
    check(attributed === truth.text.length, "authorship does not cover the whole text");
    const librarian = provenance.agents.find(agent => agent.agent.id === "librarian");
    check(librarian && librarian.accepted === 1, "the accepted suggestion is missing from provenance: " + JSON.stringify(provenance.agents));
    const stillOpen = truth.suggestions.find(suggestion => suggestion.id === open.id);
    check(stillOpen && stillOpen.status === "pending", "the suggestion on untouched text should still be pending: " + JSON.stringify(stillOpen));

    for (const client of clients) client.close();
    if (server) {
      // A clean restart must serve the same document from the log alone.
      await stopServer(server, "SIGTERM");
      server = await startServer(binary, port, dataDir);
      const again = await api(`/api/docs/${doc.id}`);
      check(again.text === truth.text && again.doc.rev === truth.doc.rev, "the document changed across a restart");
      const found = await api(`/api/search?q=${encodeURIComponent("E2E factoids")}`);
      check(found.results.some(result => result.entry.id === `doc:${doc.id}`), "the document is not searchable after a restart");
    }

    console.log(JSON.stringify({
      ok: true, editors: CLIENTS, edits: stats.edits, offline_edits: stats.offlineEdits, connection_drops: stats.drops, server_kills: stats.kills,
      final_revision: truth.doc.rev, final_units: truth.text.length,
      suggestions: { accepted: librarian.accepted, pending: librarian.pending, stale: librarian.stale, doomed_was_posted: !!doomed },
      units_by_kind: provenance.by_kind, resets: stats.resets
    }));
  } finally {
    if (server) await stopServer(server, "SIGKILL");
    fs.rmSync(work, { recursive: true, force: true });
  }
}

main().then(() => process.exit(0), error => { console.error("FAILED:", error.message); process.exit(1); });
