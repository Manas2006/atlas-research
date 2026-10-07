// Load test for live docs: many documents, a few editors in each, everyone
// typing at a human pace. This is the shape real use has; a thousand people
// in one document is not. It reports how long an edit waits for its
// acknowledgement, which includes the fsync, and checks every document
// converged.
//
//   node internal/atlas/uitests/load.js
//   DOCS=200 EDITORS=4 RATE=5 DURATION=30 node internal/atlas/uitests/load.js
//
// Record the hardware, the commit, and these settings with any number you
// quote, as docs/benchmarking.md asks.
"use strict";
const { execFileSync, spawn } = require("node:child_process");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const OT = require("../ui/ot.js");
const { CollabClient } = require("../ui/collab.js");

const DOCS = Number(process.env.DOCS || 50);
const EDITORS = Number(process.env.EDITORS || 4);
const RATE = Number(process.env.RATE || 5); // edits per editor per second
const SECONDS = Number(process.env.DURATION || 10);
const root = path.join(__dirname, "../../..");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const WORDS = ["agent ", "eval ", "trace ", "reward ", "benchmark ", "reasoning ", "\n"];

function freePort() {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => { const { port } = probe.address(); probe.close(() => resolve(port)); });
  });
}

async function main() {
  const work = fs.mkdtempSync(path.join(os.tmpdir(), "atlas-load-"));
  const binary = path.join(work, "atlas");
  execFileSync("go", ["build", "-o", binary, "./cmd/atlas"], { cwd: root, stdio: "inherit" });
  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  const server = spawn(binary, ["-listen", `127.0.0.1:${port}`, "-data", path.join(work, "data")], { stdio: "ignore" });
  try {
    for (let attempt = 0; ; attempt++) {
      try { if ((await fetch(base + "/api/health")).ok) break; } catch {}
      if (attempt > 200) throw new Error("Atlas did not start");
      await sleep(25);
    }
    const latencies = [];
    const clients = [];
    const docs = [];
    for (let d = 0; d < DOCS; d++) {
      const response = await fetch(base + "/api/docs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ title: `Load ${d}`, template: "factoid", user: { id: "load", name: "Load" } }) });
      const { doc } = await response.json();
      docs.push(doc.id);
      for (let e = 0; e < EDITORS; e++) {
        const client = new CollabClient({ url: `ws://127.0.0.1:${port}/api/docs/${doc.id}/ws`, clientId: `load${d}x${e}`, user: { id: `u${e}`, name: `Editor ${e}` }, handlers: { error: reason => { throw new Error(reason); } } });
        // Time each operation from the moment it is sent to the moment the
        // server says it is durable.
        const send = client.sendOutstanding.bind(client), acknowledged = client.acknowledged.bind(client);
        let sentAt = 0;
        client.sendOutstanding = () => { sentAt = performance.now(); send(); };
        client.acknowledged = (rev, echo) => { if (!echo && sentAt) latencies.push(performance.now() - sentAt); return acknowledged(rev, echo); };
        client.doc = doc.id;
        client.connect();
        clients.push(client);
      }
    }
    while (clients.some(client => client.rev === null)) await sleep(20);

    const started = performance.now();
    let edits = 0;
    await Promise.all(clients.map(async client => {
      await sleep(Math.random() * 1000 / RATE);
      while (performance.now() - started < SECONDS * 1000) {
        const text = client.text;
        const at = Math.floor(Math.random() * (text.length + 1));
        const word = WORDS[Math.floor(Math.random() * WORDS.length)];
        client.applyLocal(OT.fromDiff(text, text.slice(0, at) + word + text.slice(at), at + word.length));
        edits++;
        await sleep(1000 / RATE * (0.5 + Math.random()));
      }
    }));
    await Promise.all(clients.map(client => client.whenSynced()));
    const elapsed = (performance.now() - started) / 1000;

    let diverged = 0;
    for (const id of docs) {
      const truth = await (await fetch(`${base}/api/docs/${id}`)).json();
      for (const client of clients) if (client.doc === id && client.text !== truth.text) diverged++;
    }
    latencies.sort((a, b) => a - b);
    const at = q => latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * q))].toFixed(1);
    console.log(JSON.stringify({
      docs: DOCS, editors_per_doc: EDITORS, connections: clients.length, seconds: Number(elapsed.toFixed(1)),
      edits, acknowledged_ops: latencies.length, ops_per_second: Math.round(latencies.length / elapsed),
      ack_ms: { p50: Number(at(0.5)), p95: Number(at(0.95)), p99: Number(at(0.99)), max: Number(latencies[latencies.length - 1].toFixed(1)) },
      diverged_editors: diverged, cpus: os.cpus().length, node: process.version
    }));
    for (const client of clients) client.close();
    if (diverged) throw new Error(`${diverged} editors diverged`);
  } finally {
    server.kill("SIGKILL");
    fs.rmSync(work, { recursive: true, force: true });
  }
}

main().then(() => process.exit(0), error => { console.error("FAILED:", error.message); process.exit(1); });
