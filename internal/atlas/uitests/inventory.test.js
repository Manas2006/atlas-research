"use strict";

// The Drive inventory is published on GitHub Pages, so these checks guard
// what it may contain as well as how the console renders it.
//   node --test internal/atlas/uitests/inventory.test.js
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const Inventory = require("../ui/inventory.js");

const raw = fs.readFileSync(path.join(__dirname, "../../../docs/drive-inventory.json"), "utf8");
const inventory = JSON.parse(raw);
const REDACTED_ID = /^redacted-[0-9a-f]{12}$/;

test("the published inventory names no Drive folders", () => {
  assert.equal(inventory.source_root, undefined);
  assert.doesNotMatch(raw, /drive\/folders/);
  assert.equal(inventory.folders.length, inventory.summary.folders_including_root);
  for (const folder of inventory.folders) {
    assert.deepEqual(Object.keys(folder), ["path"], `folder ${folder.path} keeps only its path`);
  }
  assert.ok(inventory.folders.some(folder => folder.path === "/"));
});

test("sensitive records are redacted and stay distinct", () => {
  const sensitive = inventory.records.filter(record => record.sensitive);
  assert.equal(sensitive.length, inventory.summary.sensitive);
  assert.ok(sensitive.length > 0);
  for (const record of sensitive) {
    assert.match(record.id, REDACTED_ID, record.title);
    assert.equal(record.url, undefined, `${record.title} has no url`);
    assert.ok(record.title && record.path && record.status, `${record.title} keeps its metadata`);
  }
  const ids = inventory.records.map(record => record.id);
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(inventory.records.length, inventory.summary.files);
});

test("only non-sensitive records carry Drive IDs and links", () => {
  const urls = raw.match(/https?:\/\/[^"]+/g) || [];
  const allowed = new Set(inventory.records.filter(record => !record.sensitive).map(record => record.url));
  for (const url of urls) assert.ok(allowed.has(url), `unexpected link ${url}`);
  for (const record of inventory.records.filter(record => !record.sensitive)) {
    assert.doesNotMatch(record.id, REDACTED_ID);
    assert.ok(record.url.includes(record.id), `${record.title} links to its own file`);
  }
});

test("the console renders every record, redacted ones without links", () => {
  const entries = Inventory.entries(inventory);
  assert.equal(entries.length, inventory.records.length);
  inventory.records.forEach((record, index) => {
    const entry = entries[index];
    assert.equal(entry.id, `drive-demo:${record.id}`);
    assert.equal(entry.title, record.title);
    assert.equal(entry.updated_at, record.modified_at);
    assert.ok(entry.body.startsWith(`Metadata-only public demo for ${record.path}. `));
    assert.doesNotMatch(JSON.stringify(entry), /https?:\/\//, "entries never carry a link");
    if (record.sensitive) {
      assert.match(entry.body, /redacted from the public inventory/);
      assert.ok(entry.tags.includes("redacted"));
    } else {
      assert.doesNotMatch(entry.body, /redacted/);
      assert.ok(!entry.tags.includes("redacted"));
    }
  });
});

test("a sensitive record that still has an id and url is never exposed", () => {
  const [entry] = Inventory.entries({ records: [{ id: "redacted-0123456789ab", title: "Lab Accounts", type: "Lab wiki", path: "lab wiki/Lab Accounts", sensitive: true, status: "ready", extracted_chars: 12, url: "https://docs.google.com/document/d/secret/edit", tags: ["lab-wiki"] }] });
  assert.doesNotMatch(JSON.stringify(entry), /secret|https?:/);
  assert.equal(entry.type, "Lab wiki");
  assert.match(entry.body, /12 characters are indexed/);
  assert.ok(Inventory.isRedacted({ id: "redacted-0123456789ab" }));
  assert.ok(!Inventory.isRedacted({ id: "1c6V73xydOwVMJhNzZm4vQ", sensitive: false }));
  assert.deepEqual(Inventory.entries({}), []);
  assert.deepEqual(Inventory.entries(null), []);
});
