"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const ui = path.join(__dirname, "../ui");
const html = fs.readFileSync(path.join(ui, "index.html"), "utf8");
const app = fs.readFileSync(path.join(ui, "app.js"), "utf8");
const css = fs.readFileSync(path.join(ui, "styles.css"), "utf8");

test("primary navigation stays focused on the four lab workflows", () => {
  for (const view of ["knowledge", "experiments", "docs", "signals"]) {
    assert.match(html, new RegExp(`class="nav-item" data-view="${view}"`));
  }
  assert.doesNotMatch(html, /class="nav-item" data-view="media"/);
  assert.match(html, /data-view="media">Optional media tools/);
});

test("impact console uses the connected API and exposes decisions and diagnostics", () => {
  assert.match(app, /api\/impact-experiments/);
  assert.match(app, /Absolute lift/);
  assert.match(app, /95% CI/);
  assert.match(app, /insufficient_data/);
  assert.match(app, /duplicates.*late.*unmatched/);
  assert.match(app, /Run synthetic ad demo/);
});

test("impact layout has desktop and narrow viewport rules", () => {
  assert.match(css, /\.impact-metrics\s*\{[^}]*grid-template-columns:\s*repeat\(4,/s);
  assert.match(css, /@media \(max-width: 760px\)[\s\S]*?\.impact-metrics\s*\{[^}]*repeat\(2,/);
  assert.match(css, /@media \(max-width: 460px\)[\s\S]*?\.impact-head/);
});

