// Runs the browser OT implementation against vectors produced by the Go
// implementation, so the two can never drift apart silently.
//   node --test internal/atlas/uitests/ot.test.js
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const OT = require("../ui/ot.js");

const vectors = JSON.parse(fs.readFileSync(path.join(__dirname, "../../collab/testdata/ot_vectors.json"), "utf8"));

test("matches the Go implementation on shared vectors", () => {
  assert.ok(vectors.length >= 200);
  for (const v of vectors) {
    const [aPrime, bPrime] = OT.transform(v.a, v.b);
    assert.deepEqual(aPrime, v.a_prime, `a' for doc ${JSON.stringify(v.doc)}`);
    assert.deepEqual(bPrime, v.b_prime, `b' for doc ${JSON.stringify(v.doc)}`);
    assert.equal(OT.apply(OT.apply(v.doc, v.a), bPrime), v.merged);
    assert.equal(OT.apply(OT.apply(v.doc, v.b), aPrime), v.merged);
    assert.deepEqual(OT.compose(v.a, v.c), v.composed);
    assert.equal(OT.apply(v.doc, v.composed), v.after_ac);
    assert.equal(OT.mapPos(v.pos, v.a, false), v.pos_before);
    assert.equal(OT.mapPos(v.pos, v.a, true), v.pos_after);
    assert.deepEqual(OT.mapRange(v.range[0], v.range[1], v.a), [v.mapped[0], v.mapped[1], v.touched]);
  }
});

test("rejects operations of the wrong length", () => {
  assert.throws(() => OT.apply("abc", [2]));
  assert.throws(() => OT.transform([1], [2]));
  assert.throws(() => OT.compose([1, "x"], [1]));
});

test("fromDiff reproduces the edit", () => {
  const cases = [
    ["", "hello"], ["hello", ""], ["hello", "help"], ["hello world", "hello big world"],
    ["aaa", "aaaa"], ["abcabc", "abc"], ["x😀y", "x😁y"], ["😀", "😀😀"], ["line\n", "line\n\n"]
  ];
  for (const [before, after] of cases) {
    const op = OT.fromDiff(before, after);
    assert.equal(OT.apply(before, op), after);
  }
});

test("fromDiff keeps an ambiguous insert at the caret", () => {
  // Typing one more "a" at the start of "aaa" leaves the caret at 1.
  assert.deepEqual(OT.fromDiff("aaa", "aaaa", 1), ["a", 3]);
  assert.deepEqual(OT.fromDiff("aaa", "aaaa", 4), [3, "a"]);
  assert.deepEqual(OT.fromDiff("aaa", "aaaa", 2), [1, "a", 2]);
});

test("fromDiff never splits a surrogate pair", () => {
  const op = OT.fromDiff("x😀y", "x😁y");
  assert.deepEqual(op, [1, "😁", -2, 1]);
  for (const part of op) {
    if (typeof part !== "string") continue;
    assert.ok(!/[\ud800-\udbff]$/.test(part) && !/^[\udc00-\udfff]/.test(part));
  }
});

test("fromDiff output survives random edits", () => {
  let seed = 7;
  const random = n => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
  const alphabet = ["a", "b", " ", "\n", "é", "😀"];
  const text = () => Array.from({ length: random(8) }, () => alphabet[random(alphabet.length)]).join("");
  for (let round = 0; round < 5000; round++) {
    const head = text(), tail = text();
    const before = head + text() + tail, after = head + text() + tail;
    const op = OT.fromDiff(before, after, head.length + random(4));
    assert.equal(OT.apply(before, op), after, `${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
    assert.equal(OT.baseLen(op), before.length);
    assert.equal(OT.targetLen(op), after.length);
  }
});

test("editPos follows the last edit", () => {
  assert.equal(OT.editPos([3, "ab", 2]), 5);
  assert.equal(OT.editPos([3, -2]), 3);
  assert.equal(OT.editPos([5]), -1);
});
