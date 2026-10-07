// Operational transformation for plain text. This file mirrors
// internal/collab/ot.go: both sides must produce identical results, and the
// shared vectors in internal/collab/testdata/ot_vectors.json hold them to it.
//
// An operation is an array that walks the whole document: a positive number
// retains that many UTF-16 units, a negative number deletes, a string inserts.
(function (root) {
  "use strict";

  function retain(op, n) {
    if (n <= 0) return op;
    const last = op.length - 1;
    if (last >= 0 && typeof op[last] === "number" && op[last] > 0) op[last] += n;
    else op.push(n);
    return op;
  }

  function remove(op, n) {
    if (n <= 0) return op;
    const last = op.length - 1;
    if (last >= 0 && typeof op[last] === "number" && op[last] < 0) op[last] -= n;
    else op.push(-n);
    return op;
  }

  // An insert that directly follows a delete is stored before it, so equal
  // edits always have one representation.
  function insert(op, text) {
    if (!text) return op;
    const last = op.length - 1;
    if (last >= 0 && typeof op[last] === "string") op[last] += text;
    else if (last >= 0 && op[last] < 0) {
      if (last >= 1 && typeof op[last - 1] === "string") op[last - 1] += text;
      else op.splice(last, 0, text);
    } else op.push(text);
    return op;
  }

  function baseLen(op) {
    let length = 0;
    for (const part of op) if (typeof part === "number") length += Math.abs(part);
    return length;
  }

  function targetLen(op) {
    let length = 0;
    for (const part of op) length += typeof part === "string" ? part.length : Math.max(part, 0);
    return length;
  }

  function isNoop(op) {
    return op.length === 0 || (op.length === 1 && typeof op[0] === "number" && op[0] > 0);
  }

  function apply(text, op) {
    if (text.length !== baseLen(op)) throw new Error("operation does not match the document length");
    let out = "", position = 0;
    for (const part of op) {
      if (typeof part === "string") out += part;
      else if (part > 0) { out += text.slice(position, position + part); position += part; }
      else position -= part;
    }
    return out;
  }

  // A cursor consumes part of a component at a time.
  function cursor(op) {
    let index = 0, used = 0;
    const size = part => (typeof part === "string" ? part.length : Math.abs(part));
    return {
      done: () => index >= op.length,
      isInsert: () => typeof op[index] === "string",
      isRetain: () => typeof op[index] === "number" && op[index] > 0,
      isDelete: () => typeof op[index] === "number" && op[index] < 0,
      remaining: () => size(op[index]) - used,
      take(n) {
        const part = op[index];
        const text = typeof part === "string" ? part.slice(used, used + n) : "";
        used += n;
        if (used === size(part)) { index++; used = 0; }
        return text;
      }
    };
  }

  // compose returns one operation equivalent to applying a and then b.
  function compose(a, b) {
    if (targetLen(a) !== baseLen(b)) throw new Error("operations cannot be composed: lengths differ");
    const out = [], left = cursor(a), right = cursor(b);
    for (;;) {
      if (!left.done() && left.isDelete()) { remove(out, left.remaining()); left.take(left.remaining()); continue; }
      if (!right.done() && right.isInsert()) { insert(out, right.take(right.remaining())); continue; }
      if (left.done() && right.done()) return out;
      if (left.done() || right.done()) throw new Error("operations cannot be composed: lengths differ");
      const n = Math.min(left.remaining(), right.remaining());
      const leftInsert = left.isInsert(), rightDelete = right.isDelete();
      const text = left.take(n);
      right.take(n);
      if (leftInsert) { if (!rightDelete) insert(out, text); }
      else if (rightDelete) remove(out, n);
      else retain(out, n);
    }
  }

  // transform takes two concurrent operations on the same document and
  // returns [a', b'] so that a then b' equals b then a'. When both insert at
  // the same position a's text goes first; always pass the client's operation
  // as a, which is also what the server does.
  function transform(a, b) {
    if (baseLen(a) !== baseLen(b)) throw new Error("operations cannot be transformed: base lengths differ");
    const aPrime = [], bPrime = [], left = cursor(a), right = cursor(b);
    for (;;) {
      if (!left.done() && left.isInsert()) { const text = left.take(left.remaining()); insert(aPrime, text); retain(bPrime, text.length); continue; }
      if (!right.done() && right.isInsert()) { const text = right.take(right.remaining()); retain(aPrime, text.length); insert(bPrime, text); continue; }
      if (left.done() && right.done()) return [aPrime, bPrime];
      if (left.done() || right.done()) throw new Error("operations cannot be transformed: base lengths differ");
      const n = Math.min(left.remaining(), right.remaining());
      const leftDelete = left.isDelete(), rightDelete = right.isDelete();
      left.take(n);
      right.take(n);
      if (!leftDelete && !rightDelete) { retain(aPrime, n); retain(bPrime, n); }
      else if (leftDelete && !rightDelete) remove(aPrime, n);
      else if (!leftDelete && rightDelete) remove(bPrime, n);
    }
  }

  // mapPos moves a position through an operation. afterInsert decides where
  // the position lands when text is inserted exactly at it.
  function mapPos(position, op, afterInsert) {
    let mapped = position, index = 0;
    for (const part of op) {
      if (index > position) break;
      if (typeof part === "string") {
        if (index < position || (index === position && afterInsert)) mapped += part.length;
      } else if (part > 0) index += part;
      else {
        if (index < position) mapped -= Math.min(-part, position - index);
        index -= part;
      }
    }
    return mapped;
  }

  // mapRange moves [start, end) through an operation and returns
  // [start, end, touched]. An untouched range is mapped tightly, so text
  // inserted at either edge stays outside and the range still holds exactly
  // what it held. A touched range is mapped loosely and takes in text
  // inserted at its edges, which keeps it around a word that was replaced.
  function mapRange(start, end, op) {
    let touched = false, index = 0;
    for (const part of op) {
      if (typeof part === "string") { if (index > start && index < end) touched = true; }
      else if (part > 0) index += part;
      else {
        const deleteEnd = index - part;
        if (start === end) { if (index < start && deleteEnd > start) touched = true; }
        else if (index < end && deleteEnd > start) touched = true;
        index = deleteEnd;
      }
    }
    if (touched) return [mapPos(start, op, false), mapPos(end, op, true), true];
    const mappedStart = mapPos(start, op, true);
    return [mappedStart, Math.max(mappedStart, mapPos(end, op, false)), false];
  }

  // editPos is where the author's caret sits after the operation, or -1 when
  // the operation edits nothing.
  function editPos(op) {
    let index = 0, position = -1;
    for (const part of op) {
      if (typeof part === "string") { index += part.length; position = index; }
      else if (part > 0) index += part;
      else position = index;
    }
    return position;
  }

  // fromDiff builds the operation that turns before into after by replacing
  // the span between their common prefix and common suffix. The caret hint
  // keeps the edit where the user actually typed when the text is ambiguous,
  // such as adding one more "a" to a run of them.
  function fromDiff(before, after, caret) {
    const limit = Math.min(before.length, after.length);
    let prefix = 0;
    while (prefix < limit && before.charCodeAt(prefix) === after.charCodeAt(prefix)) prefix++;
    let suffix = 0;
    while (suffix < limit - prefix && before.charCodeAt(before.length - 1 - suffix) === after.charCodeAt(after.length - 1 - suffix)) suffix++;
    if (typeof caret === "number" && prefix + suffix === before.length) {
      // A pure insertion: slide the inserted span left so that it ends at the
      // caret when the repeated characters around it allow that.
      const inserted = after.length - before.length;
      while (prefix > 0 && prefix + inserted > caret && after.charCodeAt(prefix - 1) === after.charCodeAt(prefix - 1 + inserted)) { prefix--; suffix++; }
    }
    // Never split a surrogate pair between the kept and the replaced text.
    const isHigh = code => code >= 0xd800 && code <= 0xdbff;
    const isLow = code => code >= 0xdc00 && code <= 0xdfff;
    if (prefix > 0 && isHigh(after.charCodeAt(prefix - 1))) prefix--;
    if (suffix > 0 && isLow(after.charCodeAt(after.length - suffix))) suffix--;
    const op = [];
    retain(op, prefix);
    insert(op, after.slice(prefix, after.length - suffix));
    remove(op, before.length - prefix - suffix);
    retain(op, suffix);
    return op;
  }

  const api = { retain, remove, insert, baseLen, targetLen, isNoop, apply, compose, transform, mapPos, mapRange, editPos, fromDiff };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.AtlasOT = api;
})(typeof self !== "undefined" ? self : this);
