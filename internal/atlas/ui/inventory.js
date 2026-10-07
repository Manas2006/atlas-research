// Turns the public Drive inventory (docs/drive-inventory.json, published
// beside the console on GitHub Pages) into knowledge entries for the browser
// demo. It has no DOM dependencies, so the same file runs in the browser and
// under Node for unit tests.
//
// Records marked sensitive are published redacted: they have no url, and
// their id is "redacted-" plus the first 12 hex digits of the SHA-256 of the
// Drive file ID, which keeps records distinct without revealing the file.
// Entries carry no source link at all, so a redacted record cannot produce a
// broken one.
(function (root) {
  "use strict";

  const REDACTED_ID = /^redacted-[0-9a-f]{12}$/;

  function isRedacted(record) {
    return Boolean(record && (record.sensitive || REDACTED_ID.test(String(record.id || ""))));
  }

  function entry(record) {
    const redacted = isRedacted(record);
    const extraction = record.status === "ready"
      ? `${record.extracted_chars || 0} characters are indexed in the protected Atlas runtime.`
      : `Not extracted: ${record.skip_reason || record.status}.`;
    const notice = redacted ? "Sensitive: its Drive ID and link are redacted from the public inventory. " : "";
    return {
      id: `drive-demo:${record.id}`,
      title: record.title,
      type: record.type || "Research source",
      body: `Metadata-only public demo for ${record.path}. ${notice}${extraction}`,
      tags: ["drive-inventory", ...(record.tags || []), record.status, ...(redacted ? ["redacted"] : [])],
      updated_at: record.modified_at
    };
  }

  function entries(inventory) {
    return (inventory && Array.isArray(inventory.records) ? inventory.records : []).map(entry);
  }

  const api = { entries, isRedacted };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.AtlasInventory = api;
})(typeof self !== "undefined" ? self : this);
