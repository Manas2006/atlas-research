// The Live docs view: a shared plain-text editor where lab members and agents
// work in the same document. Sync lives in collab.js; this file is the
// surface. It relies on helpers that app.js defines (state, toast, modal,
// closeModal, escapeHTML, relative, load, save, render, connectionModal).
const AtlasDocs = (() => {
  "use strict";
  const OT = AtlasOT;
  const { CollabClient } = AtlasCollab;
  const tabId = `tab${Math.random().toString(36).slice(2, 10)}`;
  const CLIP = 320;
  const MAX_UNITS = 1000000; // the server's document limit, in UTF-16 units
  const PLACEHOLDER = "Start writing. Everyone with this page open sees it as you type.";

  const ui = {
    docs: null, damaged: [], agents: [], templates: ["blank"], loadedFor: "", loading: false, stale: false, listTimer: null,
    docId: null, endpoint: "", client: null, missingId: null, namePrompted: false,
    // The editor's DOM is kept across re-renders of the console, so a
    // refresh elsewhere on the page never costs the caret or a draft.
    root: null, focus: null,
    agent: "librarian", busy: false, hover: null,
    authorship: false, provenance: null, provenanceTimer: null,
    pins: new Set(),
    // While an input method is composing, remote edits are held back from
    // the textarea: composing is the state of the field, and replacing its
    // value would break the composition. shadow is what the field showed
    // before the latest keystroke; deferred is what it has not been shown.
    composing: false, shadow: "", deferred: null,
    retiring: new Set()
  };

  const $ = selector => document.querySelector(selector);
  const routeId = () => (location.hash.slice(1).split("/")[1] || "");
  const user = () => load("atlas.user", null);
  const clip = (text, limit = CLIP) => (text.length > limit ? text.slice(0, limit).trimEnd() + "..." : text);
  const initials = name => name.split(/\s+/).filter(Boolean).slice(0, 2).map(part => part[0].toUpperCase()).join("") || "?";
  const hasPending = client => !!(client && (client.outstanding || client.buffer));

  async function request(path, options = {}) {
    const response = await fetch(`${state.endpoint}${path}`, { ...options, headers: { "Content-Type": "application/json", ...(options.headers || {}) } });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || response.statusText);
    return body;
  }

  // ---- list ---------------------------------------------------------------

  // ensureDocs fetches the doc list, the agents, and the templates. It runs
  // once per runtime and again whenever the list has been marked stale.
  async function ensureDocs() {
    if (ui.loading || (ui.docs !== null && !ui.stale && ui.loadedFor === state.endpoint)) return;
    ui.loading = true;
    ui.stale = false;
    ui.loadedFor = state.endpoint;
    try {
      const data = await request("/api/docs");
      ui.docs = data.docs || [];
      ui.damaged = data.damaged || [];
      ui.agents = data.agents || [];
      ui.templates = data.templates || ["blank"];
      if (!ui.agents.some(agent => agent.id === ui.agent) && ui.agents.length) ui.agent = ui.agents[0].id;
    } catch (error) {
      ui.docs = ui.docs || [];
      toast(`Could not load docs: ${error.message}`);
    }
    ui.loading = false;
    if (state.view === "docs") render();
  }

  function headingHTML(title, copy, actions) {
    return `<section class="page-heading"><div><p class="eyebrow">Live workspace</p><h1>${title}</h1><p>${copy}</p></div><div class="heading-actions">${actions}</div></section>`;
  }

  const TAGLINE = "Write together, with agents that suggest instead of overwrite.";

  function disconnectedHTML() {
    return headingHTML("Live docs", TAGLINE, "") +
      `<section class="panel docs-empty"><span class="upload-icon">✎</span><h2>Live docs need a shared runtime</h2><p>Browser mode keeps everything on this device, so there is no one to write with. Connect to an Atlas service that your lab can reach and every doc becomes a shared, durable, searchable page.</p><button class="button primary" data-action="docs-connect">Connect a runtime</button></section>`;
  }

  function nameGateHTML() {
    return headingHTML("Live docs", TAGLINE, `<a class="button secondary" href="#docs">Back to docs</a>`) +
      `<section class="panel docs-empty"><span class="upload-icon">✎</span><h2>Choose a name to join</h2><p>Your name labels your cursor and your edits for everyone else in the doc.</p><button class="button primary" data-action="docs-name">Choose a name</button></section>`;
  }

  function missingHTML() {
    return headingHTML("Doc not found", "It may have been created on a different Atlas runtime.", `<a class="button secondary" href="#docs">Back to docs</a>`);
  }

  function listHTML() {
    const heading = headingHTML("Live docs", TAGLINE, `<button class="button primary" data-action="new-doc">＋ New doc</button>`);
    const damaged = ui.damaged.length ? `<section class="panel docs-damaged"><strong>${ui.damaged.length === 1 ? "One doc" : `${ui.damaged.length} docs`} could not be opened.</strong>${ui.damaged.map(doc => `<span><code>${escapeHTML(doc.id)}</code> ${escapeHTML(doc.error)}</span>`).join("")}<small>The files were left untouched in the runtime's docs folder.</small></section>` : "";
    if (!ui.docs) return heading + `<section class="panel"><div class="empty">Loading docs</div></section>`;
    if (!ui.docs.length) {
      return heading + damaged + `<section class="panel docs-empty"><span class="upload-icon">✎</span><h2>No docs yet</h2><p>Start with the factoid template for the weekly lab meeting, or a blank page. Everything typed here is saved as it is written and shows up in Knowledge search.</p><button class="button primary" data-action="new-doc">Start a doc</button></section>`;
    }
    const rows = ui.docs.map(doc => `<a class="doc-row" href="#docs/${escapeHTML(doc.id)}"><span class="type doc">D</span><span><strong>${escapeHTML(doc.title)}</strong><small>${doc.length.toLocaleString()} characters · revision ${doc.rev}</small></span><span class="doc-live">${doc.editors ? `<i class="status-dot live"></i>${doc.editors} editing` : ""}</span><span>${doc.open_suggestions ? `<i class="chip">${doc.open_suggestions} suggestion${doc.open_suggestions === 1 ? "" : "s"}</i>` : ""}</span><em>${relative(doc.updated_at)}</em></a>`).join("");
    return heading + damaged + `<section class="panel table-panel"><div class="table-toolbar"><span>Every edit is written to a per-document log before it is acknowledged</span><em>${ui.docs.length} doc${ui.docs.length === 1 ? "" : "s"}</em></div><div class="doc-list">${rows}</div></section>`;
  }

  function newDocModal() {
    const options = ui.templates.map(name => `<option value="${escapeHTML(name)}"${name === "factoid" ? " selected" : ""}>${escapeHTML(name[0].toUpperCase() + name.slice(1))}</option>`).join("");
    modal(`<button class="modal-close" data-close>×</button><p class="modal-kicker">Live docs</p><h2>Start a doc</h2><p class="modal-copy">Anyone who can reach this Atlas runtime can open it and write with you.</p><form id="docForm" class="form"><label>Title<input name="title" required autofocus maxlength="120" placeholder="F26 factoids, week 7"></label><label>Template<select name="template">${options}</select></label><div class="form-actions"><button type="button" class="button ghost" data-close>Cancel</button><button class="button primary">Create doc</button></div></form>`);
    $("#docForm input").focus();
    $("#docForm").addEventListener("submit", async event => {
      event.preventDefault();
      const data = new FormData(event.target);
      try {
        const created = await request("/api/docs", { method: "POST", body: JSON.stringify({ title: data.get("title"), template: data.get("template"), user: user() || {} }) });
        closeModal();
        ui.stale = true;
        location.hash = `docs/${created.doc.id}`;
      } catch (error) { toast(error.message); }
    });
  }

  function identityModal(then) {
    const current = user();
    modal(`<button class="modal-close" data-close>×</button><p class="modal-kicker">Live docs</p><h2>What should collaborators call you?</h2><p class="modal-copy">Your name labels your cursor and your edits. Atlas has no accounts yet, so this is taken on trust.</p><form id="identityForm" class="form"><label>Name<input name="name" required maxlength="40" value="${escapeHTML(current ? current.name : "")}" placeholder="Your name"></label><div class="form-actions"><button class="button primary">Continue</button></div></form>`);
    $("#identityForm input").focus();
    $("#identityForm").addEventListener("submit", event => {
      event.preventDefault();
      const name = new FormData(event.target).get("name").trim();
      if (!name) return;
      save("atlas.user", { id: current ? current.id : `u-${Math.random().toString(36).slice(2, 12)}`, name });
      closeModal();
      then();
    });
  }

  // ---- editor: lifecycle --------------------------------------------------

  function editorHTML(id) {
    return `<div class="doc-layout"><section class="panel doc-panel">
        <header class="doc-head"><a class="doc-back" href="#docs" aria-label="Back to docs">‹</a><input id="docTitle" class="doc-title" placeholder="Untitled" maxlength="120" aria-label="Document title"><span id="docStatus" class="doc-status"></span><div id="docPresence" class="doc-presence"></div></header>
        <div class="doc-surface"><div id="docBackdrop" class="doc-backdrop" aria-hidden="true"></div><div id="docCarets" class="doc-carets" aria-hidden="true"></div><textarea id="docInput" class="doc-input" spellcheck="true" readonly aria-label="Document text" placeholder="Loading"></textarea></div>
        <footer class="doc-foot"><span id="docIdentity"></span><span id="docMeta"></span></footer>
      </section>
      <aside class="doc-rail">
        <section class="panel rail-card"><div class="panel-head"><div><p>Agents</p><h2>Ask for a suggestion</h2></div></div>
          <div id="agentOptions" class="agent-options"></div>
          <textarea id="agentInstruction" class="agent-instruction" rows="2"></textarea>
          <button id="agentAsk" class="button primary agent-ask"></button>
          <p class="rail-hint">Uses your selection, or the line your cursor is on. Or type <code>@librarian topic</code> on its own line and press Enter. Agents only suggest: nothing changes until someone accepts.</p>
        </section>
        <section class="panel rail-card"><div class="panel-head"><div><p>Review</p><h2>Suggestions</h2></div><span id="sugCount"></span></div><div id="docSuggestions"></div></section>
        <section class="panel rail-card"><div class="panel-head"><div><p>Provenance</p><h2>Who wrote this</h2></div><button id="authorshipToggle"></button></div><div id="docAuthorship"></div><a class="rail-link" id="docHistory" href="${escapeHTML(state.endpoint)}/api/docs/${escapeHTML(id)}/log">Download the edit history as JSON Lines ↓</a></section>
      </aside></div>`;
  }

  function openDoc(id) {
    closeDoc();
    ui.docId = id;
    ui.endpoint = state.endpoint;
    const base = state.endpoint.replace(/^http/, "ws");
    const client = new CollabClient({
      url: `${base}/api/docs/${encodeURIComponent(id)}/ws`, user: user(), clientId: tabId,
      handlers: { reset: onReset, remote: onRemote, status: renderStatus, presence: () => { renderPresence(); paint(); }, suggestions: onSuggestions, title: renderTitle, error: reason => toast(`Sync problem: ${reason}. Reloading the document.`) }
    });
    ui.client = client;
    // A doc that does not exist would otherwise look like a network that
    // never connects, so ask once over HTTP first.
    request(`/api/docs/${encodeURIComponent(id)}`).then(() => { if (ui.client === client) client.connect(); }, error => {
      if (ui.client !== client) return;
      if (/not found/i.test(error.message)) { ui.missingId = id; closeDoc(); render(); } else client.connect();
    });
    scheduleProvenance(300);
  }

  // retire lets a client that still has unsaved edits finish sending them
  // after its doc has left the screen, instead of dropping them.
  function retire(client) {
    if (!hasPending(client)) return client.close();
    const title = client.doc ? client.doc.title : "the doc";
    const lost = `Some edits to ${title} could not be saved.`;
    let deadline = null;
    const finish = message => {
      if (!ui.retiring.has(client)) return;
      ui.retiring.delete(client);
      clearTimeout(deadline);
      client.close();
      if (message) toast(message);
    };
    client.handlers = { reset: info => { if (info.lost) finish(lost); } };
    ui.retiring.add(client);
    deadline = setTimeout(() => finish(hasPending(client) ? lost : ""), 30000);
    client.whenSynced().then(() => finish(""));
    toast(client.ready ? `Saving your last edits to ${title}.` : `Offline. Still trying to save your last edits to ${title}.`);
  }

  function closeDoc() {
    if (ui.client) { retire(ui.client); ui.stale = true; }
    clearTimeout(ui.provenanceTimer);
    for (const pin of ui.pins) pin.invalid = true;
    ui.pins.clear();
    Object.assign(ui, { client: null, docId: null, root: null, focus: null, provenance: null, provenanceTimer: null, hover: null, busy: false, composing: false, deferred: null, shadow: "" });
  }

  const part = selector => (ui.root ? ui.root.querySelector(selector) : null);
  const input = () => part("#docInput");

  function showText() {
    const field = input(), client = ui.client;
    if (!field || !client || client.rev === null) return;
    field.value = client.text;
    field.readOnly = false;
    field.placeholder = PLACEHOLDER;
    ui.shadow = client.text;
    ui.deferred = null;
  }

  function onReset(info) {
    // Positions held for a queued agent request refer to text that has just
    // been replaced.
    for (const pin of ui.pins) pin.invalid = true;
    ui.pins.clear();
    showText();
    const field = input();
    if (field && info.first) field.setSelectionRange(0, 0);
    if (info.lost) toast("The document was reloaded. Edits that had not reached the server may be missing.");
    paint();
    renderStatus();
  }

  // ---- editor: text in and out --------------------------------------------

  // onRemote puts someone else's edit on screen without moving this
  // person's caret or selection relative to their own text.
  function onRemote(op) {
    for (const pin of ui.pins) [pin.start, pin.end] = OT.mapRange(pin.start, pin.end, op);
    const field = input();
    if (!field) return;
    if (ui.composing || ui.deferred) {
      ui.deferred = ui.deferred ? OT.compose(ui.deferred, op) : op;
      scheduleProvenance();
      return;
    }
    const start = OT.mapPos(field.selectionStart, op, false), end = OT.mapPos(field.selectionEnd, op, false);
    const direction = field.selectionDirection;
    field.value = ui.client.text;
    field.setSelectionRange(start, end, direction);
    ui.shadow = ui.client.text;
    paint();
    reportCursor();
    scheduleProvenance();
  }

  // flushDeferred shows the remote edits that were held back during
  // composition.
  function flushDeferred() {
    ui.composing = false;
    const field = input(), held = ui.deferred;
    if (!field || !held || !ui.client) return;
    ui.deferred = null;
    const start = OT.mapPos(field.selectionStart, held, false), end = OT.mapPos(field.selectionEnd, held, false);
    field.value = ui.client.text;
    field.setSelectionRange(start, end, field.selectionDirection);
    ui.shadow = ui.client.text;
    paint();
    reportCursor();
  }

  function onInput() {
    const field = input(), client = ui.client;
    if (!client || client.rev === null) return;
    const before = ui.deferred ? ui.shadow : client.text;
    let after = field.value;
    if (after === before) return;
    if (after.length > MAX_UNITS && after.length > before.length) {
      const caret = Math.min(field.selectionStart, before.length);
      field.value = before;
      field.setSelectionRange(caret, caret);
      return toast(`That would take the doc past its limit of ${MAX_UNITS.toLocaleString()} characters.`);
    }
    if (!ui.composing && after.isWellFormed && !after.isWellFormed()) {
      // Half of a surrogate pair cannot be stored faithfully. Replace it
      // here so every copy of the doc shows the same thing.
      const caret = field.selectionStart;
      after = after.toWellFormed();
      field.value = after;
      field.setSelectionRange(caret, caret);
    }
    let op = OT.fromDiff(before, after, field.selectionStart);
    if (ui.deferred) {
      // The field has not been shown ui.deferred yet, so this edit was made
      // against older text. Carry each across the other, as the sync client
      // does for edits that cross on the network.
      [op, ui.deferred] = OT.transform(op, ui.deferred);
    }
    ui.shadow = after;
    for (const pin of ui.pins) [pin.start, pin.end] = OT.mapRange(pin.start, pin.end, op);
    client.applyLocal(op);
    paint();
    reportCursor();
    scheduleProvenance();
  }

  function reportCursor() {
    const field = input();
    if (!field || !ui.client || ui.client.rev === null || ui.deferred || document.activeElement !== field) return;
    const backward = field.selectionDirection === "backward";
    ui.client.setCursor(backward ? field.selectionStart : field.selectionEnd, backward ? field.selectionEnd : field.selectionStart);
  }

  function onSuggestions() {
    renderSuggestions();
    paint();
    scheduleProvenance();
  }

  // ---- editor: overlay ----------------------------------------------------

  // paint draws everything that sits on top of the text: other people's
  // carets and selections, agents at work, suggestion ranges, and authorship.
  // The backdrop repeats the text invisibly with identical wrapping, so a
  // highlighted span lands exactly behind the same characters in the textarea.
  function paint() {
    const backdrop = part("#docBackdrop"), client = ui.client;
    if (!backdrop || !client) return;
    if (ui.deferred) {
      // The field is behind the document while composing. Show it plainly
      // rather than draw marks measured against text it does not hold yet.
      backdrop.textContent = input().value + "​";
      part("#docCarets").innerHTML = "";
      return;
    }
    const text = client.text, length = text.length;
    const clamp = position => Math.max(0, Math.min(length, position));
    const ranges = [], carets = [];

    if (ui.authorship && ui.provenance && ui.provenance.doc.rev === client.rev && client.isSynced()) {
      for (const span of ui.provenance.spans) {
        const author = ui.provenance.authors[span.author];
        if (author.kind === "system") continue;
        ranges.push({ start: span.start, end: span.start + span.length, cls: author.kind === "agent" ? "by-agent" : `by-human h${span.author % 6}` });
      }
    }
    for (const suggestion of client.suggestions.values()) {
      const [start, end] = client.localRange(suggestion.start, suggestion.end).map(clamp);
      ranges.push({ start, end, cls: `mark ${suggestion.status === "pending" ? "pending" : "stale"}${ui.hover === suggestion.id ? " active" : ""}` });
      if (suggestion.placement === "after" || start === end) carets.push({ pos: end, cls: `insert-point ${suggestion.status === "pending" ? "pending" : "stale"}`, name: "" });
    }
    for (const peer of client.peers.values()) {
      if (typeof peer.pos !== "number") continue;
      if (peer.kind === "agent") {
        const [start, end] = client.localRange(peer.anchor, peer.pos).map(clamp);
        ranges.push({ start, end, cls: "mark working" });
        carets.push({ pos: end, cls: "agent", name: `${peer.name} is reading` });
        continue;
      }
      const color = Number(peer.color) % 8;
      const pos = clamp(client.toLocal(peer.pos, false)), anchor = clamp(client.toLocal(peer.anchor, false));
      if (pos !== anchor) ranges.push({ start: Math.min(pos, anchor), end: Math.max(pos, anchor), cls: `selection c${color}` });
      carets.push({ pos, cls: `c${color}`, name: peer.name });
    }

    const cuts = new Set([0, length]);
    for (const range of ranges) { cuts.add(range.start); cuts.add(range.end); }
    const points = [...cuts].sort((a, b) => a - b);
    let html = "";
    for (let index = 0; index + 1 < points.length; index++) {
      const at = points[index], next = points[index + 1];
      const classes = ranges.filter(range => range.start < range.end && range.start <= at && range.end >= next).map(range => range.cls);
      const chunk = escapeHTML(text.slice(at, next));
      html += classes.length ? `<span class="${classes.join(" ")}">${chunk}</span>` : chunk;
    }
    // The trailing zero-width space keeps a final empty line from collapsing
    // and gives a caret at the very end something to stand beside.
    backdrop.innerHTML = html + "​";
    placeCarets(backdrop, carets);
  }

  // placeCarets draws carets in their own layer, positioned from where the
  // browser actually laid out each character. Putting caret elements inside
  // the text would split kerning pairs and nudge every later character away
  // from its twin in the textarea.
  function placeCarets(backdrop, carets) {
    const layer = part("#docCarets");
    if (!carets.length) { layer.innerHTML = ""; return; }
    const nodes = [];
    const walker = document.createTreeWalker(backdrop, NodeFilter.SHOW_TEXT);
    for (let offset = 0, node = walker.nextNode(); node; node = walker.nextNode()) { nodes.push({ node, offset }); offset += node.length; }
    const origin = backdrop.parentElement.getBoundingClientRect();
    const range = document.createRange();
    let html = "";
    for (const caret of carets) {
      // The character at the caret's position always exists, because of the
      // trailing zero-width space. Its left edge is where the caret goes.
      let entry = nodes[0];
      for (const candidate of nodes) { if (candidate.offset > caret.pos) break; entry = candidate; }
      if (!entry) continue;
      const within = Math.min(caret.pos - entry.offset, entry.node.length - 1);
      range.setStart(entry.node, within);
      range.setEnd(entry.node, within + 1);
      const box = range.getClientRects()[0];
      if (!box) continue;
      html += `<i class="caret ${caret.cls}" data-name="${escapeHTML(caret.name)}" style="left:${(box.left - origin.left).toFixed(2)}px;top:${(box.top - origin.top).toFixed(2)}px;height:${box.height.toFixed(2)}px"></i>`;
    }
    layer.innerHTML = html;
  }

  // ---- editor: chrome -----------------------------------------------------

  function renderStatus() {
    const element = part("#docStatus"), client = ui.client;
    if (!element || !client) return;
    const pending = hasPending(client);
    let label = "Saved", kind = "ok";
    if (client.rev === null) [label, kind] = ["Connecting", "wait"];
    else if (!client.ready) [label, kind] = [pending ? "Offline, edits kept here" : "Offline, reconnecting", "off"];
    else if (pending) [label, kind] = ["Saving", "wait"];
    element.className = `doc-status ${kind}`;
    element.innerHTML = `<i></i>${label}`;
    const meta = part("#docMeta");
    if (client.rev !== null) meta.textContent = `Revision ${client.rev} · ${client.text.length.toLocaleString()} characters`;
    const identity = part("#docIdentity"), me = user();
    if (me && !identity.firstChild) {
      identity.innerHTML = `Editing as <strong>${escapeHTML(me.name)}</strong> · <button class="link-button">change</button>`;
      identity.querySelector("button").addEventListener("click", () => identityModal(() => { const id = ui.docId; closeDoc(); openDoc(id); render(); }));
    }
  }

  function renderPresence() {
    const element = part("#docPresence"), client = ui.client, me = user();
    if (!element || !client || !me) return;
    const mine = `<span class="avatar-chip c${Number(client.color) % 8}" title="${escapeHTML(me.name)} (you)">${escapeHTML(initials(me.name))}</span>`;
    const others = [...client.peers.values()].map(peer => peer.kind === "agent"
      ? `<span class="avatar-chip agent" title="${escapeHTML(peer.name)}, asked by ${escapeHTML(peer.invoked_by || "someone")}">✦</span>`
      : `<span class="avatar-chip c${Number(peer.color) % 8}" title="${escapeHTML(peer.name)}">${escapeHTML(initials(peer.name))}</span>`).join("");
    element.innerHTML = (client.ready ? mine : "") + others;
  }

  function renderTitle() {
    const field = part("#docTitle"), client = ui.client;
    if (field && client && client.doc && document.activeElement !== field) field.value = client.doc.title;
  }

  function instructionHint() {
    const agent = ui.agents.find(candidate => candidate.id === ui.agent);
    return agent && agent.kind === "llm" ? "What should change? For example: tighten this to two sentences" : "Optional: a topic to look up instead of the selection";
  }

  function renderAgents() {
    const element = part("#agentOptions");
    if (!element) return;
    element.innerHTML = ui.agents.map(agent => `<button class="agent-option${agent.id === ui.agent ? " active" : ""}" data-agent="${escapeHTML(agent.id)}"><strong>${escapeHTML(agent.name)}</strong><small>${escapeHTML(agent.model || (agent.kind === "retrieval" ? "Atlas search, no model" : agent.kind))}</small></button>`).join("");
    element.querySelectorAll("[data-agent]").forEach(button => button.addEventListener("click", () => { ui.agent = button.dataset.agent; renderAgents(); }));
    part("#agentInstruction").placeholder = instructionHint();
    renderAsk();
  }

  function renderAsk() {
    const button = part("#agentAsk");
    if (!button) return;
    const agent = ui.agents.find(candidate => candidate.id === ui.agent);
    button.disabled = ui.busy || !agent;
    button.textContent = ui.busy ? "Working" : agent ? `Ask ${agent.name}` : "No agents available";
  }

  // ---- suggestions and agents ---------------------------------------------

  function suggestionHTML(suggestion) {
    const pending = suggestion.status === "pending";
    const where = suggestion.placement === "replace" && suggestion.original
      ? `<del>${escapeHTML(clip(suggestion.original))}</del>`
      : `<small class="sug-where">Insert after “${escapeHTML(clip(suggestion.original.trim(), 60))}”</small>`;
    const actions = pending
      ? `<button class="button primary" data-resolve="accept">Accept</button><button class="button ghost" data-resolve="reject">Reject</button>`
      : `<button class="button secondary" data-resolve="again">Ask again</button><button class="button ghost" data-resolve="dismiss">Dismiss</button>`;
    return `<article class="sug-card ${pending ? "pending" : "stale"}" data-sug="${escapeHTML(suggestion.id)}">
      <div class="sug-head"><span class="agent-chip">✦ ${escapeHTML(suggestion.agent.name)}</span><small>asked by ${escapeHTML(suggestion.invoked_by.name)}${suggestion.attempts > 1 ? " · read twice" : ""}</small></div>
      ${suggestion.instruction ? `<p class="sug-instruction">${escapeHTML(clip(suggestion.instruction, 140))}</p>` : ""}
      ${where}<ins>${escapeHTML(clip(suggestion.text.trim(), 700))}</ins>
      ${pending ? "" : `<p class="sug-stale">The text this was written for has changed since the agent read it.</p>`}
      <div class="sug-actions">${actions}</div></article>`;
  }

  function renderSuggestions() {
    const element = part("#docSuggestions"), client = ui.client;
    if (!element || !client) return;
    const list = [...client.suggestions.values()].sort((a, b) => a.created_at.localeCompare(b.created_at));
    part("#sugCount").textContent = list.length ? `${list.length} open` : "";
    element.innerHTML = list.map(suggestionHTML).join("") || `<div class="empty rail-empty">Nothing waiting for review</div>`;
    element.querySelectorAll("[data-sug]").forEach(card => {
      const id = card.dataset.sug;
      card.addEventListener("mouseenter", () => { ui.hover = id; paint(); });
      card.addEventListener("mouseleave", () => { ui.hover = null; paint(); });
      card.querySelectorAll("[data-resolve]").forEach(button => button.addEventListener("click", () => resolveSuggestion(id, button.dataset.resolve)));
    });
  }

  function resolveSuggestion(id, action) {
    const client = ui.client, suggestion = client.suggestions.get(id);
    if (!suggestion) return;
    if (!client.ready) return toast("You are offline. Suggestions can be reviewed once the connection is back.");
    if (action !== "again") return client.resolve(id, action);
    // Ask again: send the agent back to wherever that text is now, and only
    // retire the outdated suggestion once the new request is under way.
    const [start, end] = client.localRange(suggestion.start, suggestion.end);
    if (invoke(suggestion.agent.id, start, end, suggestion.instruction || "", !!suggestion.command)) client.resolve(id, "dismiss");
  }

  // invoke sends an agent to a range of the text and reports whether the
  // request was started. The range is given in local coordinates and pinned,
  // so it follows any edits that land while waiting for pending changes to
  // reach the server.
  function invoke(agentId, start, end, instruction, command) {
    const client = ui.client;
    if (!client) return false;
    if (ui.busy) { toast("An agent is already working on your last request."); return false; }
    if (!client.ready) { toast("You are offline. Agents can be asked once the connection is back."); return false; }
    const agent = ui.agents.find(candidate => candidate.id === agentId);
    const pin = { start, end, invalid: false };
    ui.pins.add(pin);
    ui.busy = true;
    renderAsk();
    (async () => {
      try {
        await client.whenSynced();
        if (ui.client !== client) return;
        if (pin.invalid) return toast("The document was reloaded before the request went out. Select the text and ask again.");
        const result = await request(`/api/docs/${encodeURIComponent(ui.docId)}/invoke`, {
          method: "POST",
          body: JSON.stringify({ agent: agentId, rev: client.rev, start: pin.start, end: pin.end, instruction, command, user: user() })
        });
        if (ui.client !== client) return;
        if (!result.suggestion) toast(result.note || `${agent ? agent.name : "The agent"} had nothing to suggest.`);
        else if (result.suggestion.status === "stale") toast("The text kept changing while the agent worked, so its suggestion is marked outdated.");
      } catch (error) {
        if (ui.client === client) toast(error.message);
      } finally {
        ui.pins.delete(pin);
        if (ui.client === client) { ui.busy = false; renderAsk(); }
      }
    })();
    return true;
  }

  function lineAt(text, position) {
    const start = text.lastIndexOf("\n", position - 1) + 1;
    const found = text.indexOf("\n", position);
    return [start, found < 0 ? text.length : found];
  }

  function askFromBar() {
    const field = input(), box = part("#agentInstruction");
    if (ui.deferred) return;
    let start = field.selectionStart, end = field.selectionEnd;
    if (start === end) [start, end] = lineAt(field.value, start);
    if (invoke(ui.agent, start, end, box.value.trim(), false)) box.value = "";
  }

  // A line that reads "@agent some request" summons that agent when Enter is
  // pressed at its end. The agent's answer is offered as a replacement for
  // the line, so the request never has to be cleaned up by hand. Enter is
  // only taken over when a request will really go out; otherwise it stays an
  // ordinary new line.
  function onKeydown(event) {
    if (event.key !== "Enter" || event.shiftKey || event.metaKey || event.ctrlKey || event.altKey || event.isComposing || event.keyCode === 229) return;
    const field = input(), client = ui.client;
    if (!client || ui.busy || !client.ready || ui.composing || ui.deferred || field.selectionStart !== field.selectionEnd) return;
    const [start, end] = lineAt(field.value, field.selectionStart);
    if (field.selectionStart !== end) return;
    const match = /^@([a-z]+)\s+(\S.*)$/i.exec(field.value.slice(start, end));
    if (!match || !ui.agents.some(agent => agent.id === match[1].toLowerCase())) return;
    // A line that already has a suggestion waiting was already asked.
    for (const suggestion of client.suggestions.values()) {
      const [from, to] = client.localRange(suggestion.start, suggestion.end);
      if (from === start && to === end) return;
    }
    event.preventDefault();
    invoke(match[1].toLowerCase(), start, end, match[2].trim(), true);
  }

  // ---- provenance ---------------------------------------------------------

  // Authorship is refreshed shortly after a change. A refresh that is
  // already due is left alone, so steady typing still updates it about once
  // a second instead of postponing it forever.
  function scheduleProvenance(delay = 1200) {
    if (ui.provenanceTimer) return;
    ui.provenanceTimer = setTimeout(() => { ui.provenanceTimer = null; loadProvenance(); }, delay);
  }

  async function loadProvenance() {
    const id = ui.docId;
    if (!id || !state.connected) return;
    try {
      const provenance = await request(`/api/docs/${encodeURIComponent(id)}/provenance`);
      if (ui.docId !== id) return;
      ui.provenance = provenance;
      renderAuthorship();
      paint();
    } catch {}
  }

  function renderAuthorship() {
    const element = part("#docAuthorship"), toggle = part("#authorshipToggle");
    if (!element) return;
    toggle.textContent = ui.authorship ? "Hide in text" : "Show in text";
    const provenance = ui.provenance;
    if (!provenance || !provenance.units) { element.innerHTML = `<div class="empty rail-empty">${provenance ? "Nothing written yet" : "Loading"}</div>`; return; }
    const share = kind => Math.round(((provenance.by_kind[kind] || 0) / provenance.units) * 1000) / 10;
    const kinds = [["human", "People"], ["agent", "Agents"], ["system", "Template"]].filter(([kind]) => provenance.by_kind[kind]);
    const bar = kinds.map(([kind]) => `<i class="${kind}" style="width:${share(kind)}%"></i>`).join("");
    const legend = kinds.map(([kind, label]) => `<span><i class="${kind}"></i>${label} ${share(kind)}%</span>`).join("");
    // The swatch matches the underline that Show in text draws for that author.
    const swatch = author => { const index = provenance.authors.findIndex(other => other.id === author.id && other.name === author.name && other.kind === author.kind); return author.kind === "agent" ? "agent" : `h${index % 6}`; };
    const people = provenance.shares.filter(item => item.author.kind !== "system").slice(0, 6).map(item => `<div class="share-row"><span><i class="swatch ${swatch(item.author)}"></i>${item.author.kind === "agent" ? "✦ " : ""}${escapeHTML(item.author.name)}</span><em>${item.units.toLocaleString()} characters</em></div>`).join("");
    const agents = provenance.agents.map(item => `<div class="share-row agent"><span>${escapeHTML(item.agent.name)} suggestions</span><em>${item.accepted} of ${item.proposed} accepted</em></div>`).join("");
    element.innerHTML = `<div class="share-bar">${bar}</div><div class="share-legend">${legend}</div>${people}${agents}`;
  }

  // ---- wiring -------------------------------------------------------------

  function view() {
    if (!state.connected) return disconnectedHTML();
    const id = routeId();
    if (!id) return listHTML();
    if (!user()) return nameGateHTML();
    if (ui.missingId === id) return missingHTML();
    return editorHTML(id);
  }

  // capture runs just before the console rebuilds the page, and remembers
  // where the keyboard was so bind can put it back.
  function capture() {
    const active = document.activeElement;
    ui.focus = active && ui.root && ui.root.contains(active) && active.id
      ? { id: active.id, start: active.selectionStart, end: active.selectionEnd, direction: active.selectionDirection }
      : null;
  }

  function restoreFocus() {
    const saved = ui.focus;
    ui.focus = null;
    const element = saved && ui.root.querySelector(`#${saved.id}`);
    if (!element) return;
    element.focus({ preventScroll: true });
    try { element.setSelectionRange(saved.start, saved.end, saved.direction); } catch {}
  }

  function wireEditor() {
    const field = input();
    field.addEventListener("input", onInput);
    field.addEventListener("keydown", onKeydown);
    field.addEventListener("compositionstart", () => { ui.composing = true; ui.shadow = field.value; });
    // Some browsers deliver the final input event after compositionend, so
    // wait for this turn of the event loop to finish before catching up.
    field.addEventListener("compositionend", () => setTimeout(flushDeferred, 0));
    for (const name of ["keyup", "mouseup", "focus", "select"]) field.addEventListener(name, reportCursor);
    const title = part("#docTitle");
    title.addEventListener("change", () => {
      const client = ui.client, wanted = title.value.trim();
      if (wanted && client && client.ready) return client.setTitle(wanted);
      if (wanted && client && !client.ready) toast("You are offline. The title can be changed once the connection is back.");
      title.blur();
      renderTitle();
    });
    title.addEventListener("keydown", event => { if (event.key === "Enter") title.blur(); });
    part("#agentAsk").addEventListener("click", askFromBar);
    part("#authorshipToggle").addEventListener("click", () => { ui.authorship = !ui.authorship; renderAuthorship(); paint(); if (ui.authorship) loadProvenance(); });
  }

  function bind() {
    document.querySelectorAll("[data-action='docs-connect']").forEach(button => button.addEventListener("click", connectionModal));
    document.querySelectorAll("[data-action='new-doc']").forEach(button => button.addEventListener("click", () => (user() ? newDocModal() : identityModal(newDocModal))));
    document.querySelectorAll("[data-action='docs-name']").forEach(button => button.addEventListener("click", () => identityModal(render)));
    if (!state.connected) return closeDoc();
    const id = routeId();
    // Coming back from a doc, the list is out of date by at least that visit.
    if (!id && ui.client) closeDoc();
    ensureDocs();
    clearTimeout(ui.listTimer);
    if (!id) {
      ui.namePrompted = false;
      ui.missingId = null;
      // Keep "who is editing" on the list reasonably current while it is open.
      ui.listTimer = setTimeout(() => { if (state.view === "docs" && !routeId()) { ui.stale = true; ensureDocs(); } }, 8000);
      return;
    }
    if (!user()) {
      // Ask once. If the prompt is dismissed, the page keeps offering it.
      if (!ui.namePrompted) { ui.namePrompted = true; identityModal(render); }
      return;
    }
    if (ui.missingId === id) return;
    if (ui.docId !== id || ui.endpoint !== state.endpoint) openDoc(id);

    const fresh = $(".doc-layout");
    if (ui.root && ui.root !== fresh) {
      // Same doc, new render of the console: put the live editor back in
      // place of the blank one that was just generated.
      fresh.replaceWith(ui.root);
      renderAgents();
      restoreFocus();
      paint();
      return;
    }
    ui.root = fresh;
    wireEditor();
    showText();
    renderTitle(); renderStatus(); renderPresence(); renderSuggestions(); renderAuthorship(); renderAgents(); paint();
  }

  function leave() {
    clearTimeout(ui.listTimer);
    closeDoc();
    ui.stale = true;
    ui.namePrompted = false;
  }

  window.addEventListener("resize", () => { if (ui.client && ui.root) paint(); });
  window.addEventListener("beforeunload", event => {
    if (hasPending(ui.client) || [...ui.retiring].some(hasPending)) { event.preventDefault(); event.returnValue = ""; }
  });

  return { view, bind, leave, capture };
})();
