// The sync client for a live document. It has no DOM dependencies, so the
// same file runs in the browser and under Node for end-to-end tests.
//
// The client keeps at most one operation in flight (outstanding). Edits made
// while waiting are composed into a single buffer. Every operation that
// arrives from the server is transformed across both before it touches the
// local text, and both are transformed across it, so the next thing sent is
// always expressed against the server's latest revision.
(function (root) {
  "use strict";
  const OT = typeof module !== "undefined" && module.exports ? require("./ot.js") : root.AtlasOT;
  const OPEN = 1;

  class CollabClient {
    // options: url, user {id, name}, clientId, handlers, WebSocket (optional).
    constructor(options) {
      this.url = options.url;
      this.user = options.user;
      // Operation ids are deduplicated per client id on the server, so the
      // id must be unique to this object. A page that opens the same
      // document twice would otherwise reuse ids, and the server would
      // quietly drop the second session's edits as resends of the first.
      this.clientId = `${options.clientId}.${Math.random().toString(36).slice(2, 12)}`;
      this.handlers = options.handlers || {};
      this.WebSocket = options.WebSocket || globalThis.WebSocket;
      this.backoff = options.backoff || (attempt => Math.min(8000, 300 * 2 ** attempt) * (0.5 + Math.random() / 2));
      // A connection can die without the browser ever saying so: a laptop
      // sleeps, a network changes, a proxy swallows the handshake. These
      // limits bound how long each kind of silence is tolerated before the
      // client gives up on the socket and reconnects.
      this.timeouts = { connect: 10000, ack: 15000, ping: 20000, silence: 45000, ...(options.timeouts || {}) };

      this.text = "";
      this.rev = null;        // last server revision folded into text
      this.epoch = "";
      this.doc = null;
      this.outstanding = null; // {id, op}: sent, or to be sent on connect
      this.buffer = null;      // edits made since outstanding was sent
      this.seq = 0;

      this.socket = null;
      this.ready = false;      // init received on the current socket
      this.closed = false;
      this.resetNext = false;  // ask for the full text on the next connect
      this.attempt = 0;
      this.timer = null;
      this.watchdog = null;
      this.connectedAt = 0;
      this.heardAt = 0;
      this.sentAt = 0;
      this.pingedAt = 0;
      this.checkedAt = 0;
      this.graceUntil = 0;

      this.connId = null;
      this.color = 0;
      // Peers and suggestions are held in server coordinates, meaning
      // positions in the document at this.rev. Use toLocal and localRange
      // to place them in the text the user is looking at.
      this.peers = new Map();
      this.suggestions = new Map();
      this.cursor = null;       // where the caret is, in local coordinates
      this.serverCursor = null; // where the server believes it is
      this.waiters = [];
    }

    // A handler that throws is a display problem. It must not be mistaken
    // for a sync failure, and it must not stop a reconnect being scheduled.
    emit(name, ...args) {
      const handler = this.handlers[name];
      if (!handler) return;
      try { handler(...args); }
      catch (error) { if (typeof console !== "undefined") console.error(`collab ${name} handler failed`, error); }
    }

    connect() {
      if (this.closed) return;
      clearTimeout(this.timer);
      // Calling connect twice must not leave the first socket joined.
      const previous = this.socket;
      this.socket = null;
      if (previous) { try { previous.close(); } catch {} }
      this.ready = false;
      this.connectedAt = this.checkedAt = Date.now();
      if (!this.watchdog) this.watchdog = setInterval(() => this.checkHealth(), 1000);
      const socket = new this.WebSocket(this.url);
      this.socket = socket;
      socket.onopen = () => {
        if (socket !== this.socket) return;
        const hello = { t: "hello", client: this.clientId, user: this.user };
        // Saying which revision we hold lets the server send only what we
        // missed, which is what makes offline edits survive a reconnect.
        if (this.rev !== null && !this.resetNext) { hello.rev = this.rev; hello.epoch = this.epoch; }
        socket.send(JSON.stringify(hello));
      };
      socket.onmessage = event => {
        if (socket !== this.socket) return;
        try { this.receive(JSON.parse(event.data)); }
        catch (error) { this.desync(error.message); }
      };
      // An error always means the socket is finished. Some runtimes report
      // a failed handshake with an error alone and no close, so either event
      // counts as the drop; abandon ignores whichever comes second.
      socket.onclose = event => {
        if (socket === this.socket && event && event.code === 1009) {
          // The server refused a message for its size. Sending the same
          // edit again would be refused again, forever, so give it up.
          this.resetNext = true;
          this.emit("error", "an edit was too large to send");
        }
        this.abandon(socket);
      };
      socket.onerror = () => this.abandon(socket);
      this.emit("status");
    }

    // abandon gives up on a socket and schedules a reconnect.
    abandon(socket) {
      if (socket !== this.socket) return;
      this.socket = null;
      try { socket.close(); } catch {}
      this.dropped();
    }

    checkHealth() {
      const socket = this.socket;
      if (!socket) return;
      const now = Date.now();
      // Browsers slow timers in background tabs to as little as once a
      // minute. After a gap like that the silence was ours, not the
      // server's, so ask for a sign of life and allow time for the answer
      // instead of judging the connection by a clock that was not running.
      const late = now - this.checkedAt > 5000;
      this.checkedAt = now;
      if (late && this.ready) {
        this.graceUntil = now + 10000;
        this.pingedAt = now;
        this.send({ t: "ping" });
      }
      if (now < this.graceUntil) return;
      // Anything received counts as progress, so a slow link that is still
      // delivering a large document is not cut off halfway.
      if (!this.ready) {
        if (now - Math.max(this.connectedAt, this.heardAt) > this.timeouts.connect) this.abandon(socket);
        return;
      }
      if (this.outstanding && now - Math.max(this.sentAt, this.heardAt) > this.timeouts.ack) return this.abandon(socket);
      if (now - this.heardAt > this.timeouts.silence) return this.abandon(socket);
      if (now - this.heardAt > this.timeouts.ping && now - this.pingedAt > this.timeouts.ping) {
        this.pingedAt = now;
        this.send({ t: "ping" });
      }
    }

    dropped() {
      this.socket = null;
      this.ready = false;
      this.peers.clear();
      this.emit("presence");
      this.emit("status");
      if (this.closed) return;
      this.timer = setTimeout(() => this.connect(), this.backoff(this.attempt++));
    }

    close() {
      this.closed = true;
      clearTimeout(this.timer);
      clearInterval(this.watchdog);
      this.watchdog = null;
      const socket = this.socket;
      this.socket = null;
      this.ready = false;
      if (socket) socket.close();
    }

    send(message) {
      if (this.socket && this.socket.readyState === OPEN) this.socket.send(JSON.stringify(message));
    }

    nextId() { return `${this.clientId}:${++this.seq}`; }

    sendOutstanding() {
      this.sentAt = Date.now();
      this.send({ t: "op", rev: this.rev, id: this.outstanding.id, op: this.outstanding.op });
    }

    isSynced() { return this.ready && !this.outstanding && !this.buffer; }

    // whenSynced resolves once the server has acknowledged every local edit.
    whenSynced() {
      if (this.isSynced()) return Promise.resolve();
      return new Promise(resolve => this.waiters.push(resolve));
    }

    settled() {
      if (!this.isSynced()) return;
      this.flushCursor();
      const waiters = this.waiters;
      this.waiters = [];
      for (const resolve of waiters) resolve();
    }

    // applyLocal records an edit the user just made. The operation must be
    // expressed against this.text.
    applyLocal(op) {
      if (this.rev === null) throw new Error("document is not loaded yet");
      if (OT.isNoop(op)) return;
      this.text = OT.apply(this.text, op);
      if (!this.outstanding) {
        this.outstanding = { id: this.nextId(), op };
        if (this.ready) this.sendOutstanding();
      } else {
        this.buffer = this.buffer ? OT.compose(this.buffer, op) : op;
      }
      this.emit("status");
    }

    // setCursor takes the caret and selection anchor in local coordinates.
    // They can only be reported while nothing is pending, because that is
    // when local positions and server positions are the same thing. While
    // typing, peers follow the caret from the operations themselves.
    setCursor(pos, anchor) {
      this.cursor = { pos, anchor };
      this.flushCursor();
    }

    flushCursor() {
      if (!this.cursor || !this.isSynced()) return;
      const known = this.serverCursor;
      if (known && known.pos === this.cursor.pos && known.anchor === this.cursor.anchor) return;
      this.serverCursor = { ...this.cursor };
      this.send({ t: "cursor", rev: this.rev, pos: this.cursor.pos, anchor: this.cursor.anchor });
    }

    resolve(sid, action) { this.send({ t: "resolve", sid, action }); }
    setTitle(title) { this.send({ t: "title", title }); }

    // toLocal maps a server position into the local text by carrying it
    // across the edits the server has not seen yet.
    toLocal(pos, afterInsert) {
      if (this.outstanding) pos = OT.mapPos(pos, this.outstanding.op, !!afterInsert);
      if (this.buffer) pos = OT.mapPos(pos, this.buffer, !!afterInsert);
      return pos;
    }

    localRange(start, end) {
      if (this.outstanding) [start, end] = OT.mapRange(start, end, this.outstanding.op);
      if (this.buffer) [start, end] = OT.mapRange(start, end, this.buffer);
      return [start, end];
    }

    // advance moves everything held in server coordinates across one
    // operation in its server form, the same way the server moves it. conn
    // is the connection that made the operation; own marks it as ours.
    advance(op, conn, own) {
      for (const [id, peer] of this.peers) {
        if (typeof peer.pos !== "number") continue;
        if (peer.kind === "agent") [peer.anchor, peer.pos] = OT.mapRange(peer.anchor, peer.pos, op);
        else if (id !== conn) { peer.pos = OT.mapPos(peer.pos, op, false); peer.anchor = OT.mapPos(peer.anchor, op, false); }
      }
      const position = OT.editPos(op);
      const author = conn && this.peers.get(conn);
      if (author && position >= 0) { author.pos = position; author.anchor = position; }
      // The server places an author's caret at their edit and carries
      // everyone else's across it. Track what it believes about ours so the
      // caret is only reported when that belief is wrong.
      if (own) { if (position >= 0) this.serverCursor = { pos: position, anchor: position }; }
      else if (this.serverCursor) {
        this.serverCursor = { pos: OT.mapPos(this.serverCursor.pos, op, false), anchor: OT.mapPos(this.serverCursor.anchor, op, false) };
      }
      for (const suggestion of this.suggestions.values()) {
        [suggestion.start, suggestion.end] = OT.mapRange(suggestion.start, suggestion.end, op);
      }
    }

    desync(reason) {
      // Local state no longer lines up with the server. Reload the document
      // rather than risk sending edits against the wrong text.
      this.resetNext = true;
      this.emit("error", reason);
      if (this.socket) this.abandon(this.socket);
    }

    // acknowledged records that the server committed outstanding. echo is
    // set when that was learned from the operation coming back rather than
    // from an acknowledgement.
    acknowledged(rev, echo) {
      if (rev !== this.rev + 1) return this.desync("acknowledgement arrived out of order");
      // After being transformed across everything that was committed ahead
      // of it, outstanding is exactly the operation the server applied.
      this.advance(this.outstanding.op, null, true);
      // An echo means the operation arrived on a connection that is gone,
      // and that is where the server put the caret. This connection has to
      // report its own.
      if (echo) this.serverCursor = null;
      this.rev = rev;
      this.outstanding = null;
      this.attempt = 0;
      if (this.buffer) {
        this.outstanding = { id: this.nextId(), op: this.buffer };
        this.buffer = null;
        this.sendOutstanding();
      }
      this.settled();
      this.emit("status");
    }

    serverOp(message) {
      if (this.outstanding && message.id && message.id === this.outstanding.id) {
        // Our own operation coming back. This is how a commit is learned
        // when the acknowledgement was lost with the old connection.
        return this.acknowledged(message.rev, true);
      }
      if (message.rev !== this.rev + 1) return this.desync("operation arrived out of order");
      this.advance(message.op, message.c, false);
      let op = message.op;
      if (this.outstanding) [this.outstanding.op, op] = OT.transform(this.outstanding.op, op);
      if (this.buffer) [this.buffer, op] = OT.transform(this.buffer, op);
      this.text = OT.apply(this.text, op);
      this.rev = message.rev;
      // Keep the remembered caret on the same character, so that what gets
      // reported later is right even if the editor was not focused to
      // report the move itself.
      if (this.cursor) this.cursor = { pos: OT.mapPos(this.cursor.pos, op, false), anchor: OT.mapPos(this.cursor.anchor, op, false) };
      this.emit("remote", op, message.a);
    }

    setPeers(list) {
      this.peers = new Map();
      for (const peer of list || []) {
        if (peer.c !== this.connId) this.peers.set(peer.c, { ...peer });
      }
    }

    receive(message) {
      this.heardAt = Date.now();
      // Once sync is lost, ignore everything until the reload arrives.
      if (this.resetNext && message.t !== "init") return;
      switch (message.t) {
        case "init": {
          // Backoff restarts only once nothing is waiting to be resent. If
          // the server keeps dropping the connection over a pending edit,
          // the retries must keep slowing down.
          if (!this.outstanding) this.attempt = 0;
          this.connId = message.you;
          this.color = message.color;
          this.doc = message.doc;
          this.epoch = message.epoch;
          if (typeof message.text === "string") {
            const first = this.rev === null;
            const lost = !first && !!(this.outstanding || this.buffer);
            this.text = message.text;
            this.rev = message.rev;
            this.outstanding = null;
            this.buffer = null;
            this.resetNext = false;
            this.cursor = null;
            this.attempt = 0;
            this.emit("reset", { first, lost });
          } else {
            for (const op of message.ops || []) {
              this.serverOp(op);
              if (this.resetNext) return; // lost sync in the middle of catching up
            }
            if (this.rev !== message.rev) return this.desync("catch-up ended at the wrong revision");
          }
          this.setPeers(message.clients);
          this.suggestions = new Map((message.suggestions || []).map(s => [s.id, s]));
          this.ready = true;
          this.serverCursor = null; // a new connection starts without a caret
          if (this.outstanding) this.sendOutstanding();
          this.settled();
          this.emit("presence");
          this.emit("suggestions");
          this.emit("title");
          this.emit("status");
          break;
        }
        case "op":
          this.serverOp(message);
          break;
        case "ack":
          if (this.outstanding && message.id === this.outstanding.id) this.acknowledged(message.rev);
          break;
        case "presence":
          this.setPeers(message.clients);
          this.emit("presence");
          break;
        case "cursor": {
          const peer = this.peers.get(message.c);
          if (peer) { peer.pos = message.pos; peer.anchor = message.anchor; this.emit("presence"); }
          break;
        }
        case "sug": {
          const suggestion = message.s;
          if (suggestion.status === "pending" || suggestion.status === "stale") this.suggestions.set(suggestion.id, suggestion);
          else this.suggestions.delete(suggestion.id);
          this.emit("suggestions", suggestion);
          break;
        }
        case "title":
          if (this.doc) this.doc.title = message.title;
          this.emit("title");
          break;
        case "error":
          this.resetNext = true;
          this.emit("error", message.message);
          break;
      }
    }
  }

  if (typeof module !== "undefined" && module.exports) module.exports = { CollabClient };
  else root.AtlasCollab = { CollabClient };
})(typeof self !== "undefined" ? self : this);
