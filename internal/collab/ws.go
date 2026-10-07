package collab

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A small RFC 6455 server, enough for JSON text messages: handshake, masked
// client frames, fragmentation, ping/pong, and close. It exists so Atlas keeps
// building with only the standard library.

const (
	wsGUID         = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	wsReadTimeout  = 75 * time.Second
	wsWriteTimeout = 10 * time.Second
	wsPingInterval = 25 * time.Second

	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA

	closeNormal     = 1000
	closeProtocol   = 1002
	closeUnsupport  = 1003
	closeTooLarge   = 1009
	maxControlBytes = 125
)

var errWSProtocol = errors.New("websocket protocol error")

type wsConn struct {
	conn      net.Conn
	reader    *bufio.Reader
	writeMu   sync.Mutex
	closeSent bool // guarded by writeMu
	once      sync.Once
}

func wsAcceptKey(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// upgradeWebSocket completes the opening handshake and takes over the
// connection. On failure it has already written the HTTP error.
func upgradeWebSocket(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if r.Method != http.MethodGet || !headerHasToken(r.Header, "Connection", "upgrade") || !headerHasToken(r.Header, "Upgrade", "websocket") {
		http.Error(w, "websocket upgrade required", http.StatusBadRequest)
		return nil, errWSProtocol
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "unsupported websocket version", http.StatusUpgradeRequired)
		return nil, errWSProtocol
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if decoded, err := base64.StdEncoding.DecodeString(key); err != nil || len(decoded) != 16 {
		http.Error(w, "invalid Sec-WebSocket-Key", http.StatusBadRequest)
		return nil, errWSProtocol
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connection cannot be upgraded", http.StatusInternalServerError)
		return nil, errWSProtocol
	}
	conn, buffered, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	response := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n\r\n"
	_ = conn.SetDeadline(time.Now().Add(wsWriteTimeout))
	if _, err := buffered.WriteString(response); err != nil {
		conn.Close()
		return nil, err
	}
	if err := buffered.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return &wsConn{conn: conn, reader: buffered.Reader}, nil
}

// ReadText returns the next complete text message, answering pings and
// reassembling fragments along the way. It returns io.EOF after a clean close.
func (c *wsConn) ReadText(limit int) ([]byte, error) {
	var message []byte
	inMessage := false
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		var header [2]byte
		if _, err := io.ReadFull(c.reader, header[:]); err != nil {
			return nil, err
		}
		final := header[0]&0x80 != 0
		opcode := header[0] & 0x0f
		masked := header[1]&0x80 != 0
		length := uint64(header[1] & 0x7f)
		if header[0]&0x70 != 0 || !masked {
			// No extension was negotiated, and clients must mask every frame.
			return nil, c.failWith(closeProtocol)
		}
		switch length {
		case 126:
			var extended [2]byte
			if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
				return nil, err
			}
			length = uint64(binary.BigEndian.Uint16(extended[:]))
		case 127:
			var extended [8]byte
			if _, err := io.ReadFull(c.reader, extended[:]); err != nil {
				return nil, err
			}
			length = binary.BigEndian.Uint64(extended[:])
		}
		control := opcode >= opClose
		if control && (length > maxControlBytes || !final) {
			return nil, c.failWith(closeProtocol)
		}
		// Compare without adding, so a length near 2^64 cannot wrap around
		// the limit and reach the allocation below.
		if !control && (length > uint64(limit) || uint64(len(message)) > uint64(limit)-length) {
			return nil, c.failWith(closeTooLarge)
		}
		var mask [4]byte
		if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
			return nil, err
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, err
		}
		for index := range payload {
			payload[index] ^= mask[index%4]
		}

		switch opcode {
		case opText:
			if inMessage {
				return nil, c.failWith(closeProtocol)
			}
			message, inMessage = payload, true
		case opContinuation:
			if !inMessage {
				return nil, c.failWith(closeProtocol)
			}
			message = append(message, payload...)
		case opBinary:
			return nil, c.failWith(closeUnsupport)
		case opClose:
			_ = c.writeFrame(opClose, payload)
			return nil, io.EOF
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		default:
			return nil, c.failWith(closeProtocol)
		}
		if final {
			return message, nil
		}
	}
}

func (c *wsConn) failWith(code uint16) error {
	var status [2]byte
	binary.BigEndian.PutUint16(status[:], code)
	_ = c.writeFrame(opClose, status[:])
	return fmt.Errorf("%w: closed with status %d", errWSProtocol, code)
}

// writeFrame sends one unfragmented frame. Server frames are never masked.
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	frame := make([]byte, 0, len(payload)+10)
	frame = append(frame, 0x80|opcode)
	switch {
	case len(payload) <= 125:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= 0xffff:
		frame = append(frame, 126)
		frame = binary.BigEndian.AppendUint16(frame, uint16(len(payload)))
	default:
		frame = append(frame, 127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	frame = append(frame, payload...)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// A close frame is the last thing an endpoint may send, and it may send
	// only one. Anything after it would be a protocol violation.
	if c.closeSent {
		if opcode == opClose {
			return nil
		}
		return errWSProtocol
	}
	if opcode == opClose {
		c.closeSent = true
	}
	// Allow extra time for large frames, such as the first load of a long
	// document over a slow link.
	deadline := wsWriteTimeout + time.Duration(len(frame)/(64<<10))*time.Second
	_ = c.conn.SetWriteDeadline(time.Now().Add(deadline))
	_, err := c.conn.Write(frame)
	return err
}

func (c *wsConn) WriteText(payload []byte) error { return c.writeFrame(opText, payload) }
func (c *wsConn) WritePing() error               { return c.writeFrame(opPing, nil) }

// Close sends a close frame unless one already went out, then hangs up.
func (c *wsConn) Close() {
	c.once.Do(func() {
		// Cut short any write that is stuck on a slow peer.
		_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
		var status [2]byte
		binary.BigEndian.PutUint16(status[:], closeNormal)
		c.writeMu.Lock()
		if !c.closeSent {
			c.closeSent = true
			_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = c.conn.Write([]byte{0x80 | opClose, 2, status[0], status[1]})
		}
		c.writeMu.Unlock()
		_ = c.conn.Close()
	})
}
