package collab

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAcceptKeyMatchesRFC6455(t *testing.T) {
	// The worked example from section 1.3 of the RFC.
	if got := wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("unexpected accept key %q", got)
	}
}

// testSocket is a minimal WebSocket client that can also misbehave on
// purpose, which a real client library would not let a test do.
type testSocket struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
}

func dialSocket(t *testing.T, server *httptest.Server, path string) *testSocket {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	keyBytes := make([]byte, 16)
	rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n", path, key)
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols || response.Header.Get("Sec-WebSocket-Accept") != wsAcceptKey(key) {
		t.Fatalf("handshake failed: %s %v", response.Status, response.Header)
	}
	return &testSocket{t: t, conn: conn, reader: reader}
}

func (s *testSocket) writeFrame(final bool, opcode byte, payload []byte, masked bool) {
	s.t.Helper()
	var frame []byte
	first := opcode
	if final {
		first |= 0x80
	}
	frame = append(frame, first)
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case len(payload) <= 125:
		frame = append(frame, maskBit|byte(len(payload)))
	case len(payload) <= 0xffff:
		frame = append(frame, maskBit|126)
		frame = binary.BigEndian.AppendUint16(frame, uint16(len(payload)))
	default:
		frame = append(frame, maskBit|127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	if masked {
		mask := []byte{0x11, 0x22, 0x33, 0x44}
		frame = append(frame, mask...)
		for index, value := range payload {
			frame = append(frame, value^mask[index%4])
		}
	} else {
		frame = append(frame, payload...)
	}
	if _, err := s.conn.Write(frame); err != nil {
		s.t.Fatal(err)
	}
}

func (s *testSocket) readFrame() (byte, []byte) {
	s.t.Helper()
	s.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var header [2]byte
	if _, err := io.ReadFull(s.reader, header[:]); err != nil {
		s.t.Fatalf("read frame: %v", err)
	}
	if header[1]&0x80 != 0 {
		s.t.Fatal("server frames must not be masked")
	}
	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		var extended [2]byte
		io.ReadFull(s.reader, extended[:])
		length = uint64(binary.BigEndian.Uint16(extended[:]))
	case 127:
		var extended [8]byte
		io.ReadFull(s.reader, extended[:])
		length = binary.BigEndian.Uint64(extended[:])
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(s.reader, payload); err != nil {
		s.t.Fatalf("read payload: %v", err)
	}
	return header[0] & 0x0f, payload
}

func (s *testSocket) sendJSON(value any) {
	s.t.Helper()
	s.writeFrame(true, opText, []byte(mustJSON(value)), true)
}

// readMessage returns the next text message of the wanted type.
func (s *testSocket) readMessage(kind string) map[string]any {
	s.t.Helper()
	for {
		opcode, payload := s.readFrame()
		if opcode != opText {
			continue
		}
		var message map[string]any
		if err := json.Unmarshal(payload, &message); err != nil {
			s.t.Fatal(err)
		}
		if message["t"] == kind {
			return message
		}
	}
}

func (s *testSocket) expectClose(status uint16) {
	s.t.Helper()
	for {
		opcode, payload := s.readFrame()
		if opcode != opClose {
			continue
		}
		if len(payload) < 2 || binary.BigEndian.Uint16(payload) != status {
			s.t.Fatalf("expected close status %d, got payload %v", status, payload)
		}
		return
	}
}

func socketServer(t *testing.T) (*Store, *httptest.Server, string) {
	t.Helper()
	store := openTestStore(t, Options{})
	server := httptest.NewServer(store.Handler())
	t.Cleanup(server.Close)
	info, err := store.Create("Socket doc", "blank", Author{ID: "u", Name: "U"})
	if err != nil {
		t.Fatal(err)
	}
	return store, server, info.ID
}

func hello(name string) map[string]any {
	return map[string]any{"t": "hello", "client": name, "user": map[string]string{"id": name, "name": name}}
}

func TestWebSocketEditingEndToEnd(t *testing.T) {
	store, server, id := socketServer(t)
	alice := dialSocket(t, server, "/api/docs/"+id+"/ws")
	bob := dialSocket(t, server, "/api/docs/"+id+"/ws")
	alice.sendJSON(hello("alice"))
	if init := alice.readMessage("init"); init["text"] != "" || init["rev"].(float64) != 0 {
		t.Fatalf("unexpected init %v", init)
	}
	bob.sendJSON(hello("bob"))
	bob.readMessage("init")

	alice.sendJSON(map[string]any{"t": "op", "rev": 0, "id": "alice:1", "op": []any{"hello"}})
	if ack := alice.readMessage("ack"); ack["rev"].(float64) != 1 || ack["id"] != "alice:1" {
		t.Fatalf("unexpected ack %v", ack)
	}
	if op := bob.readMessage("op"); mustJSON(op["op"]) != `["hello"]` || op["rev"].(float64) != 1 {
		t.Fatalf("unexpected broadcast %v", op)
	}

	// A message split across three frames with a ping in the middle.
	message := []byte(`{"t":"op","rev":1,"id":"bob:1","op":[5," world"]}`)
	bob.writeFrame(false, opText, message[:10], true)
	bob.writeFrame(true, opPing, []byte("still here"), true)
	bob.writeFrame(false, opContinuation, message[10:30], true)
	bob.writeFrame(true, opContinuation, message[30:], true)
	if opcode, payload := bob.readFrame(); opcode != opPong || string(payload) != "still here" {
		t.Fatalf("expected a pong echoing the ping, got opcode %d %q", opcode, payload)
	}
	bob.readMessage("ack")

	// Frames that need the 16-bit and the 64-bit length encodings.
	medium := strings.Repeat("m", 300)
	alice.sendJSON(map[string]any{"t": "op", "rev": 2, "id": "alice:2", "op": []any{11, medium}})
	alice.readMessage("ack")
	large := strings.Repeat("L", 70_000)
	alice.sendJSON(map[string]any{"t": "op", "rev": 3, "id": "alice:3", "op": []any{311, large}})
	alice.readMessage("ack")
	bob.readMessage("op")
	if broadcast := bob.readMessage("op"); len(broadcast["op"].([]any)[1].(string)) != 70_000 {
		t.Fatal("large broadcast was truncated")
	}
	if got := docText(t, store, id); got != "hello world"+medium+large {
		t.Fatalf("unexpected text of %d bytes", len(got))
	}

	// A clean close is echoed exactly once, and then the server hangs up.
	// A second close frame would be a protocol error that browsers report.
	alice.writeFrame(true, opClose, []byte{0x03, 0xe8}, true)
	alice.expectClose(closeNormal)
	alice.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if extra, err := io.ReadAll(alice.reader); err != nil || len(extra) != 0 {
		t.Fatalf("server sent %d more bytes after its close frame (%v)", len(extra), err)
	}
	if update := bob.readMessage("presence"); len(update["clients"].([]any)) != 1 {
		t.Fatalf("alice should be gone from presence: %v", update)
	}
}

func TestWebSocketRejectsProtocolViolations(t *testing.T) {
	_, server, id := socketServer(t)
	path := "/api/docs/" + id + "/ws"

	unmasked := dialSocket(t, server, path)
	unmasked.writeFrame(true, opText, []byte(`{"t":"hello"}`), false)
	unmasked.expectClose(closeProtocol)

	binaryFrame := dialSocket(t, server, path)
	binaryFrame.writeFrame(true, opBinary, []byte{1, 2, 3}, true)
	binaryFrame.expectClose(closeUnsupport)

	oversize := dialSocket(t, server, path)
	oversize.writeFrame(false, opText, make([]byte, maxClientMessage), true)
	oversize.writeFrame(true, opContinuation, []byte("x"), true)
	oversize.expectClose(closeTooLarge)

	stray := dialSocket(t, server, path)
	stray.writeFrame(true, opContinuation, []byte("x"), true)
	stray.expectClose(closeProtocol)

	garbage := dialSocket(t, server, path)
	garbage.writeFrame(true, opText, []byte("not json"), true)
	if message := garbage.readMessage("error"); !strings.Contains(message["message"].(string), "JSON") {
		t.Fatalf("unexpected error %v", message)
	}

	early := dialSocket(t, server, path)
	early.sendJSON(map[string]any{"t": "op", "rev": 0, "id": "x", "op": []any{"x"}})
	early.readMessage("error")
	early.expectClose(closeNormal)
}

func TestWebSocketHandshakeErrors(t *testing.T) {
	_, server, id := socketServer(t)
	url := server.URL + "/api/docs/" + id + "/ws"
	plain, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	plain.Body.Close()
	if plain.StatusCode != http.StatusBadRequest {
		t.Fatalf("a plain GET should be refused, got %s", plain.Status)
	}
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "8")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	old, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	old.Body.Close()
	if old.StatusCode != http.StatusUpgradeRequired || old.Header.Get("Sec-WebSocket-Version") != "13" {
		t.Fatalf("an old protocol version should be told to upgrade, got %s", old.Status)
	}
	missing, err := http.Get(server.URL + "/api/docs/nosuchdoc000/ws")
	if err != nil {
		t.Fatal(err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown document should be 404, got %s", missing.Status)
	}
}
