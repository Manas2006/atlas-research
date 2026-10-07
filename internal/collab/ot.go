// Package collab implements real-time collaborative documents for Atlas:
// an operational transformation engine, a durable per-document operation log,
// a WebSocket sync server, and agents that propose tracked suggestions.
package collab

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"
)

// Positions and lengths are counted in UTF-16 code units so that the Go server
// and the JavaScript client agree on every index without translation.

// Op is one edit to a whole document, expressed as a sequence of components
// that walk the document from start to end: retain n units, insert text, or
// delete n units. An Op only applies to a document of exactly BaseLen units
// and always produces a document of TargetLen units.
//
// On the wire an Op is a JSON array: a positive number retains, a negative
// number deletes, and a string inserts. [5,"abc",-2,10] keeps five units,
// inserts "abc", deletes two units, and keeps the final ten.
type Op struct {
	comps     []comp
	baseLen   int
	targetLen int
}

// comp is a retain (n > 0), a delete (n < 0), or an insert (n == 0, ins set).
type comp struct {
	n   int
	ins []uint16
}

func (c comp) isRetain() bool { return c.n > 0 }
func (c comp) isDelete() bool { return c.n < 0 }
func (c comp) isInsert() bool { return c.n == 0 }

var (
	errBaseLength = errors.New("operation does not match the document length")
	errCompose    = errors.New("operations cannot be composed: lengths differ")
	errTransform  = errors.New("operations cannot be transformed: base lengths differ")
)

// NewOp returns an empty operation ready for Retain, Insert, and Delete.
func NewOp() *Op { return &Op{} }

// BaseLen is the document length this operation applies to.
func (o *Op) BaseLen() int { return o.baseLen }

// TargetLen is the document length after this operation.
func (o *Op) TargetLen() int { return o.targetLen }

// IsNoop reports whether the operation leaves every document unchanged.
func (o *Op) IsNoop() bool {
	return len(o.comps) == 0 || (len(o.comps) == 1 && o.comps[0].isRetain())
}

// Retain keeps the next n units.
func (o *Op) Retain(n int) *Op {
	if n <= 0 {
		return o
	}
	o.baseLen += n
	o.targetLen += n
	if last := len(o.comps) - 1; last >= 0 && o.comps[last].isRetain() {
		o.comps[last].n += n
		return o
	}
	o.comps = append(o.comps, comp{n: n})
	return o
}

// Delete removes the next n units.
func (o *Op) Delete(n int) *Op {
	if n <= 0 {
		return o
	}
	o.baseLen += n
	if last := len(o.comps) - 1; last >= 0 && o.comps[last].isDelete() {
		o.comps[last].n -= n
		return o
	}
	o.comps = append(o.comps, comp{n: -n})
	return o
}

// Insert adds text at the current position. An insert that directly follows a
// delete is stored before it, so equal edits always have one representation.
func (o *Op) Insert(text []uint16) *Op {
	if len(text) == 0 {
		return o
	}
	o.targetLen += len(text)
	last := len(o.comps) - 1
	switch {
	case last >= 0 && o.comps[last].isInsert():
		o.comps[last].ins = append(o.comps[last].ins, text...)
	case last >= 0 && o.comps[last].isDelete():
		if last >= 1 && o.comps[last-1].isInsert() {
			o.comps[last-1].ins = append(o.comps[last-1].ins, text...)
		} else {
			o.comps = append(o.comps, o.comps[last])
			o.comps[last] = comp{ins: append([]uint16(nil), text...)}
		}
	default:
		o.comps = append(o.comps, comp{ins: append([]uint16(nil), text...)})
	}
	return o
}

// InsertString is Insert for a Go string.
func (o *Op) InsertString(text string) *Op { return o.Insert(encodeText(text)) }

func encodeText(text string) []uint16  { return utf16.Encode([]rune(text)) }
func decodeText(units []uint16) string { return string(utf16.Decode(units)) }

// Apply runs the operation against a document and returns the new document.
func (o *Op) Apply(doc []uint16) ([]uint16, error) {
	if len(doc) != o.baseLen {
		return nil, fmt.Errorf("%w: document has %d units, operation expects %d", errBaseLength, len(doc), o.baseLen)
	}
	out := make([]uint16, 0, o.targetLen)
	position := 0
	for _, c := range o.comps {
		switch {
		case c.isRetain():
			out = append(out, doc[position:position+c.n]...)
			position += c.n
		case c.isInsert():
			out = append(out, c.ins...)
		default:
			position -= c.n
		}
	}
	return out, nil
}

// cursor walks an operation's components and lets callers consume part of a
// component at a time, which is what compose and transform both need.
type cursor struct {
	comps []comp
	index int
	used  int
}

func (c *cursor) done() bool { return c.index >= len(c.comps) }

func (c *cursor) peek() comp { return c.comps[c.index] }

// remaining is the unconsumed length of the current component.
func (c *cursor) remaining() int {
	current := c.comps[c.index]
	switch {
	case current.isInsert():
		return len(current.ins) - c.used
	case current.isRetain():
		return current.n - c.used
	default:
		return -current.n - c.used
	}
}

// take consumes n units of the current component and returns the inserted
// text covered by them when the component is an insert.
func (c *cursor) take(n int) []uint16 {
	current := c.comps[c.index]
	var text []uint16
	if current.isInsert() {
		text = current.ins[c.used : c.used+n]
	}
	c.used += n
	if c.remaining() == 0 {
		c.index++
		c.used = 0
	}
	return text
}

// Compose returns one operation equivalent to applying a and then b.
func Compose(a, b *Op) (*Op, error) {
	if a.targetLen != b.baseLen {
		return nil, errCompose
	}
	out := NewOp()
	left, right := &cursor{comps: a.comps}, &cursor{comps: b.comps}
	for {
		if !left.done() && left.peek().isDelete() {
			out.Delete(left.remaining())
			left.take(left.remaining())
			continue
		}
		if !right.done() && right.peek().isInsert() {
			out.Insert(right.take(right.remaining()))
			continue
		}
		if left.done() && right.done() {
			return out, nil
		}
		if left.done() || right.done() {
			return nil, errCompose
		}
		n := min(left.remaining(), right.remaining())
		switch l, r := left.peek(), right.peek(); {
		case l.isRetain() && r.isRetain():
			out.Retain(n)
		case l.isRetain() && r.isDelete():
			out.Delete(n)
		case l.isInsert() && r.isRetain():
			out.Insert(left.comps[left.index].ins[left.used : left.used+n])
		}
		// An insert in a that b deletes cancels out and produces nothing.
		left.take(n)
		right.take(n)
	}
}

// Transform takes two operations made concurrently against the same document
// and returns a' and b' such that applying a then b' gives the same document
// as applying b then a'. When both insert at the same position, a's text is
// placed first. Callers must pass the client's operation as a on both the
// server and the client so every replica breaks ties the same way.
func Transform(a, b *Op) (*Op, *Op, error) {
	if a.baseLen != b.baseLen {
		return nil, nil, errTransform
	}
	aPrime, bPrime := NewOp(), NewOp()
	left, right := &cursor{comps: a.comps}, &cursor{comps: b.comps}
	for {
		if !left.done() && left.peek().isInsert() {
			text := left.take(left.remaining())
			aPrime.Insert(text)
			bPrime.Retain(len(text))
			continue
		}
		if !right.done() && right.peek().isInsert() {
			text := right.take(right.remaining())
			aPrime.Retain(len(text))
			bPrime.Insert(text)
			continue
		}
		if left.done() && right.done() {
			return aPrime, bPrime, nil
		}
		if left.done() || right.done() {
			return nil, nil, errTransform
		}
		n := min(left.remaining(), right.remaining())
		switch l, r := left.peek(), right.peek(); {
		case l.isRetain() && r.isRetain():
			aPrime.Retain(n)
			bPrime.Retain(n)
		case l.isDelete() && r.isRetain():
			aPrime.Delete(n)
		case l.isRetain() && r.isDelete():
			bPrime.Delete(n)
		}
		// Both sides deleting the same units needs no further action.
		left.take(n)
		right.take(n)
	}
}

// MapPos moves a position in the base document to the matching position in
// the target document. When text is inserted exactly at the position,
// afterInsert decides whether the position ends up after it (true) or stays
// in front of it (false).
func (o *Op) MapPos(position int, afterInsert bool) int {
	mapped, index := position, 0
	for _, c := range o.comps {
		if index > position {
			break
		}
		switch {
		case c.isRetain():
			index += c.n
		case c.isInsert():
			if index < position || (index == position && afterInsert) {
				mapped += len(c.ins)
			}
		default:
			if index < position {
				mapped -= min(-c.n, position-index)
			}
			index -= c.n
		}
	}
	return mapped
}

// MapRange moves the half-open range [start, end) through the operation and
// reports whether the operation changed anything inside it.
//
// While the content is untouched the range is mapped tightly: text inserted
// at either edge stays outside, so the range keeps holding exactly the text
// it held before. For an empty range the content counts as touched only when
// a delete removes text on both sides of the position.
//
// Once the content is touched there is no exact answer, so the range is
// mapped loosely instead and takes in text inserted at its edges. Replacing
// a word is a delete plus an insert at the same spot, and this is what makes
// the range end up around the new word rather than beside it.
func (o *Op) MapRange(start, end int) (int, int, bool) {
	touched, index := false, 0
	for _, c := range o.comps {
		switch {
		case c.isRetain():
			index += c.n
		case c.isInsert():
			if index > start && index < end {
				touched = true
			}
		default:
			deleteEnd := index - c.n
			if start == end {
				if index < start && deleteEnd > start {
					touched = true
				}
			} else if index < end && deleteEnd > start {
				touched = true
			}
			index = deleteEnd
		}
	}
	if touched {
		return o.MapPos(start, false), o.MapPos(end, true), true
	}
	mappedStart := o.MapPos(start, true)
	mappedEnd := o.MapPos(end, false)
	if mappedEnd < mappedStart {
		mappedEnd = mappedStart
	}
	return mappedStart, mappedEnd, false
}

// EditPos returns where the author's caret sits after the operation: just
// after the last inserted text, or at the last deletion. ok is false when the
// operation does not edit anything.
func (o *Op) EditPos() (position int, ok bool) {
	index := 0
	for _, c := range o.comps {
		switch {
		case c.isRetain():
			index += c.n
		case c.isInsert():
			index += len(c.ins)
			position, ok = index, true
		default:
			position, ok = index, true
		}
	}
	return position, ok
}

// MarshalJSON writes the wire form described on Op.
func (o Op) MarshalJSON() ([]byte, error) {
	parts := make([]any, 0, len(o.comps))
	for _, c := range o.comps {
		if c.isInsert() {
			parts = append(parts, decodeText(c.ins))
		} else {
			parts = append(parts, c.n)
		}
	}
	return json.Marshal(parts)
}

// maxOpComponents bounds how much work one decoded operation can demand.
// A browser edit has four components at most; a buffer composed over a long
// offline session can have a few thousand.
const maxOpComponents = 20_000

// maxComponentLength is far beyond any real document and keeps length
// arithmetic clear of integer overflow.
const maxComponentLength = 1 << 30

// UnmarshalJSON reads the wire form and rejects anything that is not a
// well-formed operation. The result is normalized, so adjacent components of
// the same kind are merged.
func (o *Op) UnmarshalJSON(payload []byte) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(payload, &parts); err != nil {
		return fmt.Errorf("operation must be a JSON array: %w", err)
	}
	if len(parts) > maxOpComponents {
		return errors.New("operation has too many components")
	}
	decoded := NewOp()
	for _, part := range parts {
		if len(part) > 0 && part[0] == '"' {
			var text string
			if err := json.Unmarshal(part, &text); err != nil {
				return fmt.Errorf("invalid insert component: %w", err)
			}
			if text == "" {
				return errors.New("insert component must not be empty")
			}
			decoded.InsertString(text)
			continue
		}
		var n int64
		if err := json.Unmarshal(part, &n); err != nil {
			return fmt.Errorf("invalid operation component: %w", err)
		}
		switch {
		case n > maxComponentLength || n < -maxComponentLength:
			return errors.New("operation component is too large")
		case n > 0:
			decoded.Retain(int(n))
		case n < 0:
			decoded.Delete(int(-n))
		default:
			return errors.New("operation component must not be zero")
		}
	}
	*o = *decoded
	return nil
}

// replaceOp builds the operation that replaces [start, end) of a document of
// the given length with text.
func replaceOp(length, start, end int, text []uint16) *Op {
	return NewOp().Retain(start).Insert(text).Delete(end - start).Retain(length - end)
}

func isHighSurrogate(unit uint16) bool { return unit >= 0xd800 && unit <= 0xdbff }
func isLowSurrogate(unit uint16) bool  { return unit >= 0xdc00 && unit <= 0xdfff }

// insidePair reports whether position sits between the two halves of a
// surrogate pair in doc.
func insidePair(doc []uint16, position int) bool {
	return position > 0 && position < len(doc) && isHighSurrogate(doc[position-1]) && isLowSurrogate(doc[position])
}

// checkText rejects operations that would leave the document in a state a
// browser cannot hold faithfully. Deleting or inserting inside a surrogate
// pair leaves half a character, which is replaced when the text is sent as
// JSON, so replicas would stop agreeing on content. A carriage return is
// silently dropped by a textarea, which would put that editor's positions
// out of step with everyone else's. No browser produces either; this guards
// against other clients.
func (o *Op) checkText(doc []uint16) error {
	position := 0
	for _, c := range o.comps {
		switch {
		case c.isRetain():
			position += c.n
		case c.isInsert():
			if insidePair(doc, position) {
				return errors.New("operation inserts inside a surrogate pair")
			}
			for _, unit := range c.ins {
				if unit == '\r' {
					return errors.New("operation inserts a carriage return")
				}
			}
		default:
			if insidePair(doc, position) || insidePair(doc, position-c.n) {
				return errors.New("operation deletes half of a surrogate pair")
			}
			position -= c.n
		}
	}
	return nil
}

// normalizeNewlines turns every carriage return style into a plain newline.
func normalizeNewlines(text string) string {
	if !strings.ContainsRune(text, '\r') {
		return text
	}
	return strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
}
