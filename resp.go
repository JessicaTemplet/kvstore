// RESP is the wire protocol Redis (and every redis-cli / client library)
// speaks: https://redis.io/docs/latest/develop/reference/protocol-spec/
//
// This implements RESP2, which is all a client needs to talk to this
// server: requests arrive as arrays of bulk strings, replies go out as one
// of Simple String / Error / Integer / Bulk String / Array.
package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ---- Reading requests ----

// readCommand reads one client request from r and returns its arguments
// (e.g. ["SET", "foo", "bar"]). It supports both real RESP arrays (what
// redis-cli and every client library send) and plain inline commands
// (space-separated text, so you can `nc localhost 6380` and type commands
// by hand for quick testing) — this mirrors how real Redis servers accept
// both forms on the same port.
func readCommand(r *bufio.Reader) ([]string, error) {
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	if b[0] == '*' {
		return readRESPArray(r)
	}
	return readInline(r)
}

func readInline(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return []string{}, nil
	}
	return strings.Fields(line), nil
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("protocol error: expected '*', got %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, fmt.Errorf("protocol error: invalid array length: %w", err)
	}
	if n < 0 {
		return []string{}, nil // null array, treat as empty command
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		arg, err := readBulkString(r)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("protocol error: expected '$', got %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return "", fmt.Errorf("protocol error: invalid bulk length: %w", err)
	}
	if n < 0 {
		return "", nil // null bulk string
	}

	buf := make([]byte, n+2) // +2 for trailing \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

// readLine reads a single CRLF-terminated line and strips the terminator.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ---- Writing replies ----

// Reply is anything that knows how to serialize itself as a RESP reply.
type Reply interface {
	WriteTo(w *bufio.Writer)
}

type SimpleString string

func (s SimpleString) WriteTo(w *bufio.Writer) {
	w.WriteByte('+')
	w.WriteString(string(s))
	w.WriteString("\r\n")
}

type ErrorReply string

func (e ErrorReply) WriteTo(w *bufio.Writer) {
	w.WriteByte('-')
	w.WriteString(string(e))
	w.WriteString("\r\n")
}

type Integer int64

func (n Integer) WriteTo(w *bufio.Writer) {
	w.WriteByte(':')
	w.WriteString(strconv.FormatInt(int64(n), 10))
	w.WriteString("\r\n")
}

// BulkString is a binary-safe string reply. Valid=false serializes as the
// RESP null bulk string ($-1\r\n), used for e.g. GET on a missing key.
type BulkString struct {
	Valid bool
	Data  []byte
}

func NewBulkString(data []byte) BulkString { return BulkString{Valid: true, Data: data} }
func NilBulkString() BulkString            { return BulkString{Valid: false} }

func (b BulkString) WriteTo(w *bufio.Writer) {
	if !b.Valid {
		w.WriteString("$-1\r\n")
		return
	}
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(b.Data)))
	w.WriteString("\r\n")
	w.Write(b.Data)
	w.WriteString("\r\n")
}

type Array []Reply

func (a Array) WriteTo(w *bufio.Writer) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(a)))
	w.WriteString("\r\n")
	for _, r := range a {
		r.WriteTo(w)
	}
}

// encodeCommand serializes a command as a RESP array of bulk strings. Used
// by the AOF writer to persist commands in the same wire format clients
// send them in, so the AOF can be replayed with the exact same parser.
func encodeCommand(args []string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return []byte(b.String())
}
