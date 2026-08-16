package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// FsyncPolicy controls how aggressively the AOF is flushed to disk,
// trading durability against throughput — the same three-way tradeoff
// Redis itself exposes via appendfsync.
type FsyncPolicy int

const (
	// FsyncAlways calls File.Sync() after every single write. Safest,
	// slowest: survives a crash with zero data loss but costs a disk
	// flush per command.
	FsyncAlways FsyncPolicy = iota
	// FsyncEverySecond buffers writes and fsyncs once a second on a
	// background ticker. This is Redis's own default: at most ~1s of
	// writes lost on a hard crash, with buffered-write throughput.
	FsyncEverySecond
	// FsyncNever leaves fsync entirely to the OS's own page-cache flush
	// schedule. Fastest, least durable.
	FsyncNever
)

// AOFLogger appends every write command to disk in RESP wire format and
// can replay that log to rebuild state on startup. A single mutex
// serializes writes from concurrent client connections onto one file
// handle; the buffered writer amortizes syscall overhead between fsyncs.
type AOFLogger struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	policy FsyncPolicy

	stopFlush chan struct{}
	flushWG   sync.WaitGroup
}

// OpenAOF opens (creating if needed) the AOF file at path for appending
// and starts any background flush goroutine the policy requires.
func OpenAOF(path string, policy FsyncPolicy) (*AOFLogger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening AOF file: %w", err)
	}

	l := &AOFLogger{
		file:      f,
		writer:    bufio.NewWriter(f),
		policy:    policy,
		stopFlush: make(chan struct{}),
	}

	if policy == FsyncEverySecond {
		l.flushWG.Add(1)
		go l.periodicFlush()
	}

	return l, nil
}

func (l *AOFLogger) periodicFlush() {
	defer l.flushWG.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			l.writer.Flush()
			l.file.Sync()
			l.mu.Unlock()
		case <-l.stopFlush:
			return
		}
	}
}

// Append writes one command to the log. Called after the command has
// already been applied to the in-memory store, mirroring how the server
// only persists commands that actually mutated state.
func (l *AOFLogger) Append(args []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.writer.Write(encodeCommand(args)); err != nil {
		return err
	}

	switch l.policy {
	case FsyncAlways:
		if err := l.writer.Flush(); err != nil {
			return err
		}
		return l.file.Sync()
	case FsyncNever, FsyncEverySecond:
		// Buffered; flushed by the ticker (EverySecond) or left to the
		// OS (Never). Either way we don't block the caller here.
		return nil
	}
	return nil
}

// Close flushes any buffered data, stops the background flush goroutine
// (if running), and closes the underlying file.
func (l *AOFLogger) Close() error {
	if l.policy == FsyncEverySecond {
		close(l.stopFlush)
		l.flushWG.Wait()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.writer.Flush(); err != nil {
		return err
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	return l.file.Close()
}

// LoadAOF replays every command in the AOF file at path against store,
// rebuilding state exactly as the original writes produced it. It's a
// no-op (not an error) if the file doesn't exist yet — a brand new server
// just starts with an empty store.
func LoadAOF(path string, store *Store) (int, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("opening AOF file: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	applied := 0
	for {
		args, err := readCommand(r)
		if err != nil {
			break // EOF (or a truncated final record from a crash mid-write) ends replay
		}
		if len(args) == 0 {
			continue
		}
		// aofLogger=nil: replay must not re-append what it's replaying.
		dispatch(store, nil, args)
		applied++
	}
	return applied, nil
}
