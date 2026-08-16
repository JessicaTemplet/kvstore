package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestReader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}

// TestConcurrentAccess hammers a single Store from many goroutines doing
// SET/GET/DEL/EXPIRE/TTL/EXISTS on an overlapping key space simultaneously.
// Run with `go test -race ./...` — this is the test the -race flag exists
// for: it won't catch bugs by assertion, it catches them by the race
// detector flagging any unsynchronized map access.
func TestConcurrentAccess(t *testing.T) {
	store := NewStore()
	store.StartActiveExpire(5*time.Millisecond, 50)
	defer store.Close()

	const (
		goroutines = 64
		opsEach    = 2000
		keySpace   = 32 // deliberately small so goroutines collide on the same keys
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("key-%d", (id*31+i)%keySpace)
				switch i % 6 {
				case 0:
					store.Set(key, []byte("value"), 0)
				case 1:
					store.Get(key)
				case 2:
					store.Del(key)
				case 3:
					store.Expire(key, 5*time.Millisecond)
				case 4:
					store.TTL(key)
				case 5:
					store.Exists(key)
				}
			}
		}(g)
	}
	wg.Wait()

	// No assertion on final contents — with concurrent SET/DEL/EXPIRE on
	// shared keys the end state is inherently nondeterministic. What this
	// test proves is that none of it raced.
	_ = store.Keys()
}

// TestConcurrentDispatchWithAOF exercises the same race scenario but
// through the full dispatch() + AOFLogger path, since AOF writes add a
// second lock (the logger's) that also needs to hold up under -race.
func TestConcurrentDispatchWithAOF(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.aof"

	aof, err := OpenAOF(path, FsyncNever)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer aof.Close()

	store := NewStore()
	defer store.Close()

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("k%d", (id+i)%16)
				dispatch(store, aof, []string{"SET", key, "v"})
				dispatch(store, aof, []string{"GET", key})
				dispatch(store, aof, []string{"DEL", key})
			}
		}(g)
	}
	wg.Wait()
}

func TestSetGetDel(t *testing.T) {
	s := NewStore()
	defer s.Close()

	s.Set("foo", []byte("bar"), 0)
	v, ok := s.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("Get(foo) = %q, %v; want bar, true", v, ok)
	}

	if n := s.Del("foo"); n != 1 {
		t.Fatalf("Del(foo) = %d; want 1", n)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatalf("Get(foo) after Del: found, want not found")
	}
}

func TestPassiveExpiry(t *testing.T) {
	s := NewStore()
	defer s.Close()

	s.Set("temp", []byte("x"), 20*time.Millisecond)
	if _, ok := s.Get("temp"); !ok {
		t.Fatalf("expected key present before TTL elapses")
	}

	time.Sleep(40 * time.Millisecond)

	if _, ok := s.Get("temp"); ok {
		t.Fatalf("expected key gone after TTL elapses (passive expiry on Get)")
	}
}

func TestActiveExpiryReclaimsWithoutRead(t *testing.T) {
	s := NewStore()
	s.StartActiveExpire(10*time.Millisecond, 100)
	defer s.Close()

	s.Set("temp", []byte("x"), 15*time.Millisecond)

	// Deliberately never call Get — only the active sweep should remove
	// this key from the underlying map.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		_, present := s.data["temp"]
		s.mu.RUnlock()
		if !present {
			return // success: active-expire cycle removed it
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active expire cycle never reclaimed an expired, unread key")
}

func TestExpireAndPersist(t *testing.T) {
	s := NewStore()
	defer s.Close()

	s.Set("k", []byte("v"), 0)
	if ok := s.Expire("k", 50*time.Millisecond); !ok {
		t.Fatalf("Expire on existing key should succeed")
	}
	ttl, ok := s.TTL("k")
	if !ok || ttl <= 0 || ttl > 50*time.Millisecond {
		t.Fatalf("TTL after Expire = %v, %v; want (0, 50ms], true", ttl, ok)
	}

	if ok := s.Persist("k"); !ok {
		t.Fatalf("Persist should succeed on a key with a TTL")
	}
	ttl, ok = s.TTL("k")
	if !ok || ttl != 0 {
		t.Fatalf("TTL after Persist = %v, %v; want 0, true", ttl, ok)
	}

	time.Sleep(100 * time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Fatalf("key should survive past its original TTL after Persist")
	}
}

func TestDispatchBasics(t *testing.T) {
	s := NewStore()
	defer s.Close()

	if r := dispatch(s, nil, []string{"SET", "a", "1"}); r != SimpleString("OK") {
		t.Fatalf("SET reply = %v; want OK", r)
	}
	if r := dispatch(s, nil, []string{"GET", "a"}); r.(BulkString).Valid != true || string(r.(BulkString).Data) != "1" {
		t.Fatalf("GET reply = %v; want bulk \"1\"", r)
	}
	if r := dispatch(s, nil, []string{"GET", "missing"}); r.(BulkString).Valid {
		t.Fatalf("GET on missing key should be nil bulk string")
	}
	if r := dispatch(s, nil, []string{"DEL", "a"}); r != Integer(1) {
		t.Fatalf("DEL reply = %v; want 1", r)
	}
	r := dispatch(s, nil, []string{"BOGUS"})
	if _, isErr := r.(ErrorReply); !isErr {
		t.Fatalf("unknown command should return an error reply, got %v", r)
	}
}

func TestAOFReplay(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.aof"

	// Write phase: apply some commands through dispatch with a live logger.
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	store1 := NewStore()
	dispatch(store1, aof, []string{"SET", "a", "1"})
	dispatch(store1, aof, []string{"SET", "b", "2"})
	dispatch(store1, aof, []string{"SET", "c", "3"})
	dispatch(store1, aof, []string{"DEL", "b"})
	dispatch(store1, aof, []string{"SET", "d", "4"})
	dispatch(store1, aof, []string{"EXPIRE", "d", "3600"})
	if err := aof.Close(); err != nil {
		t.Fatalf("aof.Close: %v", err)
	}
	store1.Close()

	// Replay phase: fresh store, rebuild from the log alone.
	store2 := NewStore()
	defer store2.Close()
	n, err := LoadAOF(path, store2)
	if err != nil {
		t.Fatalf("LoadAOF: %v", err)
	}
	if n != 6 {
		t.Fatalf("LoadAOF replayed %d commands; want 6", n)
	}

	if v, ok := store2.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("a = %q, %v; want 1, true", v, ok)
	}
	if _, ok := store2.Get("b"); ok {
		t.Fatalf("b should have been deleted by replay")
	}
	if v, ok := store2.Get("c"); !ok || string(v) != "3" {
		t.Fatalf("c = %q, %v; want 3, true", v, ok)
	}
	ttl, ok := store2.TTL("d")
	if !ok || ttl <= 0 {
		t.Fatalf("d should have a positive TTL restored by replay, got %v, %v", ttl, ok)
	}
}

func TestAOFReplayMissingFileIsNotAnError(t *testing.T) {
	store := NewStore()
	defer store.Close()
	n, err := LoadAOF(os.TempDir()+"/definitely-does-not-exist.aof", store)
	if err != nil {
		t.Fatalf("LoadAOF on missing file returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("LoadAOF on missing file replayed %d commands; want 0", n)
	}
}

func TestRESPRoundTrip(t *testing.T) {
	// A real RESP array, exactly as redis-cli would send `SET foo bar`.
	raw := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := newTestReader(raw)
	args, err := readCommand(r)
	if err != nil {
		t.Fatalf("readCommand: %v", err)
	}
	want := []string{"SET", "foo", "bar"}
	if len(args) != len(want) {
		t.Fatalf("args = %v; want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args[%d] = %q; want %q", i, args[i], want[i])
		}
	}
}

func TestRESPInlineCommand(t *testing.T) {
	r := newTestReader("SET foo bar\r\n")
	args, err := readCommand(r)
	if err != nil {
		t.Fatalf("readCommand: %v", err)
	}
	if len(args) != 3 || args[0] != "SET" || args[1] != "foo" || args[2] != "bar" {
		t.Fatalf("args = %v; want [SET foo bar]", args)
	}
}
