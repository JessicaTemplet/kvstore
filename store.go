package main

import (
	"sync"
	"time"
)

// entry is one stored value plus its optional expiry.
type entry struct {
	data      []byte
	expiresAt time.Time // zero value means "no expiry"
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Store is the in-memory key-value table. A single sync.RWMutex protects
// the map: readers (GET, TTL, EXISTS, KEYS) take RLock and can run
// concurrently; writers (SET, DEL, EXPIRE, FLUSHALL) take the exclusive
// Lock. This is the classic "one map, one RWMutex" design — simple and
// fast under read-heavy workloads, and correct under `go test -race`.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry

	stopActive chan struct{}
	activeWG   sync.WaitGroup
}

// NewStore creates an empty store and starts its background active-expire
// cycle (see StartActiveExpire).
func NewStore() *Store {
	return &Store{
		data:       make(map[string]entry),
		stopActive: make(chan struct{}),
	}
}

// Set stores value under key. ttl <= 0 means "no expiry".
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	e := entry{data: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	s.mu.Lock()
	s.data[key] = e
	s.mu.Unlock()
}

// Get returns the value for key and whether it was found (and not
// expired). This implements *passive* (lazy) expiry: a read that lands on
// an expired key deletes it on the spot instead of returning stale data,
// even if the active-expire goroutine hasn't gotten to it yet.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if e.expired(time.Now()) {
		s.deleteIfExpired(key)
		return nil, false
	}
	return e.data, true
}

// deleteIfExpired re-checks expiry under the write lock before deleting,
// since time may have passed (or another goroutine may have refreshed the
// key with SET/EXPIRE) between the RLock check in Get and here.
func (s *Store) deleteIfExpired(key string) {
	now := time.Now()
	s.mu.Lock()
	if e, ok := s.data[key]; ok && e.expired(now) {
		delete(s.data, key)
	}
	s.mu.Unlock()
}

// Del removes keys, returning how many actually existed.
func (s *Store) Del(keys ...string) int {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if e, ok := s.data[k]; ok {
			if !e.expired(now) {
				n++
			}
			delete(s.data, k)
		}
	}
	return n
}

// Expire sets a TTL on an existing, non-expired key. Returns false if the
// key doesn't exist (or is already expired).
func (s *Store) Expire(key string, ttl time.Duration) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return false
	}
	if ttl <= 0 {
		delete(s.data, key) // EXPIRE with a non-positive TTL deletes immediately, matching Redis semantics
		return true
	}
	e.expiresAt = now.Add(ttl)
	s.data[key] = e
	return true
}

// Persist removes any TTL from key, making it permanent. Returns true if a
// TTL was actually removed.
func (s *Store) Persist(key string) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok || e.expired(now) || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	s.data[key] = e
	return true
}

// TTL returns remaining time-to-live. ok=false means the key doesn't
// exist; a zero duration with ok=true means the key exists but has no
// expiry set.
func (s *Store) TTL(key string) (ttl time.Duration, ok bool) {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, exists := s.data[key]
	if !exists || e.expired(now) {
		return 0, false
	}
	if e.expiresAt.IsZero() {
		return 0, true
	}
	return e.expiresAt.Sub(now), true
}

// Exists reports how many of the given keys are present (and unexpired).
func (s *Store) Exists(keys ...string) int {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, k := range keys {
		if e, ok := s.data[k]; ok && !e.expired(now) {
			n++
		}
	}
	return n
}

// Keys returns all non-expired keys. Snapshot semantics: it reflects the
// store at the instant the RLock was held, not a live view.
func (s *Store) Keys() []string {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if !e.expired(now) {
			out = append(out, k)
		}
	}
	return out
}

// FlushAll deletes every key.
func (s *Store) FlushAll() {
	s.mu.Lock()
	s.data = make(map[string]entry)
	s.mu.Unlock()
}

// Len returns the raw entry count, including not-yet-swept expired keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// StartActiveExpire launches a background goroutine that periodically
// samples the keyspace and evicts expired keys, so memory used by expired-
// but-never-read keys is eventually reclaimed even without passive access.
// Modeled on Redis's own probabilistic active-expire cycle, simplified: each
// tick it takes a bounded sample (map iteration order is randomized by Go
// itself, so no extra shuffling is needed) rather than scanning the whole
// keyspace, so a single tick never blocks incoming connections for long.
func (s *Store) StartActiveExpire(interval time.Duration, sampleSize int) {
	s.activeWG.Add(1)
	go func() {
		defer s.activeWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.activeExpireCycle(sampleSize)
			case <-s.stopActive:
				return
			}
		}
	}()
}

// activeExpireCycle does one sweep: a cheap RLock pass to find candidates,
// then a short Lock pass to actually delete them. Splitting the scan from
// the delete keeps the exclusive-lock window small and bounded by
// sampleSize, not by total keyspace size.
func (s *Store) activeExpireCycle(sampleSize int) {
	now := time.Now()

	s.mu.RLock()
	expired := make([]string, 0, sampleSize)
	i := 0
	for k, e := range s.data {
		if i >= sampleSize {
			break
		}
		i++
		if e.expired(now) {
			expired = append(expired, k)
		}
	}
	s.mu.RUnlock()

	if len(expired) == 0 {
		return
	}

	s.mu.Lock()
	for _, k := range expired {
		if e, ok := s.data[k]; ok && e.expired(now) {
			delete(s.data, k)
		}
	}
	s.mu.Unlock()
}

// Close stops the active-expire goroutine and waits for it to exit.
func (s *Store) Close() {
	close(s.stopActive)
	s.activeWG.Wait()
}
