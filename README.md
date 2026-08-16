# kvstore

A lightweight, network-accessible, in-memory key-value store, speaking a
Redis-compatible subset of the RESP wire protocol — talk to it with
`redis-cli`, `nc`, or any Redis client library.

Standard library only: `net`, `bufio`, `sync`, `time`, `os`. No third-party
dependencies.

## Build & run

```bash
go build -o kvstore .
./kvstore -addr :6380
```

Talk to it with real `redis-cli` (point it at the non-default port):

```bash
redis-cli -p 6380
127.0.0.1:6380> SET foo bar EX 60
OK
127.0.0.1:6380> GET foo
"bar"
127.0.0.1:6380> TTL foo
(integer) 59
```

Or with plain `nc` / `telnet`, since the server also accepts inline
(non-RESP-array) commands for quick manual testing:

```bash
nc localhost 6380
SET foo bar
GET foo
```

## Commands

`PING`, `ECHO`, `SET key value [EX seconds | PX ms]`, `GET`, `DEL key...`,
`EXPIRE key seconds`, `PERSIST key`, `TTL key`, `EXISTS key...`,
`KEYS *` (or an exact literal — no glob matching beyond `*`), `FLUSHALL`,
`DBSIZE`, `QUIT`.

## Flags

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:6380` | TCP listen address |
| `-aof` | *(disabled)* | Append-only-file path; enables persistence |
| `-aof-fsync` | `everysec` | `always` \| `everysec` \| `no` |
| `-active-expire-interval` | `100ms` | Background TTL sweep frequency |
| `-active-expire-sample` | `20` | Keys sampled per sweep |

`Ctrl-C`/SIGTERM drains in-flight connections and flushes the AOF before
exiting.

## Architecture

```
main.go       flags, AOF replay on startup, server lifecycle
store.go      Store: map[string]entry behind sync.RWMutex, TTL logic
resp.go       RESP2 reader (array + inline) and reply writer
commands.go   dispatch(): parses args, mutates Store, persists to AOF
aof.go        AOFLogger: append writes, replay on load
server.go     TCP accept loop, per-connection command loop
store_test.go concurrency stress test + AOF round-trip test
```

**Thread safety.** `Store` is one `map[string]entry` behind one
`sync.RWMutex`: `GET`/`TTL`/`EXISTS`/`KEYS` take `RLock` (concurrent
readers don't block each other), `SET`/`DEL`/`EXPIRE`/`FLUSHALL` take the
exclusive `Lock`. `go test -race` runs 64 goroutines × 2,000 ops each
against a deliberately small (32-key) shared key space to force
collisions, and passes clean.

**Passive TTL eviction.** Every `Get` checks the entry's expiry before
returning it. If expired, it deletes the key on the spot (`deleteIfExpired`
re-checks under the write lock in case the key was refreshed between the
read check and the delete) rather than serving stale data.

**Active TTL eviction.** A background goroutine (`StartActiveExpire`) ticks
every `-active-expire-interval` and reclaims memory from expired keys that
nothing ever reads again. Each cycle takes a bounded sample (not a full
keyspace scan) under a brief `RLock`, then deletes just those candidates
under a brief `Lock` — so one sweep is O(sample size), not O(keyspace), and
never blocks incoming connections for long. (Go's own map iteration order
is randomized per-run, which is what gives this "sampling" its randomness
for free — no separate shuffle needed.)

**AOF persistence.** Every write command is appended to disk in the exact
RESP wire format clients send it in (`encodeCommand`), so the same
`readCommand` parser used for live connections also drives replay
(`LoadAOF`) — one parser, two roles. `dispatch(store, aof, args)` only
appends *after* the command has been successfully applied, so a rejected
command (bad arg count, etc.) never pollutes the log. Three fsync policies
mirror Redis's own tradeoff: `always` (fsync every write — safest,
slowest), `everysec` (buffered + ticker-flushed once a second — the
default, and Redis's own default), `no` (leave it to the OS).

## Known limitations

- **TTL replay is relative, not absolute.** The AOF logs `EXPIRE key 100`
  verbatim; replaying it on restart computes a *new* deadline 100s from
  restart time, not from the original SET. A key that had 5 seconds left
  when the server stopped gets a full new TTL on restart. Real Redis avoids
  this by rewriting relative expiries as absolute `PEXPIREAT` timestamps in
  the AOF — worth doing here too if TTL precision across restarts matters
  for your use case.
- **`KEYS` only supports `*` or an exact literal**, not general glob
  patterns (`user:*`, `?oo`, etc.).
- **No AOF compaction/rewrite.** The log only ever grows; a long-running
  server with heavy write traffic will want a `BGREWRITEAOF`-style compaction
  pass that snapshots current state and truncates the log — not implemented
  here.
- **Single shard.** One mutex for the whole keyspace is simple and correct,
  but a write-heavy workload with many keys would benefit from sharding
  (e.g. 16 or 32 `Store`s selected by `hash(key) % N`) to reduce lock
  contention — noted as the natural next step, not implemented to keep this
  version's locking story easy to reason about (and easy to verify under
  `-race`).
