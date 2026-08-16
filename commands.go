package main

import (
	"strconv"
	"strings"
	"time"
)

// writeCommands lists command names that mutate state and therefore get
// persisted to the AOF (when one is configured). Read commands never touch
// the log.
var writeCommands = map[string]bool{
	"SET": true, "DEL": true, "EXPIRE": true, "PERSIST": true, "FLUSHALL": true,
}

// dispatch executes one already-parsed command against store and returns
// the RESP reply to send back. If aof is non-nil and the command is a
// write command, the command is appended to the log *after* it has been
// successfully applied — so a command that fails validation (wrong number
// of arguments, etc.) never gets persisted.
//
// dispatch is also the replay engine: LoadAOF calls it with aof=nil for
// every record in the log, which both reuses all the same command logic
// and guarantees replay can never itself write back to the file it's
// reading from.
func dispatch(store *Store, aof *AOFLogger, args []string) Reply {
	if len(args) == 0 {
		return ErrorReply("ERR empty command")
	}

	cmd := strings.ToUpper(args[0])
	rest := args[1:]

	var reply Reply
	switch cmd {
	case "PING":
		reply = cmdPing(rest)
	case "ECHO":
		reply = cmdEcho(rest)
	case "SET":
		reply = cmdSet(store, rest)
	case "GET":
		reply = cmdGet(store, rest)
	case "DEL":
		reply = cmdDel(store, rest)
	case "EXPIRE":
		reply = cmdExpire(store, rest)
	case "PERSIST":
		reply = cmdPersist(store, rest)
	case "TTL":
		reply = cmdTTL(store, rest)
	case "EXISTS":
		reply = cmdExists(store, rest)
	case "KEYS":
		reply = cmdKeys(store, rest)
	case "FLUSHALL":
		reply = cmdFlushAll(store, rest)
	case "DBSIZE":
		reply = Integer(store.Len())
	case "QUIT":
		reply = SimpleString("OK")
	default:
		reply = ErrorReply("ERR unknown command '" + args[0] + "'")
	}

	if aof != nil && writeCommands[cmd] {
		if _, isErr := reply.(ErrorReply); !isErr {
			if err := aof.Append(args); err != nil {
				// Persistence failing shouldn't corrupt the reply the
				// client already earned by mutating in-memory state
				// successfully; surface it as a server-side log line
				// instead (wired up in server.go).
				logAOFError(err)
			}
		}
	}

	return reply
}

func cmdPing(args []string) Reply {
	if len(args) == 0 {
		return SimpleString("PONG")
	}
	return NewBulkString([]byte(args[0]))
}

func cmdEcho(args []string) Reply {
	if len(args) != 1 {
		return ErrorReply("ERR wrong number of arguments for 'echo' command")
	}
	return NewBulkString([]byte(args[0]))
}

// cmdSet implements SET key value [EX seconds | PX milliseconds].
func cmdSet(store *Store, args []string) Reply {
	if len(args) < 2 {
		return ErrorReply("ERR wrong number of arguments for 'set' command")
	}
	key, value := args[0], args[1]

	var ttl time.Duration
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "EX", "PX":
			if i+1 >= len(args) {
				return ErrorReply("ERR syntax error")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || n <= 0 {
				return ErrorReply("ERR value is not an integer or out of range")
			}
			if opt == "EX" {
				ttl = time.Duration(n) * time.Second
			} else {
				ttl = time.Duration(n) * time.Millisecond
			}
			i += 2
		default:
			return ErrorReply("ERR syntax error")
		}
	}

	store.Set(key, []byte(value), ttl)
	return SimpleString("OK")
}

func cmdGet(store *Store, args []string) Reply {
	if len(args) != 1 {
		return ErrorReply("ERR wrong number of arguments for 'get' command")
	}
	v, ok := store.Get(args[0])
	if !ok {
		return NilBulkString()
	}
	return NewBulkString(v)
}

func cmdDel(store *Store, args []string) Reply {
	if len(args) < 1 {
		return ErrorReply("ERR wrong number of arguments for 'del' command")
	}
	return Integer(store.Del(args...))
}

func cmdExpire(store *Store, args []string) Reply {
	if len(args) != 2 {
		return ErrorReply("ERR wrong number of arguments for 'expire' command")
	}
	secs, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return ErrorReply("ERR value is not an integer or out of range")
	}
	ok := store.Expire(args[0], time.Duration(secs)*time.Second)
	if !ok {
		return Integer(0)
	}
	return Integer(1)
}

func cmdPersist(store *Store, args []string) Reply {
	if len(args) != 1 {
		return ErrorReply("ERR wrong number of arguments for 'persist' command")
	}
	if store.Persist(args[0]) {
		return Integer(1)
	}
	return Integer(0)
}

// cmdTTL implements TTL key, following Redis's own return convention:
// -2 if the key doesn't exist, -1 if it exists but has no expiry, else the
// remaining seconds (rounded up, so a key with 400ms left reports 1 rather
// than a misleading 0).
func cmdTTL(store *Store, args []string) Reply {
	if len(args) != 1 {
		return ErrorReply("ERR wrong number of arguments for 'ttl' command")
	}
	ttl, ok := store.TTL(args[0])
	if !ok {
		return Integer(-2)
	}
	if ttl == 0 {
		return Integer(-1) // Store.TTL returns exactly 0 to mean "no expiry set"
	}
	secs := int64(ttl / time.Second)
	if secs == 0 {
		secs = 1
	}
	return Integer(secs)
}

func cmdExists(store *Store, args []string) Reply {
	if len(args) < 1 {
		return ErrorReply("ERR wrong number of arguments for 'exists' command")
	}
	return Integer(store.Exists(args...))
}

// cmdKeys implements KEYS pattern. Only "*" (all keys) and exact literal
// matches are supported — full glob matching is intentionally out of scope
// for this subset.
func cmdKeys(store *Store, args []string) Reply {
	if len(args) != 1 {
		return ErrorReply("ERR wrong number of arguments for 'keys' command")
	}
	pattern := args[0]
	all := store.Keys()

	if pattern == "*" {
		out := make(Array, len(all))
		for i, k := range all {
			out[i] = NewBulkString([]byte(k))
		}
		return out
	}

	var out Array
	for _, k := range all {
		if k == pattern {
			out = append(out, NewBulkString([]byte(k)))
		}
	}
	return out
}

func cmdFlushAll(store *Store, args []string) Reply {
	if len(args) != 0 {
		return ErrorReply("ERR wrong number of arguments for 'flushall' command")
	}
	store.FlushAll()
	return SimpleString("OK")
}
