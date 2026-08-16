// Command kvstore is a lightweight, network-accessible, in-memory
// key-value store speaking a Redis-compatible subset of the RESP wire
// protocol: SET/GET/DEL/EXPIRE plus a handful of supporting commands,
// TTL eviction (both passive-on-read and an active background sweep), and
// optional append-only-file persistence.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var (
		addr           = flag.String("addr", ":6380", "TCP address to listen on")
		aofPath        = flag.String("aof", "", "append-only file path (empty = persistence disabled)")
		fsyncFlag      = flag.String("aof-fsync", "everysec", "AOF durability: always|everysec|no")
		expireInterval = flag.Duration("active-expire-interval", 100*time.Millisecond, "how often the background TTL sweep runs")
		expireSample   = flag.Int("active-expire-sample", 20, "keys sampled per active-expire sweep")
	)
	flag.Parse()

	store := NewStore()
	store.StartActiveExpire(*expireInterval, *expireSample)
	defer store.Close()

	var aof *AOFLogger
	if *aofPath != "" {
		policy, err := parseFsyncPolicy(*fsyncFlag)
		if err != nil {
			log.Fatalf("invalid -aof-fsync: %v", err)
		}

		n, err := LoadAOF(*aofPath, store)
		if err != nil {
			log.Fatalf("replaying AOF: %v", err)
		}
		if n > 0 {
			log.Printf("replayed %d commands from %s (%d keys loaded)", n, *aofPath, store.Len())
		}

		aof, err = OpenAOF(*aofPath, policy)
		if err != nil {
			log.Fatalf("opening AOF: %v", err)
		}
		defer aof.Close()
	}

	server := NewServer(store, aof)

	// Shut down cleanly on Ctrl-C/SIGTERM: stop accepting new connections,
	// let in-flight commands finish, then flush the AOF via the deferred
	// Close calls above.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		server.Shutdown()
	}()

	if err := server.Serve(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func parseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "always":
		return FsyncAlways, nil
	case "everysec":
		return FsyncEverySecond, nil
	case "no":
		return FsyncNever, nil
	default:
		return 0, errInvalidFsyncPolicy(s)
	}
}

type errInvalidFsyncPolicy string

func (e errInvalidFsyncPolicy) Error() string {
	return "must be one of always|everysec|no, got " + string(e)
}
