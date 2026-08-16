package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// aofErrCount lets the periodic flush / append path report failures
// without a hard dependency on *log.Logger threading through every call.
var aofErrCount int64

func logAOFError(err error) {
	if atomic.AddInt64(&aofErrCount, 1) <= 5 { // don't spam the log if the disk is out forever
		log.Printf("aof: write error: %v", err)
	}
}

// Server owns the listener and every open connection, so it can drain
// connections cleanly on shutdown instead of yanking the socket out from
// under in-flight commands.
type Server struct {
	store *Store
	aof   *AOFLogger

	listener net.Listener

	connWG sync.WaitGroup
}

func NewServer(store *Store, aof *AOFLogger) *Server {
	return &Server{store: store, aof: aof}
}

// Serve binds addr and accepts connections until the listener is closed
// (typically via Shutdown from a signal handler).
func (s *Server) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("kvstore listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // clean shutdown
			}
			log.Printf("accept error: %v", err)
			continue
		}
		s.connWG.Add(1)
		go s.handleConn(conn)
	}
}

// Shutdown stops accepting new connections and waits for in-flight ones to
// finish their current command and exit.
func (s *Server) Shutdown() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.connWG.Wait()
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.connWG.Done()
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		args, err := readCommand(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// A malformed request desyncs the protocol for this
				// connection (we no longer know where the next command
				// starts), so we log and close rather than try to
				// resync — same behavior real Redis takes on a protocol
				// error.
				log.Printf("%s: protocol error: %v", conn.RemoteAddr(), err)
			}
			return
		}
		if len(args) == 0 {
			continue // blank inline command line, e.g. someone just hit Enter
		}

		reply := dispatch(s.store, s.aof, args)
		reply.WriteTo(w)

		if err := w.Flush(); err != nil {
			return
		}

		if len(args) == 1 && strings.ToUpper(args[0]) == "QUIT" {
			return
		}
	}
}
