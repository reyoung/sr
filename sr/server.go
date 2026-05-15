package sr

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

type ServerConfig struct {
	ListenAddr string
	KeyPath    string
}

type serverState struct {
	mu       sync.Mutex
	exposers map[string]*exposer
	nextID   atomic.Uint64
}

type exposer struct {
	service string
	ctrl    *jsonConn
	pending map[string]chan *jsonConn
	done    chan struct{}
}

func RunServer(ctx context.Context, cfg ServerConfig) error {
	tlsCfg, err := LoadServerTLSConfig(cfg.KeyPath)
	if err != nil {
		return err
	}
	ln, err := tls.Listen("tcp", cfg.ListenAddr, tlsCfg)
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	st := &serverState{exposers: map[string]*exposer{}}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go st.handle(conn)
	}
}

func (s *serverState) handle(conn net.Conn) {
	jc := newJSONConn(conn)
	msg, err := jc.readMessage()
	if err != nil {
		_ = conn.Close()
		return
	}
	switch msg.Type {
	case "expose":
		s.handleExpose(jc, msg)
	case "discover":
		s.handleDiscover(jc, msg)
	case "listen_stream":
		s.handleListenStream(jc, msg)
	case "expose_stream":
		s.handleExposeStream(jc, msg)
	default:
		_ = jc.writeMessage(message{Type: "error", Error: "unknown message type"})
		_ = conn.Close()
	}
}

func (s *serverState) handleExpose(jc *jsonConn, msg message) {
	if err := validateServiceName(msg.Service); err != nil {
		_ = jc.writeMessage(message{Type: "error", Error: err.Error()})
		_ = jc.conn.Close()
		return
	}
	ex := &exposer{service: msg.Service, ctrl: jc, pending: map[string]chan *jsonConn{}, done: make(chan struct{})}

	s.mu.Lock()
	if _, exists := s.exposers[msg.Service]; exists {
		s.mu.Unlock()
		_ = jc.writeMessage(message{Type: "error", Error: "service already exposed"})
		_ = jc.conn.Close()
		return
	}
	s.exposers[msg.Service] = ex
	s.mu.Unlock()

	_ = jc.writeMessage(message{Type: "ok"})
	_, _ = jc.r.ReadBytes(0)

	s.mu.Lock()
	if s.exposers[msg.Service] == ex {
		delete(s.exposers, msg.Service)
	}
	for _, ch := range ex.pending {
		close(ch)
	}
	close(ex.done)
	s.mu.Unlock()
	_ = jc.conn.Close()
}

func (s *serverState) handleDiscover(jc *jsonConn, msg message) {
	if err := validateServiceName(msg.Service); err != nil {
		_ = jc.writeMessage(message{Type: "error", Error: err.Error()})
		_ = jc.conn.Close()
		return
	}
	s.mu.Lock()
	_, exists := s.exposers[msg.Service]
	s.mu.Unlock()
	if !exists {
		_ = jc.writeMessage(message{Type: "error", Error: "service is not exposed"})
		_ = jc.conn.Close()
		return
	}
	_ = jc.writeMessage(message{Type: "ok"})
	_ = jc.conn.Close()
}

func (s *serverState) handleListenStream(jc *jsonConn, msg message) {
	if err := validateServiceName(msg.Service); err != nil {
		_ = jc.writeMessage(message{Type: "error", Error: err.Error()})
		_ = jc.conn.Close()
		return
	}
	id := fmt.Sprintf("%d", s.nextID.Add(1))
	streamCh := make(chan *jsonConn, 1)

	s.mu.Lock()
	ex := s.exposers[msg.Service]
	if ex == nil {
		s.mu.Unlock()
		_ = jc.writeMessage(message{Type: "error", Error: "service is not exposed"})
		_ = jc.conn.Close()
		return
	}
	ex.pending[id] = streamCh
	s.mu.Unlock()

	if err := ex.ctrl.writeMessage(message{Type: "connect", Service: msg.Service, ID: id}); err != nil {
		s.removePending(ex, id)
		_ = jc.writeMessage(message{Type: "error", Error: "exposer is unavailable"})
		_ = jc.conn.Close()
		return
	}

	select {
	case remote, ok := <-streamCh:
		if !ok || remote == nil {
			_ = jc.writeMessage(message{Type: "error", Error: "exposer disconnected"})
			_ = jc.conn.Close()
			return
		}
		_ = jc.writeMessage(message{Type: "ok"})
		_ = remote.writeMessage(message{Type: "ok"})
		pipeConn(jc, remote)
	case <-ex.done:
		_ = jc.writeMessage(message{Type: "error", Error: "exposer disconnected"})
		_ = jc.conn.Close()
	}
}

func (s *serverState) handleExposeStream(jc *jsonConn, msg message) {
	s.mu.Lock()
	ex := s.exposers[msg.Service]
	var ch chan *jsonConn
	if ex != nil {
		ch = ex.pending[msg.ID]
		delete(ex.pending, msg.ID)
	}
	s.mu.Unlock()
	if ch == nil {
		_ = jc.writeMessage(message{Type: "error", Error: "unknown stream"})
		_ = jc.conn.Close()
		return
	}
	ch <- jc
}

func (s *serverState) removePending(ex *exposer, id string) {
	s.mu.Lock()
	delete(ex.pending, id)
	s.mu.Unlock()
}
