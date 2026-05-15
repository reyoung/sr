package sr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

type closeWriter interface {
	CloseWrite() error
}

type message struct {
	Type    string `json:"type"`
	Service string `json:"service,omitempty"`
	ID      string `json:"id,omitempty"`
	Error   string `json:"error,omitempty"`
}

type jsonConn struct {
	conn net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex
}

func newJSONConn(conn net.Conn) *jsonConn {
	return &jsonConn{conn: conn, r: bufio.NewReader(conn)}
}

func (c *jsonConn) readMessage() (message, error) {
	var msg message
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		return msg, err
	}
	if err := json.Unmarshal(line, &msg); err != nil {
		return msg, err
	}
	return msg, nil
}

func (c *jsonConn) writeMessage(msg message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = c.conn.Write(b)
	return err
}

func pipeConn(a *jsonConn, b *jsonConn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(a.conn, b.r)
		if cw, ok := a.conn.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(b.conn, a.r)
		if cw, ok := b.conn.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}()
	wg.Wait()
	_ = a.conn.Close()
	_ = b.conn.Close()
}

func validateServiceName(service string) error {
	if service == "" {
		return fmt.Errorf("service name is empty")
	}
	return nil
}
