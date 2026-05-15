package sr

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestForwardRoundTrip(t *testing.T) {
	dir := t.TempDir()
	serverKey, clientKey := writeTestKeys(t, dir)

	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()
	serverAddr := freeAddr(t)
	listenAddr := freeAddr(t)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	go func() {
		err := RunServer(serverCtx, ServerConfig{ListenAddr: serverAddr, KeyPath: serverKey})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("server failed: %v", err)
		}
	}()
	waitTCP(t, serverAddr)

	exposeCtx, cancelExpose := context.WithCancel(context.Background())
	defer cancelExpose()
	go func() {
		err := RunExpose(exposeCtx, ExposeConfig{Service: "echo", LocalAddr: echoAddr, RemoteAddr: serverAddr, KeyPath: clientKey})
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Errorf("expose failed: %v", err)
		}
	}()
	waitForExpose(t, clientKey, serverAddr, "echo")

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()
	go func() {
		err := RunListen(listenCtx, ListenConfig{Service: "echo", ListenAddr: listenAddr, RemoteAddr: serverAddr, KeyPath: clientKey})
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Errorf("listen failed: %v", err)
		}
	}()
	waitTCP(t, listenAddr)

	conn, err := net.Dial("tcp", listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello sr")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("hello sr"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello sr" {
		t.Fatalf("got %q", string(buf))
	}
}

func TestDuplicateExposeRejected(t *testing.T) {
	dir := t.TempDir()
	serverKey, clientKey := writeTestKeys(t, dir)
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()
	serverAddr := freeAddr(t)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	go func() {
		err := RunServer(serverCtx, ServerConfig{ListenAddr: serverAddr, KeyPath: serverKey})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("server failed: %v", err)
		}
	}()
	waitTCP(t, serverAddr)

	exposeCtx, cancelExpose := context.WithCancel(context.Background())
	defer cancelExpose()
	go func() {
		err := RunExpose(exposeCtx, ExposeConfig{Service: "dup", LocalAddr: echoAddr, RemoteAddr: serverAddr, KeyPath: clientKey})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("first expose failed: %v", err)
		}
	}()
	waitForExpose(t, clientKey, serverAddr, "dup")

	err := RunExpose(context.Background(), ExposeConfig{Service: "dup", LocalAddr: echoAddr, RemoteAddr: serverAddr, KeyPath: clientKey})
	if err == nil || !strings.Contains(err.Error(), "service already exposed") {
		t.Fatalf("expected duplicate expose rejection, got %v", err)
	}
}

func TestDiscoverDoesNotTriggerExposeConnect(t *testing.T) {
	dir := t.TempDir()
	serverKey, clientKey := writeTestKeys(t, dir)
	serverAddr := freeAddr(t)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	go func() {
		err := RunServer(serverCtx, ServerConfig{ListenAddr: serverAddr, KeyPath: serverKey})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("server failed: %v", err)
		}
	}()
	waitTCP(t, serverAddr)

	tlsCfg, err := LoadClientTLSConfig(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	exposeConn, err := tls.Dial("tcp", serverAddr, tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer exposeConn.Close()
	exposeCtrl := newJSONConn(exposeConn)
	if err := exposeCtrl.writeMessage(message{Type: "expose", Service: "discover-only"}); err != nil {
		t.Fatal(err)
	}
	msg, err := exposeCtrl.readMessage()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != "ok" {
		t.Fatalf("expected expose ok, got %#v", msg)
	}

	if err := discoverRemoteService(tlsCfg, serverAddr, "discover-only"); err != nil {
		t.Fatal(err)
	}
	if err := exposeConn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	msg, err = exposeCtrl.readMessage()
	if err == nil {
		t.Fatalf("discover unexpectedly triggered control message %#v", msg)
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected timeout waiting for control message, got %v", err)
	}
}

func TestListenWaitsForExposeBeforeBinding(t *testing.T) {
	dir := t.TempDir()
	serverKey, clientKey := writeTestKeys(t, dir)
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()
	serverAddr := freeAddr(t)
	listenAddr := freeAddr(t)
	logs := &safeBuffer{}

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	go func() {
		err := RunServer(serverCtx, ServerConfig{ListenAddr: serverAddr, KeyPath: serverKey})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("server failed: %v", err)
		}
	}()
	waitTCP(t, serverAddr)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()
	go func() {
		err := RunListen(listenCtx, ListenConfig{
			Service:       "late",
			ListenAddr:    listenAddr,
			RemoteAddr:    serverAddr,
			KeyPath:       clientKey,
			RetryInterval: 25 * time.Millisecond,
			LogWriter:     logs,
		})
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Errorf("listen failed: %v", err)
		}
	}()
	waitNoTCP(t, listenAddr, 150*time.Millisecond)

	exposeCtx, cancelExpose := context.WithCancel(context.Background())
	defer cancelExpose()
	go func() {
		err := RunExpose(exposeCtx, ExposeConfig{Service: "late", LocalAddr: echoAddr, RemoteAddr: serverAddr, KeyPath: clientKey})
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Errorf("expose failed: %v", err)
		}
	}()
	waitTCP(t, listenAddr)

	conn, err := net.Dial("tcp", listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("late hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("late hello"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "late hello" {
		t.Fatalf("got %q", string(buf))
	}
	logText := logs.String()
	if !strings.Contains(logText, "service late is not exposed") {
		t.Fatalf("missing retry log in %q", logText)
	}
	if !strings.Contains(logText, "service late discovered") {
		t.Fatalf("missing discovered log in %q", logText)
	}
}

func writeTestKeys(t *testing.T, dir string) (string, string) {
	t.Helper()
	serverBundle, err := GenerateServerBundle()
	if err != nil {
		t.Fatal(err)
	}
	clientBundle, err := GenerateClientBundle(serverBundle, "test-user")
	if err != nil {
		t.Fatal(err)
	}
	serverKey := filepath.Join(dir, "server.key")
	clientKey := filepath.Join(dir, "client.key")
	if err := os.WriteFile(serverKey, serverBundle, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientKey, clientBundle, 0600); err != nil {
		t.Fatal(err)
	}
	return serverKey, clientKey
}

type safeBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func startEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitNoTCP(t *testing.T, addr string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 25*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("%s accepted TCP before service discovery", addr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", addr)
}

func waitForExpose(t *testing.T, keyPath, serverAddr, service string) {
	t.Helper()
	tlsCfg, err := LoadClientTLSConfig(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := discoverRemoteService(tlsCfg, serverAddr, service)
		if err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for expose %s", service)
}
