package sr

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type ExposeConfig struct {
	Service    string
	LocalAddr  string
	RemoteAddr string
	KeyPath    string
}

type ListenConfig struct {
	Service       string
	ListenAddr    string
	RemoteAddr    string
	KeyPath       string
	RetryInterval time.Duration
	LogWriter     io.Writer
}

func RunExpose(ctx context.Context, cfg ExposeConfig) error {
	if err := validateServiceName(cfg.Service); err != nil {
		return err
	}
	tlsCfg, err := LoadClientTLSConfig(cfg.KeyPath)
	if err != nil {
		return err
	}
	conn, err := tls.Dial("tcp", cfg.RemoteAddr, tlsCfg)
	if err != nil {
		return err
	}
	ctrl := newJSONConn(conn)
	if err := ctrl.writeMessage(message{Type: "expose", Service: cfg.Service}); err != nil {
		_ = conn.Close()
		return err
	}
	msg, err := ctrl.readMessage()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if msg.Type != "ok" {
		_ = conn.Close()
		if msg.Error != "" {
			return errors.New(msg.Error)
		}
		return fmt.Errorf("server rejected expose")
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	for {
		msg, err := ctrl.readMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if msg.Type != "connect" {
			continue
		}
		go exposeOne(ctx, tlsCfg, cfg, msg.ID)
	}
}

func exposeOne(ctx context.Context, tlsCfg *tls.Config, cfg ExposeConfig, id string) {
	remote, err := tls.Dial("tcp", cfg.RemoteAddr, tlsCfg)
	if err != nil {
		return
	}
	rjc := newJSONConn(remote)
	if err := rjc.writeMessage(message{Type: "expose_stream", Service: cfg.Service, ID: id}); err != nil {
		_ = remote.Close()
		return
	}
	msg, err := rjc.readMessage()
	if err != nil || msg.Type != "ok" {
		_ = remote.Close()
		return
	}
	local, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.LocalAddr)
	if err != nil {
		_ = remote.Close()
		return
	}
	pipePlainJSON(local, rjc)
}

func RunListen(ctx context.Context, cfg ListenConfig) error {
	if err := validateServiceName(cfg.Service); err != nil {
		return err
	}
	tlsCfg, err := LoadClientTLSConfig(cfg.KeyPath)
	if err != nil {
		return err
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if err := waitForRemoteService(ctx, tlsCfg, cfg); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go listenOne(tlsCfg, cfg, conn)
	}
}

func waitForRemoteService(ctx context.Context, tlsCfg *tls.Config, cfg ListenConfig) error {
	for {
		err := discoverRemoteService(tlsCfg, cfg.RemoteAddr, cfg.Service)
		if err == nil {
			logListen(cfg, "service %s discovered on %s; listening on %s", cfg.Service, cfg.RemoteAddr, cfg.ListenAddr)
			return nil
		}
		if !errors.Is(err, errServiceNotExposed) {
			return err
		}
		logListen(cfg, "service %s is not exposed on %s; retrying in %s", cfg.Service, cfg.RemoteAddr, cfg.RetryInterval)
		timer := time.NewTimer(cfg.RetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var errServiceNotExposed = errors.New("service is not exposed")

func discoverRemoteService(tlsCfg *tls.Config, remoteAddr, service string) error {
	remote, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", remoteAddr, tlsCfg)
	if err != nil {
		return err
	}
	defer remote.Close()
	rjc := newJSONConn(remote)
	if err := rjc.writeMessage(message{Type: "discover", Service: service}); err != nil {
		return err
	}
	msg, err := rjc.readMessage()
	if err != nil {
		return err
	}
	if msg.Type == "ok" {
		return nil
	}
	if msg.Error == errServiceNotExposed.Error() {
		return errServiceNotExposed
	}
	if msg.Error != "" {
		return errors.New(msg.Error)
	}
	return fmt.Errorf("server rejected discover")
}

func logListen(cfg ListenConfig, format string, args ...any) {
	if cfg.LogWriter == nil {
		return
	}
	line := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(line, "\n") {
		line += "\n"
	}
	_, _ = io.WriteString(cfg.LogWriter, line)
}

func listenOne(tlsCfg *tls.Config, cfg ListenConfig, local net.Conn) {
	remote, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", cfg.RemoteAddr, tlsCfg)
	if err != nil {
		_ = local.Close()
		return
	}
	rjc := newJSONConn(remote)
	if err := rjc.writeMessage(message{Type: "listen_stream", Service: cfg.Service}); err != nil {
		_ = local.Close()
		_ = remote.Close()
		return
	}
	msg, err := rjc.readMessage()
	if err != nil || msg.Type != "ok" {
		_ = local.Close()
		_ = remote.Close()
		return
	}
	pipePlainJSON(local, rjc)
}

func pipePlainJSON(plain net.Conn, jc *jsonConn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(plain, jc.r)
		if cw, ok := plain.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(jc.conn, plain)
		if cw, ok := jc.conn.(closeWriter); ok {
			_ = cw.CloseWrite()
		}
	}()
	wg.Wait()
	_ = plain.Close()
	_ = jc.conn.Close()
}
