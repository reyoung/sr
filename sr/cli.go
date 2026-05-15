package sr

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"
)

const defaultKeyPath = "client.key"

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return fmt.Errorf("missing command")
	}

	switch args[0] {
	case "gen-key":
		return runGenKey(args[1:], stdout)
	case "server":
		fs := flag.NewFlagSet("server", flag.ContinueOnError)
		fs.SetOutput(stderr)
		listen := fs.String("listen", "", "server listen address")
		key := fs.String("key", "server.key", "server key bundle")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *listen == "" {
			return fmt.Errorf("server requires --listen")
		}
		return RunServer(ctx, ServerConfig{ListenAddr: *listen, KeyPath: *key})
	case "local-forward-expose":
		fs := flag.NewFlagSet("local-forward-expose", flag.ContinueOnError)
		fs.SetOutput(stderr)
		service := fs.String("serv", "", "service name")
		key := fs.String("key", defaultKeyPath, "client key bundle")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *service == "" {
			return fmt.Errorf("local-forward-expose requires --serv")
		}
		if fs.NArg() != 2 {
			return fmt.Errorf("usage: sr local-forward-expose --serv NAME [--key client.key] local_addr remote_addr")
		}
		return RunExpose(ctx, ExposeConfig{Service: *service, LocalAddr: fs.Arg(0), RemoteAddr: fs.Arg(1), KeyPath: *key})
	case "local-forward-listen":
		fs := flag.NewFlagSet("local-forward-listen", flag.ContinueOnError)
		fs.SetOutput(stderr)
		service := fs.String("serv", "", "service name")
		listen := fs.String("listen", "", "local listen address")
		key := fs.String("key", defaultKeyPath, "client key bundle")
		retryInterval := fs.Duration("retry-interval", time.Second, "remote service discovery retry interval")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *service == "" {
			return fmt.Errorf("local-forward-listen requires --serv")
		}
		if *listen == "" {
			return fmt.Errorf("local-forward-listen requires --listen")
		}
		if fs.NArg() != 1 {
			return fmt.Errorf("usage: sr local-forward-listen --serv NAME --listen local_addr [--key client.key] [--retry-interval 1s] remote_addr")
		}
		return RunListen(ctx, ListenConfig{Service: *service, ListenAddr: *listen, RemoteAddr: fs.Arg(0), KeyPath: *key, RetryInterval: *retryInterval, LogWriter: stderr})
	default:
		usage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  sr gen-key --server > server.key")
	fmt.Fprintln(w, "  sr gen-key --client server.key --label USER_NAME > client.key")
	fmt.Fprintln(w, "  sr server --key server.key --listen remote_ip:remote_port")
	fmt.Fprintln(w, "  sr local-forward-expose --key client.key --serv name local_ip:local_port remote_ip:remote_port")
	fmt.Fprintln(w, "  sr local-forward-listen --key client.key --serv name --listen local_addr --retry-interval 1s remote_ip:remote_port")
}
