# sr

`sr` is a TCP reverse forwarding tool. A remote `sr server` accepts mutually
authenticated TLS connections from clients that expose local TCP services and
from clients that bind those exposed services to a local listener.

## Usage

Generate a server key bundle:

```sh
sr gen-key --server > server.key
```

Generate a client key bundle signed by the server bundle:

```sh
sr gen-key --client server.key --label USER_NAME > client.key
```

Run the remote server:

```sh
sr server --key server.key --listen ${remote_ip}:${remote_port}
```

Expose a local TCP service through the remote server:

```sh
sr local-forward-expose --key client.key --serv some-serv-name ${local_ip}:${local_port} ${remote_ip}:${remote_port}
```

Bind an exposed service to a local listener:

```sh
sr local-forward-listen --key client.key --serv some-serv-name --listen ${local_addr} --retry-interval 1s ${remote_ip}:${remote_port}
```

All commands are blocking. For a single `sr server` instance, each client key
can only have one active `local-forward-expose` process for a service name.
Different client keys can expose the same service name independently, and a
`local-forward-listen` process only discovers services exposed by the same
client key. `local-forward-listen` waits until the remote server can discover
the requested service before binding the local listener, which helps deployment
flows where the server endpoint is available before the exposing workload is
ready.
