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
sr local-forward-listen --key client.key --serv some-serv-name --listen ${local_addr} ${remote_ip}:${remote_port}
```

All commands are blocking. For a single `sr server` instance, a service name can
only have one active `local-forward-expose` process.
