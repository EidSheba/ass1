# Concurrent RPC Chat Room (Go, net/rpc, polling)

A terminal chat with a central server and any number of clients, built on the
standard library only (`net/rpc` over TCP).

```
rpcapi/        shared types: Message, RPC args/replies, service name
server/main.go server program
client/main.go client program
```

## Run

```
go run ./server                       # listens on localhost:1234
go run ./server -addr :9000           # custom address

go run ./client                       # prompts for a username
go run ./client -addr host:9000 -name alice
```

Client commands: plain text sends a message, `/users` lists who is online,
`/quit` leaves and exits. Ctrl+C does the same as `/quit`.

Race check (requires cgo / a C compiler such as gcc): `go run -race ./server`
and `go run -race ./client`.

## Design

**Server state.** `Chat` holds `users map[string]*user` behind one
`sync.Mutex`. Each `user` has a queue of pending `Message`s and a `lastPoll`
timestamp. Every RPC method takes the lock, does a short in-memory operation
and releases it; no I/O happens under the lock.

**RPC methods:** `Join`, `Leave`, `SendMessage`, `ListUsers`, `Poll`.
- `Join` rejects empty, over-long, whitespace-containing and duplicate names.
- `SendMessage` appends to every queue except the sender's.
- Join/leave/timeout/shutdown produce *system* messages queued for everyone else.
- `Poll` returns the queue and clears it atomically.

**Concurrency.** The accept loop starts a goroutine per connection running
`rpc.ServeConn`; `net/rpc` additionally dispatches each request in its own
goroutine, so the mutex is what keeps state consistent.

**Failure handling.** A reaper goroutine runs every second and removes any user
whose last Join/Poll/Send is older than 10 s, notifying the others. A client
that sees `unknown user` on Poll knows it was removed and exits. A client that
fails to reach the server 3 polls in a row exits with a message. All client
RPCs have a 3 s timeout.

**Shutdown.** Server: on Ctrl+C it queues "server is shutting down" for all
users, waits 1 s so clients can poll it, closes the listener, closes open
connections, waits for connection goroutines and clears state. Client: on
Ctrl+C, `/quit` or EOF it stops the poller, calls `Leave`, and exits.

**Client display.** A poller goroutine calls `Poll` every 500 ms. All output goes
through a mutex-guarded printer which erases the current line (`\r` + ANSI
erase), prints the message, and redraws the `> ` prompt. Limitation: in a
plain line-buffered terminal, characters you are halfway through typing stay
in the OS input buffer and are submitted correctly, but they are not redrawn
after an incoming message. Fixing that needs raw terminal mode.
ANSI escapes need Windows Terminal / VS Code terminal / any Unix terminal.

### Why polling?

With `net/rpc` the server can only answer requests; it cannot push to a
client. Polling keeps the protocol to plain request/response RPCs, needs no
extra connections or streaming, and doubles as a heartbeat (a client that
stops polling is dead). Costs: up to ~500 ms latency and constant small
traffic (2 requests/s/client). A push design (WebSockets, gRPC streams, or a
client-side RPC server) would be lower latency but more complex.

## Demo script

1. Terminal 1: `go run ./server`
2. Terminal 2: `go run ./client -name alice`
3. Terminal 3: `go run ./client -name bob`
   - alice sees `* bob joined the chat`
4. In bob: `hello alice` → appears in alice's terminal (not echoed to bob).
5. In alice: `/users` → `Online (2): alice, bob`
6. Terminal 4: `go run ./client -name alice` → `join failed: username "alice" is already taken`
7. In bob: `/quit` → alice sees `* bob left the chat`.
8. Timeout: start carol, then kill her process hard (close the terminal /
   `kill -9`). Within ~10 s alice sees `* carol disconnected (timed out)`.
9. Ctrl+C the server → clients print `* server is shutting down`, then exit
   with `lost connection to server`.
