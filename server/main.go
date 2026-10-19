// Command server runs the chat room RPC server.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/rpcapi"
)

const (
	// pollTimeout: a user who has not polled for this long is presumed dead.
	pollTimeout = 10 * time.Second
	// reapInterval: how often the reaper looks for dead users.
	reapInterval = 1 * time.Second
	// shutdownGrace: after announcing shutdown, wait this long so clients
	// get one more chance to Poll and see the notice.
	shutdownGrace = 1 * time.Second
	maxNameLen    = 32
	maxTextLen    = 2000
)

// user is the per-client state kept by the server.
type user struct {
	queue    []rpcapi.Message // pending messages, cleared by Poll
	lastPoll time.Time        // last Join/Poll activity, used for liveness
}

// Chat is the RPC receiver. All fields are guarded by mu.
type Chat struct {
	mu    sync.Mutex
	users map[string]*user
}

func NewChat() *Chat { return &Chat{users: make(map[string]*user)} }

// broadcastLocked appends msg to every user's queue except `except`.
// The caller must hold c.mu.
func (c *Chat) broadcastLocked(msg rpcapi.Message, except string) {
	for name, u := range c.users {
		if name != except {
			u.queue = append(u.queue, msg)
		}
	}
}

func (c *Chat) systemLocked(text, except string) {
	c.broadcastLocked(rpcapi.Message{Text: text, Time: time.Now(), System: true}, except)
}

// Join registers a new user. Empty/duplicate/invalid names are rejected.
func (c *Chat) Join(args *rpcapi.JoinArgs, _ *rpcapi.Empty) error {
	name := args.Username
	switch {
	case strings.TrimSpace(name) == "":
		return errors.New("username must not be empty")
	case len(name) > maxNameLen:
		return fmt.Errorf("username too long (max %d bytes)", maxNameLen)
	case strings.ContainsAny(name, " \t\r\n"):
		return errors.New("username must not contain whitespace")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, taken := c.users[name]; taken {
		return fmt.Errorf("username %q is already taken", name)
	}
	c.users[name] = &user{lastPoll: time.Now()}
	c.systemLocked(name+" joined the chat", name)
	log.Printf("join: %s (%d online)", name, len(c.users))
	return nil
}

// removeLocked deletes a user and notifies the rest. Caller holds c.mu.
func (c *Chat) removeLocked(name, reason string) bool {
	if _, ok := c.users[name]; !ok {
		return false
	}
	delete(c.users, name)
	c.systemLocked(name+" "+reason, name)
	log.Printf("remove: %s (%s, %d online)", name, reason, len(c.users))
	return true
}

// Leave removes a user voluntarily.
func (c *Chat) Leave(args *rpcapi.LeaveArgs, _ *rpcapi.Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.removeLocked(args.Username, "left the chat") {
		return errors.New(rpcapi.ErrUnknownUser)
	}
	return nil
}

// SendMessage queues a message for everyone except the sender.
func (c *Chat) SendMessage(args *rpcapi.SendArgs, _ *rpcapi.Empty) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return errors.New("message must not be empty")
	}
	if len(text) > maxTextLen {
		return fmt.Errorf("message too long (max %d bytes)", maxTextLen)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[args.Username]
	if !ok {
		return errors.New(rpcapi.ErrUnknownUser)
	}
	u.lastPoll = time.Now() // sending also proves the client is alive
	c.broadcastLocked(rpcapi.Message{From: args.Username, Text: text, Time: time.Now()}, args.Username)
	return nil
}

// ListUsers returns the sorted names of connected users.
func (c *Chat) ListUsers(_ *rpcapi.Empty, reply *rpcapi.ListReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Users = make([]string, 0, len(c.users))
	for name := range c.users {
		reply.Users = append(reply.Users, name)
	}
	sort.Strings(reply.Users)
	return nil
}

// Poll returns and clears the user's pending messages and refreshes liveness.
func (c *Chat) Poll(args *rpcapi.PollArgs, reply *rpcapi.PollReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	u, ok := c.users[args.Username]
	if !ok {
		return errors.New(rpcapi.ErrUnknownUser)
	}
	u.lastPoll = time.Now()
	reply.Messages = u.queue
	u.queue = nil
	return nil
}

// reap removes users that have not polled within pollTimeout.
func (c *Chat) reap(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, u := range c.users {
		if now.Sub(u.lastPoll) > pollTimeout {
			c.removeLocked(name, "disconnected (timed out)")
		}
	}
}

// reaper runs reap periodically until stop is closed.
func (c *Chat) reaper(stop <-chan struct{}) {
	t := time.NewTicker(reapInterval)
	defer t.Stop()
	for {
		select {
		case now := <-t.C:
			c.reap(now)
		case <-stop:
			return
		}
	}
}

// connSet tracks open connections so shutdown can close them.
type connSet struct {
	mu sync.Mutex
	m  map[net.Conn]struct{}
}

func (s *connSet) add(c net.Conn) { s.mu.Lock(); s.m[c] = struct{}{}; s.mu.Unlock() }
func (s *connSet) del(c net.Conn) { s.mu.Lock(); delete(s.m, c); s.mu.Unlock() }
func (s *connSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.m {
		c.Close()
	}
}

func main() {
	addr := flag.String("addr", "localhost:1234", "address to listen on")
	flag.Parse()

	chat := NewChat()
	srv := rpc.NewServer()
	if err := srv.RegisterName(rpcapi.ServiceName, chat); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s", ln.Addr())

	stop := make(chan struct{})
	go chat.reaper(stop)

	conns := &connSet{m: make(map[net.Conn]struct{})}
	var wg sync.WaitGroup // tracks per-connection goroutines

	// Accept loop: one goroutine per connection.
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				log.Printf("accept: %v", err)
				continue
			}
			conns.add(conn)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conns.del(conn)
				srv.ServeConn(conn) // returns when the client disconnects
				conn.Close()
			}()
		}
	}()

	// Graceful shutdown on Ctrl+C / SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")

	chat.mu.Lock()
	chat.systemLocked("server is shutting down", "")
	chat.mu.Unlock()
	time.Sleep(shutdownGrace) // let clients poll the notice

	close(stop)
	ln.Close() // stop accepting
	<-acceptDone
	conns.closeAll() // unblock ServeConn goroutines
	wg.Wait()

	chat.mu.Lock()
	chat.users = make(map[string]*user)
	chat.mu.Unlock()
	log.Println("server stopped")
}
