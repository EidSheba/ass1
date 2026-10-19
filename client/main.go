// Command client is the terminal chat client. It polls the server for new
// messages in a background goroutine while the main goroutine reads input.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/rpcapi"
)

const (
	pollInterval = 500 * time.Millisecond
	callTimeout  = 3 * time.Second
	maxPollFails = 3 // consecutive transport failures before giving up
	prompt       = "> "
)

// terminal serialises all output so the poller and the input loop never
// interleave writes. Incoming text clears the current line (\r + erase),
// prints, then redraws the prompt.
type terminal struct{ mu sync.Mutex }

func (t *terminal) printAsync(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Printf("\r\033[K%s\n%s", s, prompt)
}

func (t *terminal) printPrompt() {
	t.mu.Lock()
	defer t.mu.Unlock()
	fmt.Print(prompt)
}

// call performs an RPC with a timeout so a dead server can't hang the client.
func call(c *rpc.Client, method string, args, reply any) error {
	done := c.Go(rpcapi.ServiceName+"."+method, args, reply, make(chan *rpc.Call, 1)).Done
	select {
	case r := <-done:
		return r.Error
	case <-time.After(callTimeout):
		return fmt.Errorf("%s: timed out", method)
	}
}

func format(m rpcapi.Message) string {
	ts := m.Time.Format("15:04:05")
	if m.System {
		return fmt.Sprintf("[%s] * %s", ts, m.Text)
	}
	return fmt.Sprintf("[%s] %s: %s", ts, m.From, m.Text)
}

func main() {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (prompted if empty)")
	flag.Parse()

	in := bufio.NewScanner(os.Stdin)
	username := strings.TrimSpace(*name)
	if username == "" {
		fmt.Print("Username: ")
		if !in.Scan() {
			return
		}
		username = strings.TrimSpace(in.Text())
	}

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect:", err)
		os.Exit(1)
	}
	defer client.Close()

	if err := call(client, "Join", &rpcapi.JoinArgs{Username: username}, &rpcapi.Empty{}); err != nil {
		fmt.Fprintln(os.Stderr, "join failed:", err)
		os.Exit(1)
	}
	fmt.Printf("Joined as %q. Commands: /users, /quit. Anything else is sent as a message.\n", username)

	term := &terminal{}
	quit := make(chan string, 4) // reason to exit; first one wins
	stopPoll := make(chan struct{})
	var pollWG sync.WaitGroup

	// Poller goroutine.
	pollWG.Add(1)
	go func() {
		defer pollWG.Done()
		t := time.NewTicker(pollInterval)
		defer t.Stop()
		fails := 0
		for {
			select {
			case <-stopPoll:
				return
			case <-t.C:
			}
			var r rpcapi.PollReply
			if err := call(client, "Poll", &rpcapi.PollArgs{Username: username}, &r); err != nil {
				if strings.Contains(err.Error(), rpcapi.ErrUnknownUser) {
					quit <- "you were removed from the server"
					return
				}
				if fails++; fails >= maxPollFails {
					quit <- "lost connection to server: " + err.Error()
					return
				}
				continue
			}
			fails = 0
			for _, m := range r.Messages {
				term.printAsync(format(m))
			}
			// If the server announced shutdown, the connection will drop
			// soon; the failure counter above handles exiting.
		}
	}()

	// Input goroutine: lines -> channel (Scan blocks, so keep it separate).
	lines := make(chan string)
	go func() {
		for in.Scan() {
			lines <- in.Text()
		}
		close(lines) // EOF
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	leaveOnExit := true
	term.printPrompt()
loop:
	for {
		select {
		case <-sig:
			break loop
		case reason := <-quit:
			fmt.Printf("\r\033[K%s\n", reason)
			leaveOnExit = false
			break loop
		case line, ok := <-lines:
			if !ok {
				break loop
			}
			line = strings.TrimSpace(line)
			switch {
			case line == "":
			case line == "/quit":
				break loop
			case line == "/users":
				var r rpcapi.ListReply
				if err := call(client, "ListUsers", &rpcapi.Empty{}, &r); err != nil {
					term.printAsync("error: " + err.Error())
				} else {
					term.printAsync(fmt.Sprintf("Online (%d): %s", len(r.Users), strings.Join(r.Users, ", ")))
				}
				continue
			case strings.HasPrefix(line, "/"):
				term.printAsync("unknown command (try /users, /quit)")
				continue
			default:
				if err := call(client, "SendMessage", &rpcapi.SendArgs{Username: username, Text: line}, &rpcapi.Empty{}); err != nil {
					term.printAsync("send failed: " + err.Error())
					continue
				}
			}
			term.printPrompt()
		}
	}

	close(stopPoll)
	pollWG.Wait()
	if leaveOnExit {
		if err := call(client, "Leave", &rpcapi.LeaveArgs{Username: username}, &rpcapi.Empty{}); err != nil {
			fmt.Fprintln(os.Stderr, "\nleave:", err)
		}
	}
	fmt.Println("\nGoodbye.")
}
