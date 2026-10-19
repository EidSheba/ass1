// Package rpcapi holds the types shared by the chat server and client:
// the RPC argument/reply structs and the Message type.
package rpcapi

import "time"

// ServiceName is the name the server registers its RPC receiver under.
// Clients call methods as ServiceName + ".Join", etc.
const ServiceName = "Chat"

// Message is one item delivered to a client by Poll.
type Message struct {
	From   string    // sender username; empty for system notifications
	Text   string    // message body
	Time   time.Time // when the server created the message
	System bool      // true for join/leave/shutdown notifications
}

// JoinArgs is the argument of Chat.Join.
type JoinArgs struct{ Username string }

// LeaveArgs is the argument of Chat.Leave.
type LeaveArgs struct{ Username string }

// SendArgs is the argument of Chat.SendMessage.
type SendArgs struct {
	Username string
	Text     string
}

// PollArgs is the argument of Chat.Poll.
type PollArgs struct{ Username string }

// PollReply carries the messages queued for the polling user.
type PollReply struct{ Messages []Message }

// ListReply carries the sorted list of connected usernames.
type ListReply struct{ Users []string }

// Empty is used for methods with no meaningful argument or reply.
type Empty struct{}

// Error strings returned by the server; clients compare against these
// to detect that they were removed (e.g. after a poll timeout).
const (
	ErrUnknownUser = "unknown user (not joined, or removed for inactivity)"
)
