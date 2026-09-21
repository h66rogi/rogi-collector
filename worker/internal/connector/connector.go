package connector

import (
	"context"

	"github.com/h66rogi/rogi-collector/shared/model"
)

// PlatformConnector defines the interface for connecting to a live streaming
// platform's chat and receiving messages.
type PlatformConnector interface {
	// Connect establishes a connection to the platform's chat for the given channel.
	// It blocks until the connection is established or an error occurs.
	Connect(ctx context.Context, channel model.LiveChannel) error

	// Disconnect gracefully closes the connection.
	Disconnect() error

	// IsAlive returns true if the connection is still active.
	IsAlive() bool

	// Messages returns a read-only channel that delivers parsed chat messages.
	Messages() <-chan model.ChatMessage

	// Errors returns a read-only channel that delivers connection errors.
	// A value on this channel typically means the connection has been lost.
	Errors() <-chan error
}
