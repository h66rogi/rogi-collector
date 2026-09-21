package model

import "time"

// Worker status constants.
const (
	WorkerStatusAlive    = "alive"
	WorkerStatusDraining = "draining"
	WorkerStatusDead     = "dead"
)

// Worker represents a chat-collecting worker instance.
type Worker struct {
	ID            string    `db:"id"`
	Status        string    `db:"status"`
	MaxCapacity   int       `db:"max_capacity"`
	RegisteredAt  time.Time `db:"registered_at"`
	LastHeartbeat time.Time `db:"last_heartbeat"`
}

// WorkerLoad represents real-time load metrics for a worker.
type WorkerLoad struct {
	WorkerID     string  `json:"workerId"`
	Connections  int     `json:"connections"`
	MsgPerSec    float64 `json:"msgPerSec"`
	MaxConn      int     `json:"maxConn"`
	MaxMsgPerSec float64 `json:"maxMsgPerSec"`
}

// Load returns the current load ratio as the max of connection ratio and
// message-rate ratio. A value of 1.0 means fully loaded.
func (w *WorkerLoad) Load() float64 {
	var connRatio, msgRatio float64

	if w.MaxConn > 0 {
		connRatio = float64(w.Connections) / float64(w.MaxConn)
	}
	if w.MaxMsgPerSec > 0 {
		msgRatio = w.MsgPerSec / w.MaxMsgPerSec
	}

	if connRatio > msgRatio {
		return connRatio
	}
	return msgRatio
}
