package interfaces

import "time"

// StateManager handles persistent state storage and synchronization tracking
type StateManager interface {
	Initialize(dbPath string) error
	UpdateHandbookSync(hashes map[string]string, timestamp time.Time) error
	UpdateServerState(status ServerStatus) error
	GetLastSync() (time.Time, error)
	GetHandbookHashes() (map[string]string, error)
	Close() error
}
