package state

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
)

// Manager implements the StateManager interface
type Manager struct {
	db *sql.DB
}

// NewManager creates a new state manager instance
func NewManager() *Manager {
	return &Manager{}
}

// Initialize creates and initializes the SQLite database with the required schema
func (m *Manager) Initialize(dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return errors.NewGenericError("failed to open database", err)
	}

	m.db = db

	// Test the database connection to detect corruption early
	if err := m.db.Ping(); err != nil {
		m.db.Close()
		return errors.NewGenericError("database file is corrupted or inaccessible", err)
	}

	// Create the schema
	schema := `
	CREATE TABLE IF NOT EXISTS handbook_files (
		path TEXT PRIMARY KEY,
		hash TEXT NOT NULL,
		last_synced TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS server_state (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at TIMESTAMP NOT NULL
	);

	CREATE TABLE IF NOT EXISTS sync_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		operation TEXT NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		status TEXT NOT NULL,
		details TEXT
	);
	`

	if _, err := m.db.Exec(schema); err != nil {
		m.db.Close()
		// Check if this is a corruption error
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted and cannot be initialized", err)
		}
		return errors.NewGenericError("failed to create database schema", err)
	}

	return nil
}

// isCorruptionError checks if an error indicates database corruption
func isCorruptionError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	// Common SQLite corruption error messages
	return contains(errMsg, "database disk image is malformed") ||
		contains(errMsg, "file is not a database") ||
		contains(errMsg, "database is locked") ||
		contains(errMsg, "database corruption")
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// UpdateHandbookSync stores file hashes and timestamps for handbook synchronization
func (m *Manager) UpdateHandbookSync(hashes map[string]string, timestamp time.Time) error {
	if m.db == nil {
		return errors.NewGenericError("database not initialized", nil)
	}

	tx, err := m.db.Begin()
	if err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to begin transaction", err)
	}
	defer tx.Rollback()

	// Clear existing handbook files
	if _, err := tx.Exec("DELETE FROM handbook_files"); err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to clear handbook files", err)
	}

	// Insert new file hashes
	stmt, err := tx.Prepare("INSERT INTO handbook_files (path, hash, last_synced) VALUES (?, ?, ?)")
	if err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to prepare statement", err)
	}
	defer stmt.Close()

	for path, hash := range hashes {
		if _, err := stmt.Exec(path, hash, timestamp); err != nil {
			if isCorruptionError(err) {
				return errors.NewGenericError("database file is corrupted", err)
			}
			return errors.NewGenericError(fmt.Sprintf("failed to insert file hash for %s", path), err)
		}
	}

	// Record sync history
	if _, err := tx.Exec(
		"INSERT INTO sync_history (operation, timestamp, status, details) VALUES (?, ?, ?, ?)",
		"handbook_sync",
		timestamp,
		"success",
		fmt.Sprintf("Synced %d files", len(hashes)),
	); err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to record sync history", err)
	}

	if err := tx.Commit(); err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to commit transaction", err)
	}

	return nil
}

// UpdateServerState tracks the current server status
func (m *Manager) UpdateServerState(status interfaces.ServerStatus) error {
	if m.db == nil {
		return errors.NewGenericError("database not initialized", nil)
	}

	tx, err := m.db.Begin()
	if err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to begin transaction", err)
	}
	defer tx.Rollback()

	timestamp := time.Now()

	// Update server state
	updates := map[string]string{
		"running":        fmt.Sprintf("%t", status.Running),
		"healthy":        fmt.Sprintf("%t", status.Healthy),
		"container_name": status.ContainerName,
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO server_state (key, value, updated_at) VALUES (?, ?, ?)")
	if err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to prepare statement", err)
	}
	defer stmt.Close()

	for key, value := range updates {
		if _, err := stmt.Exec(key, value, timestamp); err != nil {
			if isCorruptionError(err) {
				return errors.NewGenericError("database file is corrupted", err)
			}
			return errors.NewGenericError(fmt.Sprintf("failed to update server state for %s", key), err)
		}
	}

	if err := tx.Commit(); err != nil {
		if isCorruptionError(err) {
			return errors.NewGenericError("database file is corrupted", err)
		}
		return errors.NewGenericError("failed to commit transaction", err)
	}

	return nil
}

// GetLastSync retrieves the timestamp of the last successful handbook synchronization
func (m *Manager) GetLastSync() (time.Time, error) {
	if m.db == nil {
		return time.Time{}, errors.NewGenericError("database not initialized", nil)
	}

	var timestampStr sql.NullString
	err := m.db.QueryRow(`
		SELECT MAX(last_synced) FROM handbook_files
	`).Scan(&timestampStr)

	if err == sql.ErrNoRows || !timestampStr.Valid {
		return time.Time{}, nil
	}

	if err != nil {
		if isCorruptionError(err) {
			return time.Time{}, errors.NewGenericError("database file is corrupted", err)
		}
		return time.Time{}, errors.NewGenericError("failed to get last sync timestamp", err)
	}

	// Try multiple timestamp formats that SQLite might use
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
		time.DateTime,
	}

	var timestamp time.Time
	var parseErr error
	for _, format := range formats {
		timestamp, parseErr = time.Parse(format, timestampStr.String)
		if parseErr == nil {
			return timestamp, nil
		}
	}

	return time.Time{}, errors.NewGenericError("failed to parse timestamp", parseErr)
}

// GetHandbookHashes retrieves the stored file hashes from the last sync
func (m *Manager) GetHandbookHashes() (map[string]string, error) {
	if m.db == nil {
		return nil, errors.NewGenericError("database not initialized", nil)
	}

	rows, err := m.db.Query("SELECT path, hash FROM handbook_files")
	if err != nil {
		if isCorruptionError(err) {
			return nil, errors.NewGenericError("database file is corrupted", err)
		}
		return nil, errors.NewGenericError("failed to query handbook files", err)
	}
	defer rows.Close()

	hashes := make(map[string]string)
	for rows.Next() {
		var path, hash string
		if err := rows.Scan(&path, &hash); err != nil {
			if isCorruptionError(err) {
				return nil, errors.NewGenericError("database file is corrupted", err)
			}
			return nil, errors.NewGenericError("failed to scan handbook file row", err)
		}
		hashes[path] = hash
	}

	if err := rows.Err(); err != nil {
		if isCorruptionError(err) {
			return nil, errors.NewGenericError("database file is corrupted", err)
		}
		return nil, errors.NewGenericError("failed to iterate handbook files", err)
	}

	return hashes, nil
}

// Close closes the database connection
func (m *Manager) Close() error {
	if m.db == nil {
		return nil
	}

	if err := m.db.Close(); err != nil {
		return errors.NewGenericError("failed to close database", err)
	}

	m.db = nil
	return nil
}
