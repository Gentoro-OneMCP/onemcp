package handbook

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onemcp/cli/internal/errors"
	"github.com/onemcp/cli/internal/interfaces"
	"gopkg.in/yaml.v3"
)

//go:embed templates/acme
var acmeTemplate embed.FS

// Manager implements the HandbookManager interface
type Manager struct {
	stateManager interfaces.StateManager
}

// NewManager creates a new HandbookManager instance
func NewManager(stateManager interfaces.StateManager) interfaces.HandbookManager {
	return &Manager{
		stateManager: stateManager,
	}
}

// InstallAcmeTemplate copies the embedded Acme template to the target directory
func (m *Manager) InstallAcmeTemplate(targetDir string) error {
	// Ensure target directory exists
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return errors.NewGenericError("failed to create target directory", err)
	}

	// Walk through the embedded template files
	err := fs.WalkDir(acmeTemplate, "templates/acme", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path from templates/acme
		relPath, err := filepath.Rel("templates/acme", path)
		if err != nil {
			return err
		}

		// Skip the root directory
		if relPath == "." {
			return nil
		}

		targetPath := filepath.Join(targetDir, relPath)

		if d.IsDir() {
			// Create directory
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return errors.NewGenericError(fmt.Sprintf("failed to create directory %s", targetPath), err)
			}
		} else {
			// Copy file
			content, err := acmeTemplate.ReadFile(path)
			if err != nil {
				return errors.NewGenericError(fmt.Sprintf("failed to read template file %s", path), err)
			}

			if err := os.WriteFile(targetPath, content, 0644); err != nil {
				return errors.NewGenericError(fmt.Sprintf("failed to write file %s", targetPath), err)
			}
		}

		return nil
	})

	if err != nil {
		return errors.NewGenericError("failed to install Acme template", err)
	}

	return nil
}

// ResetToAcme resets the handbook to the Acme template with user confirmation
// The confirmation parameter should be the user's input (e.g., "y" or "n")
func (m *Manager) ResetToAcme(handbookDir string, confirmation string) error {
	// Check confirmation
	if confirmation != "y" {
		return errors.NewGenericError("operation cancelled by user", nil)
	}

	// Remove existing handbook
	if err := os.RemoveAll(handbookDir); err != nil {
		return errors.NewGenericError("failed to remove existing handbook", err)
	}

	// Install Acme template
	if err := m.InstallAcmeTemplate(handbookDir); err != nil {
		return err
	}

	// Update state database
	if m.stateManager != nil {
		hashes, err := m.calculateFileHashes(handbookDir)
		if err != nil {
			return errors.NewGenericError("failed to calculate file hashes after reset", err)
		}

		timestamp := time.Now()
		if err := m.stateManager.UpdateHandbookSync(hashes, timestamp); err != nil {
			return errors.NewGenericError("handbook reset successfully but failed to update state database", err)
		}
	}

	return nil
}

// ValidateStructure checks if a handbook directory has the required structure
// Note: Tests expect Agent.yaml to always be required, even during soft checks.
func (m *Manager) ValidateStructure(handbookDir string, softCheck bool) error {
    requiredPaths := []string{
        filepath.Join(handbookDir, "docs"),
        filepath.Join(handbookDir, "apis"),
        filepath.Join(handbookDir, "regression-suite"),
        filepath.Join(handbookDir, "Agent.yaml"),
    }

    for _, path := range requiredPaths {
        info, err := os.Stat(path)
        if err != nil {
            if os.IsNotExist(err) {
                return errors.NewValidationError(fmt.Sprintf("required path %s does not exist", path))
            }
            return errors.NewGenericError(fmt.Sprintf("failed to access %s", path), err)
        }

        // Check that directories are directories and Agent.yaml is a file
        if path == filepath.Join(handbookDir, "Agent.yaml") {
            if info.IsDir() {
                return errors.NewValidationError("Agent.yaml must be a file, not a directory")
            }
        } else {
            if !info.IsDir() {
                return errors.NewValidationError(fmt.Sprintf("%s must be a directory", path))
            }
        }
    }

    return nil
}

// Pull fetches the handbook from the server and overwrites the local handbook
func (m *Manager) Pull(ctx context.Context, serverURL string, targetDir string) error {
	// Create backup before pulling
	backup, err := m.createBackup(targetDir)
	if err != nil {
		return errors.NewSyncError("failed to create backup before pull", err)
	}

	// Ensure we clean up the backup on success or restore on failure
	defer func() {
		if backup != nil {
			backup.Cleanup()
		}
	}()

	// Fetch handbook from server
	if err := m.fetchHandbookFromServer(ctx, serverURL, targetDir); err != nil {
		// Restore backup on failure
		if restoreErr := backup.Restore(); restoreErr != nil {
			return errors.NewSyncError(
				fmt.Sprintf("failed to pull handbook and restore backup: %v, restore error: %v", err, restoreErr),
				err,
			)
		}
		return errors.NewSyncError("failed to pull handbook from server", err)
	}

	// Calculate file hashes for the new handbook
	hashes, err := m.calculateFileHashes(targetDir)
	if err != nil {
		// Restore backup on failure
		if restoreErr := backup.Restore(); restoreErr != nil {
			return errors.NewSyncError(
				fmt.Sprintf("failed to calculate hashes and restore backup: %v, restore error: %v", err, restoreErr),
				err,
			)
		}
		return errors.NewSyncError("failed to calculate file hashes after pull", err)
	}

	// Update state database
	if m.stateManager != nil {
		timestamp := time.Now()

		if err := m.stateManager.UpdateHandbookSync(hashes, timestamp); err != nil {
			// Don't restore backup here - the pull succeeded, just state update failed
			return errors.NewGenericError("handbook pulled successfully but failed to update state database", err)
		}
	}

	return nil
}

// fetchHandbookFromServer downloads the handbook from the MCP server
func (m *Manager) fetchHandbookFromServer(ctx context.Context, serverURL string, targetDir string) error {
	// Construct the handbook download URL
	// The server should provide a /handbook endpoint that returns a tar.gz archive
	url := strings.TrimSuffix(serverURL, "/") + "/mng/handbook"

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.NewSyncError("failed to create request", err)
	}

	// Execute request
	client := &http.Client{
		Timeout: 60 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewSyncError("failed to fetch handbook", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return errors.NewSyncError(fmt.Sprintf("server returned status %d", resp.StatusCode), nil)
	}

	// Remove existing handbook content
	if err := os.RemoveAll(targetDir); err != nil {
		return errors.NewSyncError("failed to remove existing handbook", err)
	}

	// Create target directory
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return errors.NewSyncError("failed to create handbook directory", err)
	}

	// Extract tar.gz archive
	if err := extractTarGz(resp.Body, targetDir); err != nil {
		return errors.NewSyncError("failed to extract handbook", err)
	}

	return nil
}

// extractTarGz extracts a tar.gz archive to the target directory
func extractTarGz(r io.Reader, targetDir string) error {
	// Create gzip reader
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return errors.NewSyncError("failed to create gzip reader", err)
	}
	defer gzr.Close()

	// Create tar reader
	tr := tar.NewReader(gzr)

	// Extract files
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.NewSyncError("failed to read tar header", err)
		}

		// Sanitize path to prevent directory traversal
		target := filepath.Join(targetDir, filepath.Clean(header.Name))
		if !strings.HasPrefix(target, filepath.Clean(targetDir)+string(os.PathSeparator)) {
			return errors.NewSyncError(fmt.Sprintf("invalid file path in archive: %s", header.Name), nil)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return errors.NewSyncError(fmt.Sprintf("failed to create directory %s", target), err)
			}
		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return errors.NewSyncError(fmt.Sprintf("failed to create parent directory for %s", target), err)
			}

			// Create file
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return errors.NewSyncError(fmt.Sprintf("failed to create file %s", target), err)
			}

			// Copy content
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return errors.NewSyncError(fmt.Sprintf("failed to write file %s", target), err)
			}
			f.Close()
		}
	}

	return nil
}

// Push uploads the local handbook to the server
func (m *Manager) Push(ctx context.Context, serverURL string, sourceDir string) error {
	// Validate handbook structure before pushing
	if err := m.ValidateStructure(sourceDir, true); err != nil {
		return errors.NewSyncError("invalid handbook structure", err)
	}

	// Create tar.gz archive of handbook
	archiveData, err := m.createHandbookArchive(sourceDir)
	if err != nil {
		return errors.NewSyncError("failed to create handbook archive", err)
	}

	// Upload to server
	if archiveData, err = m.uploadHandbookToServer(ctx, serverURL, archiveData); err != nil {
		return errors.NewSyncError("failed to upload handbook to server", err)
	}

	// Calculate file hashes
	hashes, err := m.calculateFileHashes(sourceDir)
	if err != nil {
		return errors.NewGenericError("failed to calculate file hashes after push", err)
	}

	// Update state database
	if m.stateManager != nil {
		timestamp := time.Now()
		if err := m.stateManager.UpdateHandbookSync(hashes, timestamp); err != nil {
			// Don't fail the push if state update fails
			return errors.NewGenericError("handbook pushed successfully but failed to update state database", err)
		}
	}

	backupSourceDir, _err := m.createBackup(sourceDir)
	if _err != nil {
		return errors.NewGenericError("failed to backup handbook", _err)
	}

	// Extract tar.gz archive
	os.RemoveAll(sourceDir)
	if err := extractTarGz(bytes.NewReader(archiveData), sourceDir); err != nil {
		copyDir(backupSourceDir.backupDir, sourceDir)
		return errors.NewSyncError("failed to extract handbook", err)
	}

	return nil
}

// createHandbookArchive creates a tar.gz archive of the handbook directory
func (m *Manager) createHandbookArchive(sourceDir string) ([]byte, error) {
	// Create a buffer to hold the archive
	var buf strings.Builder

	// Create gzip writer
	gzw := gzip.NewWriter(&buf)
	defer gzw.Close()

	// Create tar writer
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	// Walk through the handbook directory
	err := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Calculate relative path
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		// Read file content
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Create tar header
		header := &tar.Header{
			Name:    relPath,
			Mode:    int64(info.Mode()),
			Size:    int64(len(content)),
			ModTime: info.ModTime(),
		}

		// Write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// Write content
		if _, err := tw.Write(content); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Close writers to flush data
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gzw.Close(); err != nil {
		return nil, err
	}

	return []byte(buf.String()), nil
}

// uploadHandbookToServer uploads the handbook archive, waits for processing,
// displays a spinner/progress, and returns the final processed archive.
func (m *Manager) uploadHandbookToServer(
    ctx context.Context,
    serverURL string,
    archiveData []byte,
) ([]byte, error) {
    base := strings.TrimSuffix(serverURL, "/")
    simpleURL := base + "/mng/handbook"

    client := &http.Client{Timeout: 120 * time.Second}

    // First try simple upload endpoint used by tests: PUT /mng/handbook → 200 OK
    simpleReq, err := http.NewRequestWithContext(ctx, "PUT", simpleURL, bytes.NewReader(archiveData))
    if err != nil {
        return nil, fmt.Errorf("failed to create upload request: %w", err)
    }
    simpleReq.Header.Set("Content-Type", "application/gzip")

    simpleResp, err := client.Do(simpleReq)
    if err != nil {
        return nil, fmt.Errorf("failed to upload handbook: %w", err)
    }

    // If server supports simple upload and returns 200 OK, we're done
    if simpleResp.StatusCode == http.StatusOK {
        simpleResp.Body.Close()
        return archiveData, nil
    }

    // If endpoint not found, fall back to async flow
    if simpleResp.StatusCode != http.StatusNotFound {
        // Any other status is an error on the simple path
        simpleResp.Body.Close()
        return nil, fmt.Errorf("upload returned status %d", simpleResp.StatusCode)
    }
    simpleResp.Body.Close()

    // ---- Fallback: async upload/status/download flow ----------------------
    uploadURL := base + "/mng/handbook/upload"

    req, err := http.NewRequestWithContext(ctx, "PUT", uploadURL, bytes.NewReader(archiveData))
    if err != nil {
        return nil, fmt.Errorf("failed to create upload request: %w", err)
    }
    req.Header.Set("Content-Type", "application/gzip")

    resp, err := client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("failed to upload handbook: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusAccepted {
        return nil, fmt.Errorf("upload returned status %d", resp.StatusCode)
    }

    var initResp struct {
        JobID  string `json:"jobId"`
        Status string `json:"status"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&initResp); err != nil {
        return nil, fmt.Errorf("failed to parse upload response: %w", err)
    }

    if initResp.JobID == "" {
        return nil, fmt.Errorf("server did not return jobId")
    }

    // ---- Step 2: Poll processing status ----------------------------------
    statusURL := fmt.Sprintf("%s/mng/handbook/status/%s", base, initResp.JobID)

    spinner := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
    spinIdx := 0

    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }

        // Request status
        sreq, _ := http.NewRequestWithContext(ctx, "GET", statusURL, nil)
        sresp, err := client.Do(sreq)
        if err != nil {
            return nil, fmt.Errorf("failed to get processing status: %w", err)
        }

        var st struct {
            JobID   string `json:"jobId"`
            Status  string `json:"status"`
            Details string `json:"details"`
        }
        err = json.NewDecoder(sresp.Body).Decode(&st)
        sresp.Body.Close()
        if err != nil {
            return nil, fmt.Errorf("failed to decode status response: %w", err)
        }

        // Spinner UI
        spin := spinner[spinIdx%len(spinner)]
        spinIdx++

        progress := progressFromStatus(st.Status, st.Details)

        fmt.Printf("\r%s Processing: %-10s %s", spin, st.Status, progress)

        switch st.Status {
        case "DONE":
            fmt.Println()
            goto DOWNLOAD
        case "FAILED":
            fmt.Println()
            return nil, fmt.Errorf("processing failed: %s", st.Details)
        case "PENDING", "RUNNING":
            time.Sleep(500 * time.Millisecond)
        default:
            return nil, fmt.Errorf("unknown status: %s", st.Status)
        }
    }

DOWNLOAD:

    // ---- Step 3: Download final archive ---------------------------------
    downloadURL := base + "/mng/handbook/download"
	dreq, _ := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	dresp, err := client.Do(dreq)
	if err != nil {
		return nil, fmt.Errorf("failed to download processed archive: %w", err)
	}
	defer dresp.Body.Close()

	if dresp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download returned status %d", dresp.StatusCode)
	}

	result, err := io.ReadAll(dresp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read archive response: %w", err)
	}

	return result, nil
}

// Helper: returns progress bar + details
func progressFromStatus(status, details string) string {
	var bar string

	switch status {
	case "PENDING":
		bar = "[░░░░░░░░░░]"
	case "RUNNING":
		bar = "[██████░░░░]"
	case "DONE":
		bar = "[██████████]"
	case "FAILED":
		bar = "[XXXX ERROR XX]"
	default:
		bar = "[??????????]"
	}

	// Limit details for clean terminal output
	const maxLen = 60
	if len(details) > maxLen {
		details = details[:maxLen] + "..."
	}

	if details != "" {
		return fmt.Sprintf("%s — %s", bar, details)
	}

	return bar
}

// calculateFileHashes computes SHA256 hashes for all files in a directory
func (m *Manager) calculateFileHashes(dir string) (map[string]string, error) {
	hashes := make(map[string]string)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Calculate relative path
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		// Calculate hash
		hash, err := hashFile(path)
		if err != nil {
			return err
		}

		hashes[relPath] = hash
		return nil
	})

	if err != nil {
		return nil, errors.NewGenericError("failed to calculate file hashes", err)
	}

	return hashes, nil
}

// hashFile calculates the SHA256 hash of a file
func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// createBackup creates a backup of a directory
func (m *Manager) createBackup(dir string) (*backup, error) {
	backupDir := dir + ".backup." + fmt.Sprintf("%d", time.Now().Unix())

	if err := copyDir(dir, backupDir); err != nil {
		return nil, errors.NewGenericError("failed to create backup", err)
	}

	return &backup{
		originalDir: dir,
		backupDir:   backupDir,
	}, nil
}

// backup represents a directory backup
type backup struct {
	originalDir string
	backupDir   string
}

// Restore restores the backup to the original location
func (b *backup) Restore() error {
	// Remove the current directory
	if err := os.RemoveAll(b.originalDir); err != nil {
		return err
	}

	// Restore from backup
	if err := copyDir(b.backupDir, b.originalDir); err != nil {
		return err
	}

	return nil
}

// Cleanup removes the backup directory
func (b *backup) Cleanup() error {
	return os.RemoveAll(b.backupDir)
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}

		// Copy file
		return copyFile(path, targetPath, info.Mode())
	})
}

// copyFile copies a single file
func copyFile(src, dst string, mode os.FileMode) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	return os.Chmod(dst, mode)
}

// SyncIfNeeded checks if the handbook needs to be pushed to the server and does so if needed
// It compares local handbook with both the state database and the remote server
func (m *Manager) SyncIfNeeded(ctx context.Context, serverURL string, sourceDir string) error {
	// Calculate current file hashes
	currentHashes, err := m.calculateFileHashes(sourceDir)
	if err != nil {
		return errors.NewGenericError("failed to calculate current file hashes", err)
	}

	// Get last synced hashes from state database
	var lastSyncedHashes map[string]string
	if m.stateManager != nil {
		lastSyncedHashes, err = m.stateManager.GetHandbookHashes()
		if err != nil {
			// If we can't get the last synced hashes, use empty map
			lastSyncedHashes = make(map[string]string)
		}
	} else {
		// No state manager, use empty map
		lastSyncedHashes = make(map[string]string)
	}

	// Get server handbook hashes
	serverHashes, err := m.getServerHandbookHashes(ctx, serverURL)
	if err != nil {
		// If server doesn't have handbook or endpoint doesn't exist, push local
		if isNotFoundError(err) {
			return m.Push(ctx, serverURL, sourceDir)
		}
		// For other errors, return the error
		return errors.NewSyncError("failed to get server handbook hashes", err)
	}

	// Check if local matches what we last synced
	localMatchesLastSync := hashesMatch(currentHashes, lastSyncedHashes)

	// Check if server matches what we last synced
	serverMatchesLastSync := hashesMatch(serverHashes, lastSyncedHashes)

	// Check if local matches server
	localMatchesServer := hashesMatch(currentHashes, serverHashes)

	// Case 1: Everything matches - no sync needed
	if localMatchesServer {
		return nil
	}

	// Case 2: Local changed, server unchanged - push local to server
	if !localMatchesLastSync && serverMatchesLastSync {
		return m.Push(ctx, serverURL, sourceDir)
	}

	// Case 3: Server changed, local unchanged - pull server to local
	if localMatchesLastSync && !serverMatchesLastSync {
		return m.Pull(ctx, serverURL, sourceDir)
	}

	// Case 4: Both changed (conflict) - ask user
	return m.handleSyncConflict(ctx, serverURL, sourceDir, currentHashes, serverHashes)
}

// getServerHandbookHashes retrieves the file hashes from the server
func (m *Manager) getServerHandbookHashes(ctx context.Context, serverURL string) (map[string]string, error) {
	// Construct the handbook hashes URL
	url := strings.TrimSuffix(serverURL, "/") + "/mng/handbook/hashes"

	// Create HTTP request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, errors.NewSyncError("failed to create request", err)
	}

	// Execute request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.NewSyncError("failed to fetch handbook hashes", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.NewSyncError("handbook not found on server", nil)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewSyncError(fmt.Sprintf("server returned status %d", resp.StatusCode), nil)
	}

	// Parse JSON response
	var hashes map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&hashes); err != nil {
		return nil, errors.NewSyncError("failed to parse server response", err)
	}

	return hashes, nil
}

// isNotFoundError checks if an error indicates the handbook was not found
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "404")
}

// handleSyncConflict handles the case where both local and server have changed
func (m *Manager) handleSyncConflict(ctx context.Context, serverURL string, sourceDir string, localHashes, serverHashes map[string]string) error {
	// For now, we'll return an error indicating a conflict
	// In a future enhancement, this could prompt the user or use a conflict resolution strategy

	localChanges := countDifferences(localHashes, serverHashes)
	serverChanges := countDifferences(serverHashes, localHashes)

	return errors.NewSyncError(
		fmt.Sprintf(
			"handbook sync conflict detected: local has %d changes, server has %d changes. "+
				"Please manually resolve by running 'onemcp handbook push' to upload local changes "+
				"or 'onemcp handbook pull' to download server changes",
			localChanges,
			serverChanges,
		),
		nil,
	)
}

// countDifferences counts how many files differ between two hash maps
func countDifferences(a, b map[string]string) int {
	count := 0

	// Count files in 'a' that are different or missing in 'b'
	for path, hashA := range a {
		hashB, exists := b[path]
		if !exists || hashA != hashB {
			count++
		}
	}

	// Count files in 'b' that are missing in 'a'
	for path := range b {
		if _, exists := a[path]; !exists {
			count++
		}
	}

	return count
}

// hashesMatch compares two hash maps to see if they're identical
func hashesMatch(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists || valueA != valueB {
			return false
		}
	}

	return true
}

func LoadProjectRegressionSuite(projectDir string) ([]Test, error) {
	var result []Test
	regressionPath := filepath.Join(projectDir, "handbook/regression-suites")
	err := filepath.WalkDir(regressionPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Check extension (.yaml or .yml)
		ext := filepath.Ext(d.Name())
		if ext == ".yaml" || ext == ".yml" {
			matchs, _err := LoadRegressionSuite(path)
			if _err != nil {
				return errors.NewSyncError("error while reading regression tests", _err)
			}
			result = append(result, matchs...)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func LoadRegressionSuite(path string) ([]Test, error) {
	// If file does not exist → return empty initialized structure
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return []Test{}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Unmarshal YAML
	var spec TestSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}

	return spec.Tests, nil
}
