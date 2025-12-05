package interfaces

import "context"

// HandbookManager handles handbook synchronization and template management
type HandbookManager interface {
	Pull(ctx context.Context, serverURL string, targetDir string) error
	Push(ctx context.Context, serverURL string, sourceDir string) error
	InstallAcmeTemplate(targetDir string) error
	ValidateStructure(handbookDir string, softCheck bool) error
	ResetToAcme(handbookDir string, confirmation string) error
	SyncIfNeeded(ctx context.Context, serverURL string, sourceDir string) error
}
