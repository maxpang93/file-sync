package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"filesync/internal/utils"
)

func ComputeManifest(filePath string) (*Manifest, error) {
	manifest := Manifest{}

	if filePath == "" {
		if dir, err := os.Getwd(); err != nil {
			return nil, err
		} else {
			filePath = dir
		}
	}

	err := filepath.WalkDir(filePath, func(path string, d os.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get info for %s", path)
		}
		hash, err := utils.HashFileMd5(path)
		if err != nil {
			return fmt.Errorf("failed to calculate md5 hash for %s", path)
		}
		manifest.Files = append(manifest.Files, FileMetadata{
			Path:         path,
			Size:         info.Size(),
			ModifiedTime: info.ModTime(),
			Hash:         hash,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk dir path %s: %w", filePath, err)
	}
	return &manifest, nil
}
