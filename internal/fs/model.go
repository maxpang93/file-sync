package fs

import (
	"time"
)

type FileMetadata struct {
	Path         string    `json:"path"`
	Size         int64     `json:"size"`
	ModifiedTime time.Time `json:"modifiedTime"`
	Hash         string    `json:"hash"`
}

type Manifest struct {
	Files []FileMetadata `json:"files"`
}
