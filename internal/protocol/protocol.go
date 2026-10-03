package protocol

import (
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	// Protect against OOM attacks by capping the maximum message size (e.g., 1MB)
	MaxMessageSize = 1024 * 1024
	// Prevent hung streams by enforcing strict network timeouts
	NetworkTimeout     = 15 * time.Second
	ManifestProtocolID = protocol.ID("/filesync/manifest/1.0.0")
	GetManifestAction  = "GET_MANIFEST"
)

type Request struct {
	Action string `json:"action"`
}
