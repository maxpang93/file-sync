package protocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"filesync/internal/fs"
)

// Manages a robust, bi-directional connection with a peer.
func HandleManifestStream(stream network.Stream) {
	defer stream.Close()

	remotePeer := stream.Conn().RemotePeer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readChan := make(chan []byte, 10)
	errChan := make(chan error, 2)

	// Async reader loop that decodes framed length-prefixed messages from the stream
	go func() {
		defer close(readChan)

		reader := bufio.NewReader(stream)
		for {
			_ = stream.SetDeadline(time.Now().Add(NetworkTimeout))

			var msgLen uint32
			err := binary.Read(reader, binary.BigEndian, &msgLen)
			if err != nil {
				errChan <- err
				return
			}

			// Guard against OOM
			if msgLen > MaxMessageSize {
				errChan <- fmt.Errorf("message length %d exceeds max allowed size", msgLen)
				return
			}

			// Use a memory buffer pool to prevent GC thrashing under heavy load
			buf := pool.Get(int(msgLen))
			_ = stream.SetReadDeadline(time.Now().Add(NetworkTimeout))
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				pool.Put(buf)
				errChan <- err
				return
			}

			select {
			case readChan <- buf:
			case <-ctx.Done():
				pool.Put(buf)
				return
			}
		}
	}()

	// Processing loop
	for {
		select {
		case msgData, ok := <-readChan:
			if !ok {
				return
			}

			fmt.Printf("[%s]: Received %d bytes\n", remotePeer.ShortString(), len(msgData))

			var msg Request
			if err := json.Unmarshal(msgData, &msg); err != nil {
				log.Printf("Failed to unmarshal request: %v", err)
				return
			}

			replyPayload, err := processData(msg.Action)
			if err != nil {
				pool.Put(msgData)
				log.Printf("Failed to process request action %q for %s: %v\n", msg.Action, remotePeer.ShortString(), err)
				return
			}

			pool.Put(msgData) // Return buffer

			stream.SetWriteDeadline(time.Now().Add(NetworkTimeout))

			var prefixBuf [4]byte
			binary.BigEndian.PutUint32(prefixBuf[:], uint32(len(replyPayload)))

			if _, err := stream.Write(append(prefixBuf[:], replyPayload...)); err != nil {
				log.Printf("Failed to send reply to %s: %v\n", remotePeer.ShortString(), err)
				return
			}

		case err := <-errChan:
			if err != io.EOF {
				log.Printf("Stream error with peer %s: %v\n", remotePeer.ShortString(), err)
				// Forcefully terminate the stream topology on malicious/broken errors
				_ = stream.Reset()
			}
			return
		case <-time.After(NetworkTimeout * 2):
			log.Printf("Stream idle timeout with peer %s\n", remotePeer.ShortString())
			_ = stream.Reset()
			return
		}
	}
}

func processData(action string) ([]byte, error) {
	if strings.ToUpper(action) == GetManifestAction {
		manifest, err := fs.ComputeManifest("")
		if err != nil {
			return nil, err
		}
		returnData, err := json.Marshal(manifest)
		return returnData, err
	}
	return nil, fmt.Errorf("invalid action %s", action)
}

func RequestManifest(ctx context.Context, h host.Host, target peer.ID) (*fs.Manifest, error) {
	stream, err := h.NewStream(ctx, target, ManifestProtocolID)
	if err != nil {
		return nil, fmt.Errorf("open stream to %s: %w", target.ShortString(), err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(NetworkTimeout))

	req := Request{
		Action: GetManifestAction,
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest request: %w", err)
	}

	var prefixBuf [4]byte
	binary.BigEndian.PutUint32(prefixBuf[:], uint32(len(reqBytes)))
	if _, err := stream.Write(append(prefixBuf[:], reqBytes...)); err != nil {
		return nil, fmt.Errorf("write manifest request to %s: %w", target.ShortString(), err)
	}

	// Read reply on the same stream
	reader := bufio.NewReader(stream)
	var msgLen uint32
	err = binary.Read(reader, binary.BigEndian, &msgLen)
	if err != nil {
		return nil, err
	}

	buf := pool.Get(int(msgLen))
	defer func() {
		pool.Put(buf)
	}()
	_ = stream.SetReadDeadline(time.Now().Add(NetworkTimeout))
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	manifest := fs.Manifest{}
	err = json.Unmarshal(buf, &manifest)
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}
