package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"filesync/internal/fs"
	"filesync/internal/p2p"
	proto "filesync/internal/protocol"
	"filesync/internal/scheduler"
)

func main() {
	h, err := p2p.NewHost()
	if err != nil {
		log.Fatalf("Failed to create new p2p host: %v", err)
	}
	h.RegisterStream()

	ctx := context.Background()
	if err := h.SetupDHT(ctx); err != nil {
		log.Fatalf("Failed to bootstrap: %v", err)
	}
	h.Bootstrap(ctx)
	if err := h.Initiate(ctx); err != nil {
		log.Fatalf("Failed to initiate: %v", err)
	}
	// defer h.host.Close()

	engine := scheduler.NewEngine(30*time.Second, work(h))
	engine.Start()

	// Set up OS signal interception
	// Create a channel that listens for interrupt (Ctrl+C) or termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// block forever until signal received
	<-sigChan

	engine.Stop()

}

func work(h *p2p.P2PHost) func() error {
	return func() error {
		ctx := context.Background()
		peers := h.Host.Network().Peers()
		remoteManifestList := getManifest(ctx, h.Host, peers)
		localManifest, err := fs.ComputeManifest("")
		if err != nil {
			return nil
		}
		log.Printf("localManifest: %v\n", localManifest)
		log.Printf("remoteManifestList: %v\n", remoteManifestList)
		return nil
	}
}

func getManifest(ctx context.Context, h host.Host, peers []peer.ID) []fs.Manifest {
	if ctx.Err() != nil {
		return nil
	}

	const maxConcurrency = 20
	sem := make(chan struct{}, maxConcurrency)
	results := make(chan fs.Manifest, maxConcurrency)
	consumerDone := make(chan struct{})
	var wg sync.WaitGroup

	manifestList := make([]fs.Manifest, 0, len(peers))

	go func() {
		defer close(consumerDone)
		for m := range results {
			manifestList = append(manifestList, m)
		}
	}()

	for _, peerID := range peers {
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(pID peer.ID) {
			defer wg.Done()
			defer func() { <-sem }()
			manifest, err := proto.RequestManifest(ctx, h, peerID)
			if err != nil {
				log.Printf("Failed to retrieve manifest from peer %s: %v", pID.ShortString(), err)
				return
			}
			results <- *manifest
		}(peerID)
	}

	wg.Wait()      // Block until all producers are done
	close(results) // Notify consumer end of range loop
	<-consumerDone // Block until consumer is done

	return manifestList
}

func computeManifest(ctx context.Context) (*fs.Manifest, error) {
	manifest, err := fs.ComputeManifest("")
	if err != nil {
		return nil, err
	}
	return manifest, nil
}
