package main

import (
	"context"
	"log"

	"filesync/internal/p2p"
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

	select {}
}
