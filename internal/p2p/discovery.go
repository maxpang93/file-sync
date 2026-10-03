package p2p

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"

	"filesync/internal/utils"
)

const (
	RendezvousString = "filesync-p2p-v1"
)

func (h *P2PHost) Initiate(ctx context.Context) error {
	routingDiscovery := routing.NewRoutingDiscovery(h.dht)
	dutil.Advertise(ctx, routingDiscovery, RendezvousString)
	log.Println("Announced presence to DHT. Looking for peers...")

	return discoverAndConnect(ctx, h.host, routingDiscovery)
}

func discoverAndConnect(ctx context.Context, h host.Host, rd *routing.RoutingDiscovery) error {
	peerChan, err := rd.FindPeers(ctx, RendezvousString)
	if err != nil {
		return fmt.Errorf("find peers: %w", err)
	}

	for peerInfo := range peerChan {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Skip self
		if peerInfo.ID == h.ID() || len(peerInfo.Addrs) == 0 {
			continue
		}

		if h.Network().Connectedness(peerInfo.ID) != network.Connected {
			log.Printf("\n[+] Found peer %s. Connecting...\n", peerInfo.ID.ShortString())
			err := utils.Retry(ctx, 3, 5*time.Second, func() error {
				connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				return h.Connect(connectCtx, peerInfo)
			})
			if err != nil {
				// Offline / dead peer from a previous run, skip silently
				log.Printf("Failed to connect to peer %s: %v\n", peerInfo.ID.ShortString(), err)
				continue
			}
		}

		log.Printf("[✓] Connected to %s!\n", peerInfo.ID.ShortString())
	}
	return nil
}
