package p2p

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	proto "filesync/internal/protocol"
)

type P2PHost struct {
	Host host.Host
	DHT  *dht.IpfsDHT
}

func NewHost() (*P2PHost, error) {
	identityKey, err := getIdentityKey(".peer.key")
	if err != nil {
		return nil, fmt.Errorf("load or generate identity key: %w", err)
	}
	host, err := libp2p.New(
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
			"/ip6/::/tcp/0",
			"/ip6/::/udp/0/quic-v1",
		),
		// Relay is required for DCUtR hole punching: two NATed peers use the relay
		// as a temporary meeting point to synchronize their simultaneous UDP punch.
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.NATPortMap(), // Attempts UPnP/NAT-PMP router port-forwarding
		libp2p.Identity(identityKey),
	)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}
	for _, addr := range host.Addrs() {
		log.Printf("Listening on: %s/p2p/%s\n", addr, host.ID())
	}
	h := P2PHost{Host: host}
	return &h, nil
}

// Register handler for incoming streams
func (h *P2PHost) RegisterStream() {
	h.Host.SetStreamHandler(proto.ManifestProtocolID, proto.HandleManifestStream)
}

// Connect to Kademlia DHT
func (h *P2PHost) SetupDHT(ctx context.Context) error {
	kademliaDHT, err := dht.New(h.Host, dht.Mode(dht.ModeAuto))
	if err != nil {
		return fmt.Errorf("initialize Kademlia DHT: %w", err)
	}
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap DHT: %w", err)
	}
	h.DHT = kademliaDHT
	return nil
}

// Connect to default DHT bootstrap peers
func (h *P2PHost) Bootstrap(ctx context.Context) {
	var wg sync.WaitGroup
	log.Println("Connecting to DHT bootstrap nodes...")
	for _, p := range dht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			_ = h.Host.Connect(ctx, pi)
		}(p)
	}
	wg.Wait()
	log.Println("DHT Bootstrap complete.")
}
