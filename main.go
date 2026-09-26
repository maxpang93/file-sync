package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const (
	RendezvousString = "file-sync-p2p-v1"
	ProtocolID       = protocol.ID("/file-sync/1.0.0") // Custom application stream identifier
)

var greetedPeers sync.Map // Prevents duplicate "Hello World" spam

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize a dual-stack (IPv4/IPv6) libp2p host with TCP and QUIC.
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
	)
	if err != nil {
		log.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer host.Close()

	fmt.Printf("Node started! Peer ID: %s\n", host.ID())
	for _, addr := range host.Addrs() {
		fmt.Printf("Listening on: %s/p2p/%s\n", addr, host.ID())
	}

	// 2. Register handler for incoming streams
	host.SetStreamHandler(ProtocolID, handleIncomingStream)

	// 3. Connect to Kademlia DHT
	kademliaDHT, err := dht.New(host, dht.Mode(dht.ModeAuto))
	if err != nil {
		log.Fatalf("Failed to initialize Kademlia DHT: %v", err)
	}

	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("Failed to bootstrap DHT: %v", err)
	}

	// Bootstrap connects to public initial nodes so our node joins the global DHT mesh
	bootstrapPeers(ctx, host, dht.GetDefaultBootstrapPeerAddrInfos())

	// 4. Advertise and search for peers under the rendezvous topic
	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)
	dutil.Advertise(ctx, routingDiscovery, RendezvousString)
	fmt.Println("Announced presence to DHT. Looking for peers...")

	// Search for peers in the background
	go discoverAndConnect(ctx, host, routingDiscovery)

	// Block main goroutine to keep the node running
	select {}
}

func bootstrapPeers(ctx context.Context, h host.Host, peers []peer.AddrInfo) {
	var wg sync.WaitGroup
	fmt.Println("Connecting to DHT bootstrap nodes...")
	for _, p := range peers {
		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			_ = h.Connect(ctx, pi)
		}(p)
	}
	wg.Wait()
	fmt.Println("DHT Bootstrap complete.")
}

func discoverAndConnect(ctx context.Context, h host.Host, rd *routing.RoutingDiscovery) {
	for {
		peerChan, err := rd.FindPeers(ctx, RendezvousString)
		if err != nil {
			log.Printf("Error finding peers: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for peerInfo := range peerChan {
			// Skip self
			if peerInfo.ID == h.ID() || len(peerInfo.Addrs) == 0 {
				continue
			}

			// Avoid reconnecting or duplicate hello messages
			if _, alreadyGreeted := greetedPeers.Load(peerInfo.ID); alreadyGreeted {
				continue
			}

			if h.Network().Connectedness(peerInfo.ID) != network.Connected {
				fmt.Printf("\n[+] Found peer %s. Connecting...\n", peerInfo.ID.ShortString())
				connectCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				err := h.Connect(connectCtx, peerInfo)
				cancel()
				if err != nil {
					// Offline / dead peer from a previous run, skip silently
					fmt.Printf("Failed to connect to peer %s: %v\n", peerInfo.ID.ShortString(), err)
					continue
				}
			}

			// Mark peer as greeted so we only initiate once
			greetedPeers.Store(peerInfo.ID, true)
			fmt.Printf("[✓] Connected to %s! Sending Hello World...\n", peerInfo.ID.ShortString())
			go sendHello(ctx, h, peerInfo.ID)
		}
		time.Sleep(10 * time.Second)
	}
}

func sendHello(ctx context.Context, h host.Host, target peer.ID) {
	stream, err := h.NewStream(ctx, target, ProtocolID)
	if err != nil {
		log.Printf("Failed to open stream to %s: %v", target.ShortString(), err)
		return
	}
	defer stream.Close()

	msg := fmt.Sprintf("Hello World from %s at %s\n", h.ID().ShortString(), time.Now().Format(time.RFC3339))
	_, _ = stream.Write([]byte(msg))

	// Read reply on the same stream
	reply, _ := bufio.NewReader(stream).ReadString('\n')
	fmt.Printf("[Reply from %s]: %s", target.ShortString(), reply)
}

func handleIncomingStream(stream network.Stream) {
	defer stream.Close()

	msg, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		return
	}

	remotePeer := stream.Conn().RemotePeer()
	fmt.Printf("\n[Incoming Message from %s]: %s", remotePeer.ShortString(), msg)

	// Send ACK reply back on the same stream
	localPeer := stream.Conn().LocalPeer()
	reply := fmt.Sprintf("ACK: Hello received by %s!\n", localPeer.ShortString())
	_, _ = stream.Write([]byte(reply))
}
