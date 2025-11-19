package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	// "math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coordpb "triton-lb-proxy/proto/coordpb"
)

type serverInfo struct {
	addr      string // "host:port" of proxy's control server
	lastDesire string
	lastSeen  time.Time
	cid       string
	cpuUtil   float64
	queueLen  int64
}

type CoordinatorServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu      sync.Mutex
	servers map[string]*serverInfo // serverID -> info

	// clients to proxies so we can call SetMode
	proxyClients map[string]coordpb.ProxyControlClient

	loadgenAddr string
	loadgenClient coordpb.LoadgenControlClient
}

func NewCoordinatorServer() *CoordinatorServer {
	return &CoordinatorServer{
		servers:      make(map[string]*serverInfo),
		proxyClients: make(map[string]coordpb.ProxyControlClient),
	}
}

// Receive NotifyState from proxies
func (s *CoordinatorServer) NotifyState(ctx context.Context, req *coordpb.NotifyRequest) (*coordpb.NotifyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	srv, ok := s.servers[req.ServerId]
	if !ok {
		// address for this server may be unknown; the coordinator may have been started with a list
		// so we just record the desire. If we know addr via config we won't hit this branch.
		srv = &serverInfo{addr: "", cid: req.ServerId}
		s.servers[req.ServerId] = srv
	}
	srv.lastDesire = req.Desire
	srv.lastSeen = time.Now()
	srv.cpuUtil = req.CpuUtil
	srv.queueLen = req.QueueLen
	log.Printf("[COORD] NotifyState from %s desire=%s cpu=%.2f qlen=%d", req.ServerId, req.Desire, req.CpuUtil, req.QueueLen)
	return &coordpb.NotifyResponse{Ok: true, Message: "noted"}, nil
}

// utility to call SetMode on a proxy. It logs failure but continues.
func (s *CoordinatorServer) callSetMode(serverID, addr, mode string) {
	// get or dial client
	s.mu.Lock()
	client, ok := s.proxyClients[serverID]
	s.mu.Unlock()
	if !ok {
		// dial
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			log.Printf("[COORD] dial proxy %s (%s) failed: %v", serverID, addr, err)
			return
		}
		client = coordpb.NewProxyControlClient(conn)
		s.mu.Lock()
		s.proxyClients[serverID] = client
		s.mu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.SetMode(ctx, &coordpb.ModeRequest{Mode: mode, CoordinatorId: "coordinator-1"})
	if err != nil {
		log.Printf("[COORD] SetMode(%s,%s) failed: %v", serverID, mode, err)
	}
}

func (s *CoordinatorServer) ensureLoadgenClient() {
    if s.loadgenClient != nil {
        return
    }
    if s.loadgenAddr == "" {
        return
    }

    conn, err := grpc.Dial(
        s.loadgenAddr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock(),
        grpc.WithTimeout(2*time.Second),
    )
    if err != nil {
        log.Printf("[COORD] loadgen dial failed: %v", err)
        return
    }

    s.loadgenClient = coordpb.NewLoadgenControlClient(conn)
    log.Printf("[COORD] connected to loadgen at %s", s.loadgenAddr)
}

// simple policy: every interval decide randomly how many active servers and set modes
func (s *CoordinatorServer) runSimpleRandomPolicy(interval time.Duration, proxyAddrs []string, kHotUtilThresh float64) {
	// initialize servers map from provided proxyAddrs (format: serverID=host:port or host:port)
	for _, a := range proxyAddrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		// if format "id=addr", split
		parts := strings.SplitN(a, "=", 2)
		var id, addr string
		if len(parts) == 2 {
			id, addr = parts[0], parts[1]
		} else {
			// if no id supplied, use addr as id
			addr = a
			id = a
		}
		s.servers[id] = &serverInfo{addr: addr, cid: id}
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		// collect server IDs and addresses for selection
		var ids []string
		for id, info := range s.servers {
			// only consider those with addresses
			if info.addr != "" {
				ids = append(ids, id)
			}
		}
		s.mu.Unlock()

		if len(ids) == 0 {
			log.Printf("[COORD] no known proxy addresses to manage")
			continue
		}

		// // choose a random number of active servers between 1 and len(ids)
		// k := rand.Intn(len(ids)) + 1
		// // shuffle and pick first k
		// rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		totalCpu := float64(0)
		for _, id := range ids {
			s.mu.Lock()
			totalCpu += s.servers[id].cpuUtil
			s.mu.Unlock()
		}
		k := int(totalCpu/kHotUtilThresh) + 1 // target ~70% per active server
		activeSet := map[string]bool{}
		activeIndices := make([]int32, 0, k)
		for i := 0; i < k; i++ {
			activeSet[ids[i]] = true
			activeIndices = append(activeIndices, int32(i))
		}
		// inform loadgen of active server indices
		s.ensureLoadgenClient()

		if s.loadgenClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := s.loadgenClient.UpdateActiveServers(ctx, &coordpb.UpdateActiveServersRequest{
				ServerIndices: activeIndices,
			})
			cancel()
			if err != nil {
				log.Printf("[COORD] UpdateActiveServers failed: %v", err)
			}
		}


		log.Printf("[COORD] policy: want %d active of %d total", k, len(ids))
		// instruct each proxy
		for _, id := range ids {
			s.mu.Lock()
			addr := s.servers[id].addr
			s.mu.Unlock()
			if activeSet[id] {
				go s.callSetMode(id, addr, "ACTIVE")
			} else {
				go s.callSetMode(id, addr, "IDLE")
			}
		}
	}
}

func main() {
	port := flag.Int("port", 9060, "Coordinator listening port (for proxies NotifyState)")
	proxyList := flag.String("proxies", "", "Comma-separated list of proxy control addresses. Format: serverID=host:port or host:port (serverID==addr).")
	interval := flag.Int("interval", 10, "Policy interval seconds")
	loadgenAddr := flag.String("loadgen", "", "Address of loadgen control server (host:port)")
	kHotUtilThresh := flag.Float64("khot_util_thresh", 0.80, "CPU utilization threshold per active server for k-hot policy")
	flag.Parse()

	coord := NewCoordinatorServer()
	coord.loadgenAddr = *loadgenAddr 

	// parse proxies
	proxyAddrs := []string{}
	if *proxyList != "" {
		for _, p := range strings.Split(*proxyList, ",") {
			proxyAddrs = append(proxyAddrs, strings.TrimSpace(p))
		}
	}

	// start gRPC server to receive NotifyState from proxies
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, coord)
	go func() {
		log.Printf("[COORD] listening NotifyState :%d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	// start policy loop (random)
	go coord.runSimpleRandomPolicy(time.Duration(*interval)*time.Second, proxyAddrs, *kHotUtilThresh)

	// block forever
	select {}
}
