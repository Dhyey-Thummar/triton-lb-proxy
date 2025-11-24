package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"

	// "math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coordpb "triton-lb-proxy/proto/coordpb"
)

const (
	ModeActive = "ACTIVE"
	ModeIdle   = "IDLE"

	CollectFrequency = 200
	KHotUtilThresh   = 0.80
	MinServers       = 0
)

type serverInfo struct {
	addr            string // "host:port" of proxy's control server
	desire          string
	lastSeen        time.Time
	cid             string
	cpuUtil         float64
	queueLen        int64
	currentMode     string
	pingsWantActive int
	pingsWantIdle   int
}

type CoordinatorServer struct {
	coordpb.UnimplementedCoordinatorServer

	mu      sync.Mutex
	servers map[string]*serverInfo // serverID -> info

	// clients to proxies so we can call SetMode
	proxyClients map[string]coordpb.ProxyControlClient

	loadgenAddr   string
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
	srv.desire = req.Desire
	srv.lastSeen = time.Now()
	srv.cpuUtil = req.CpuUtil
	srv.queueLen = req.QueueLen
	srv.currentMode = req.CurrentMode
	switch req.Desire {
	case ModeActive:
		srv.pingsWantActive++
	case ModeIdle:
		srv.pingsWantIdle++
	}
	// log.Printf("[COORD] NotifyState from %s desire=%s cpu=%.2f qlen=%d", req.ServerId, req.Desire, req.CpuUtil, req.QueueLen)
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
func (s *CoordinatorServer) runSimpleRandomPolicy(interval int, proxyAddrs []string, kHotUtilThresh float64) {
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

	currentActive := 0
	// activeReported := 0
	// idleReported := 0
	activeSet := make(map[string]bool, len(s.servers))
	prevSet := make(map[string]bool, len(s.servers))
	wantActiveSet := make(map[string]bool, len(s.servers))
	wantIdleSet := make(map[string]bool, len(s.servers))
	totalActivePings := 0
	totalIdlePings := 0
	uniqueActiveRequesters := 0
	uniqueIdleRequesters := 0

	t := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		// collect server IDs and addresses for selection
		var ids []string
		currentActive = 0
		// activeReported = 0
		// idleReported = 0
		totalActivePings = 0
		totalIdlePings = 0
		uniqueActiveRequesters = 0
		uniqueIdleRequesters = 0

		// for id, info := range s.servers {
		// 	log.Printf("[COORD] Before Server %s addr=%s desire=%s mode=%s cpu=%.2f qlen=%d", id, info.addr, info.desire, info.currentMode, info.cpuUtil, info.queueLen)
		// }

		for id, info := range s.servers {
			// only consider those with addresses
			if info.addr != "" {
				ids = append(ids, id)
			}
			wantActiveSet[id] = false
			wantIdleSet[id] = false
			activeSet[id] = false
			prevSet[id] = false

			totalActivePings += info.pingsWantActive
			totalIdlePings += info.pingsWantIdle

			if info.pingsWantActive > 0 {
				uniqueActiveRequesters++
				info.pingsWantActive = 0 // reset for next interval
			}
			if info.pingsWantIdle > 0 {
				uniqueIdleRequesters++
				info.pingsWantIdle = 0 // reset for next interval
			}
			// count current active servers
			if info.currentMode == ModeActive {
				currentActive++
				prevSet[id] = true
			}
			// count reported active servers
			switch info.desire {
			case ModeActive:
				wantActiveSet[id] = true
			case ModeIdle:
				wantIdleSet[id] = true
			}

		}
		s.mu.Unlock()

		sort.Slice(ids, func(i, j int) bool {
			id1 := ids[i]
			id2 := ids[j]
			if strings.HasPrefix(id1, "proxy-") && strings.HasPrefix(id2, "proxy-") {
				n1, _ := strconv.Atoi(id1[6:])
				n2, _ := strconv.Atoi(id2[6:])
				return n1 < n2
			}
			return id1 < id2
		})

		if len(ids) == 0 {
			log.Printf("[COORD] no known proxy addresses to manage")
			continue
		}

		activeRequired := (uniqueActiveRequesters - uniqueIdleRequesters) + currentActive
		if activeRequired < MinServers {
			activeRequired = MinServers // <--- PREVENT 0 SERVERS
		}
		if activeRequired > len(ids) {
			activeRequired = len(ids)
		}
		for i := 0; i < activeRequired; i++ {
			activeSet[ids[i]] = true
		}
		log.Printf("[COORD] Current active: %d, reported active: %d, reported idle: %d, desired active: %d", currentActive, uniqueActiveRequesters, uniqueIdleRequesters, activeRequired)
		s.mu.Lock()
		for id, info := range s.servers {
			if activeSet[id] {
				info.currentMode = ModeActive
			} else {
				info.currentMode = ModeIdle
			}
		}
		s.mu.Unlock()

		// // print active set
		// log.Printf("current active: %d, reported active: %d, reported idle: %d, desired active: %d", currentActive, activeReported, idleReported, activeRequired)
		// log.Printf("[COORD] Desired active set: %v", wantActiveSet)
		// log.Printf("[COORD] Desired idle set: %v", wantIdleSet)
		// log.Printf("[COORD] Active servers: %v", activeSet)
		// log.Printf("[COORD] Previous active servers: %v", prevSet)
		// s.mu.Lock()
		// for id, info := range s.servers {
		// 	log.Printf("[COORD] After Server %s addr=%s desire=%s mode=%s cpu=%.2f qlen=%d", id, info.addr, info.desire, info.currentMode, info.cpuUtil, info.queueLen)
		// }
		// s.mu.Unlock()
		// inform loadgen of active server indices
		s.ensureLoadgenClient()

		if s.loadgenClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := s.loadgenClient.UpdateActiveServers(ctx, &coordpb.UpdateActiveServersRequest{
				ServerActive: activeSet,
			})
			cancel()
			if err != nil {
				log.Printf("[COORD] UpdateActiveServers failed: %v", err)
			}
		}
		// instruct each proxy
		for _, id := range ids {
			s.mu.Lock()
			addr := s.servers[id].addr
			s.mu.Unlock()
			if activeSet[id] != prevSet[id] {
				if activeSet[id] {
					// log.Printf("[COORD] Instructing %s to ACTIVE", id)
					s.callSetMode(id, addr, ModeActive)
				} else {
					// log.Printf("[COORD] Instructing %s to IDLE", id)
					s.callSetMode(id, addr, ModeIdle)
				}
			}
		}
	}
}

func main() {
	port := flag.Int("port", 9060, "Coordinator listening port (for proxies NotifyState)")
	proxyList := flag.String("proxies", "", "Comma-separated list of proxy control addresses. Format: serverID=host:port or host:port (serverID==addr).")
	collectFrequency := flag.Int("interval", CollectFrequency, "Policy interval milliseconds")
	loadgenAddr := flag.String("loadgen", "", "Address of loadgen control server (host:port)")
	kHotUtilThresh := flag.Float64("khot_util_thresh", KHotUtilThresh, "CPU utilization threshold per active server for k-hot policy")
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
	go coord.runSimpleRandomPolicy(*collectFrequency, proxyAddrs, *kHotUtilThresh)

	// block forever
	select {}
}
