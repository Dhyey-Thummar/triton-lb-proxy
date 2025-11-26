package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strconv"

	// "math/rand"
	"container/heap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"strings"
	"sync"
	"time"

	coordpb "triton-lb-proxy/proto/coordpb"
)

const (
	ModeActive       = "ACTIVE"
	ModeIdle         = "IDLE"
	ModeFailOver     = "FAILOVER"
	CollectFrequency = 200
	KHotUtilThresh   = 0.80
	MinActiveServers = 1
	MinIdleServers   = 1
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
	bounceServerID  string
}

type ActiveServerHeap []*serverInfo

func (h ActiveServerHeap) Len() int { return len(h) }
func (h ActiveServerHeap) Less(i, j int) bool {
	id1 := h[i].cid
	id2 := h[j].cid
	if strings.HasPrefix(id1, "proxy-") && strings.HasPrefix(id2, "proxy-") {
		n1, _ := strconv.Atoi(id1[6:])
		n2, _ := strconv.Atoi(id2[6:])
		return n1 > n2
	}
	return id1 > id2
}
func (h ActiveServerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *ActiveServerHeap) Push(x interface{}) {
	*h = append(*h, x.(*serverInfo))
}
func (h *ActiveServerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h ActiveServerHeap) Peek() interface{} {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type IdleServerHeap []*serverInfo

func (h IdleServerHeap) Len() int { return len(h) }
func (h IdleServerHeap) Less(i, j int) bool {
	id1 := h[i].cid
	id2 := h[j].cid
	if strings.HasPrefix(id1, "proxy-") && strings.HasPrefix(id2, "proxy-") {
		n1, _ := strconv.Atoi(id1[6:])
		n2, _ := strconv.Atoi(id2[6:])
		return n1 < n2
	}
	return id1 < id2
}
func (h IdleServerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *IdleServerHeap) Push(x interface{}) {
	*h = append(*h, x.(*serverInfo))
}
func (h *IdleServerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h IdleServerHeap) Peek() interface{} {
	if len(h) == 0 {
		return nil
	}
	return h[0]
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
func (s *CoordinatorServer) callSetMode(serverID, addr, mode string, bounceServerID string) {
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
	_, err := client.SetMode(ctx, &coordpb.ModeRequest{Mode: mode, BounceServerId: bounceServerID, CoordinatorId: "coordinator-1"})
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


func printHeapSorted(h interface{}) {
    // Create a copy so we don't destroy the actual heap
    var copyHeap interface{}
    
    // Type assertion to copy the correct heap type
    if ah, ok := h.(*ActiveServerHeap); ok {
        tmp := make(ActiveServerHeap, len(*ah))
        copy(tmp, *ah)
        copyHeap = &tmp
    } else if ih, ok := h.(*IdleServerHeap); ok {
        tmp := make(IdleServerHeap, len(*ih))
        copy(tmp, *ih)
        copyHeap = &tmp
    }

    // Pop and print
    if ch, ok := copyHeap.(heap.Interface); ok {
        for ch.Len() > 0 {
            item := heap.Pop(ch).(*serverInfo)
            fmt.Print(item.cid + " ")
        }
    }
    fmt.Println()
}

// simple policy: every interval decide randomly how many active servers and set modes
func (s *CoordinatorServer) runSimpleRandomPolicy(interval int, proxyAddrs []string, kHotUtilThresh float64) {
	// initial wait for proxies to register
	log.Printf("[COORD] waiting 20s for proxies to register...")
	time.Sleep(20*time.Second)
	var ids []string
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
		s.mu.Lock()
		s.servers[id] = &serverInfo{addr: addr, cid: id}
		s.mu.Unlock()
		ids = append(ids, id)
	}

	currentActive := 0
	serverLen := len(s.servers)
	activeSet := make(map[string]bool, serverLen)
	activeRequired := 0
	prevActive := 0

	uniqueActiveRequesters := 0
	uniqueIdleRequesters := 0

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

	// init the heaps
	activeHeap := &ActiveServerHeap{}
	idleHeap := &IdleServerHeap{}
	heap.Init(activeHeap)
	heap.Init(idleHeap)

	// last server is the failover node
	failoverID := ids[serverLen-1]
	sinkID := ids[serverLen-2]

	s.mu.Lock()
	for id, info := range s.servers {
		if id == failoverID || id == sinkID {
			continue
		}
		info.bounceServerID = failoverID
		heap.Push(activeHeap, info)
		activeSet[id] = true
		currentActive++
		go s.callSetMode(id, info.addr, ModeActive, failoverID)
	}
	s.servers[failoverID].bounceServerID = sinkID
	s.servers[sinkID].bounceServerID = sinkID
	heap.Push(idleHeap, s.servers[sinkID])
	go s.callSetMode(sinkID, s.servers[sinkID].addr, ModeIdle, sinkID)
	go s.callSetMode(failoverID, s.servers[failoverID].addr, ModeFailOver, sinkID)
	s.mu.Unlock()

	fmt.Printf("[COORD] Failover server: %s, Sink server: %s\n", failoverID, sinkID)
	fmt.Print("[COORD] Initial Active server heaps: ")
	printHeapSorted(activeHeap)
	fmt.Print("[COORD] Initial Idle server heaps: ")
	printHeapSorted(idleHeap)

	t := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer t.Stop()

	for range t.C {
		uniqueActiveRequesters = 0
		uniqueIdleRequesters = 0
		activeRequired = 0
		s.mu.Lock()

		for _, info := range s.servers {
			if info.pingsWantActive > 0 {
				uniqueActiveRequesters++
				info.pingsWantActive = 0 // reset for next interval
			}
			if info.pingsWantIdle > 0 {
				uniqueIdleRequesters++
				info.pingsWantIdle = 0 // reset for next interval
			}
		}

		s.mu.Unlock()
		prevActive = currentActive
		activeRequired = (uniqueActiveRequesters - uniqueIdleRequesters) + currentActive
		if activeRequired < MinActiveServers {
			activeRequired = MinActiveServers
		}
		if activeRequired > (serverLen - MinIdleServers - 1) {
			activeRequired = serverLen - MinIdleServers - 1
		}

		if activeRequired > currentActive {
			// need to activate more servers
			toActivate := activeRequired - currentActive
			for range toActivate {
				if idleHeap.Len() == 0 {
					break
				}
				srv := heap.Pop(idleHeap).(*serverInfo)
				srv.bounceServerID = failoverID
				activeSet[srv.cid] = true
				heap.Push(activeHeap, srv)
				lastIdle := idleHeap.Peek().(*serverInfo)
				if lastIdle != nil {
					lastIdle.bounceServerID = lastIdle.cid
				}
				currentActive++
				s.mu.Lock()
				go s.callSetMode(srv.cid, srv.addr, ModeActive, failoverID)
				go s.callSetMode(lastIdle.cid, lastIdle.addr, ModeIdle, lastIdle.cid)
				s.mu.Unlock()
			}
		} else if activeRequired < currentActive {
			toIdle := currentActive - activeRequired
			for range toIdle {
				if activeHeap.Len() == 0 {
					break
				}
				srv := heap.Pop(activeHeap).(*serverInfo)
				srv.bounceServerID = srv.cid
				activeSet[srv.cid] = false
				heap.Push(idleHeap, srv)
				lastIdle := idleHeap.Peek().(*serverInfo)
				if lastIdle != nil {
					lastIdle.bounceServerID = srv.cid
				}
				currentActive--
				s.mu.Lock()
				go s.callSetMode(srv.cid, srv.addr, ModeIdle, srv.cid)
				go s.callSetMode(lastIdle.cid, lastIdle.addr, ModeIdle, srv.cid)
				s.mu.Unlock()
			}
		}
		fmt.Print("[COORD] Active server heaps: ")
		printHeapSorted(activeHeap)
		fmt.Print("[COORD] Idle server heaps: ")
		printHeapSorted(idleHeap)

		log.Printf("[COORD] Current active: %d, reported active: %d, reported idle: %d, desired active: %d", prevActive, uniqueActiveRequesters, uniqueIdleRequesters, activeRequired)
		s.mu.Lock()
		for id, info := range s.servers {
			if activeSet[id] {
				info.currentMode = ModeActive
			} else {
				info.currentMode = ModeIdle
			}
		}
		s.mu.Unlock()

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
