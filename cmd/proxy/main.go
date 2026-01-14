package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	// "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "triton-lb-proxy/proto"              // your existing triton inference pb
	coordpb "triton-lb-proxy/proto/coordpb" // generated from coordinator.proto
)

// MODE constants
const (
	ModeActive         = "ACTIVE"
	ModeIdle           = "IDLE"
	ModeFailOver       = "FAILOVER"
	CollectFrequency   = 10
	ReportFrequency    = 200
	QueuelenThresh     = 2
	QueueHistoryLength = 10
	QueueUpperBound    = 0
)

type TritonMetricsTracker struct {
	url                 string
	cpuUtil             atomic.Uint64
	queueLen            atomic.Uint64
	queueHistory        []int64
	queueHistoryCounter atomic.Uint64
}

func newTritonMetricsTracker(metricsURL string, interval int) *TritonMetricsTracker {
	t := &TritonMetricsTracker{url: metricsURL, queueHistory: make([]int64, 0, QueueHistoryLength)}
	go func() {
		for {
			resp, err := http.Get(metricsURL)
			if err != nil {
				log.Printf("[METRICS] failed to GET %s: %v", metricsURL, err)
				time.Sleep(time.Duration(interval) * time.Millisecond)
				continue
			}
			scanner := bufio.NewScanner(resp.Body)
			var cpuUtil, queueLen float64
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "nv_cpu_utilization") {
					fields := strings.Fields(line)
					if len(fields) == 2 {
						cpuUtil, _ = strconv.ParseFloat(fields[1], 64)
					}
				} else if strings.HasPrefix(line, "nv_inference_pending_request_count") {
					fields := strings.Fields(line)
					if len(fields) == 2 {
						queueLen, _ = strconv.ParseFloat(fields[1], 64)
					}
				}
			}
			resp.Body.Close()
			t.cpuUtil.Store(uint64(cpuUtil * 10000))
			t.queueLen.Store(uint64(queueLen))
			// t.queueHistory = append(t.queueHistory, int64(queueLen))
			// if len(t.queueHistory) > QueueHistoryLength {
			// 	t.queueHistory = t.queueHistory[1:]
			// }
			t.queueHistoryCounter.Add(uint64(queueLen))
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
	}()
	return t
}

func (t *TritonMetricsTracker) Util() float64 {
	return float64(t.cpuUtil.Load()) / 10000.0
}
func (t *TritonMetricsTracker) QueueLen() int64 {
	return int64(t.queueLen.Load())
}

type asyncMetrics struct {
	triton     *TritonMetricsTracker
	qlenThresh int64
	active     uint64
	total      uint64
	bounces    uint64
}

func newAsyncMetrics(metricsURL string, qlenThresh int64, collectFreq int) *asyncMetrics {
	m := &asyncMetrics{
		triton:     newTritonMetricsTracker(metricsURL, collectFreq),
		qlenThresh: qlenThresh,
	}
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for range t.C {
			log.Printf("[METRICS] cpu=%.2f%% qlen=%d active=%d total=%d bounces=%d",
				m.triton.Util()*100, m.triton.QueueLen(), atomic.LoadUint64(&m.active),
				atomic.LoadUint64(&m.total), atomic.LoadUint64(&m.bounces))
		}
	}()
	return m
}
func (m *asyncMetrics) incActive()  { atomic.AddUint64(&m.active, 1) }
func (m *asyncMetrics) decActive()  { atomic.AddUint64(&m.active, ^uint64(0)) }
func (m *asyncMetrics) incTotal()   { atomic.AddUint64(&m.total, 1) }
func (m *asyncMetrics) incBounces() { atomic.AddUint64(&m.bounces, 1) }

type TritonLBProxy struct {
	pb.UnimplementedGRPCInferenceServiceServer

	serverID    string
	localClient pb.GRPCInferenceServiceClient
	idleClients []pb.GRPCInferenceServiceClient

	metrics *asyncMetrics

	// round robin and mode
	// rrCounter uint32
	mode      atomic.Value // stores string "ACTIVE" or "IDLE"

	// coordinator client (to notify desire)
	coordClient coordpb.CoordinatorClient
	coordAddr   string

	bounceServerID atomic.Value // stores string
}

func dialClient(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(100*1024*1024),
			grpc.MaxCallSendMsgSize(100*1024*1024),
		),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewTritonLBProxy(serverID, localAddr string, idleAddrs []string, qlenThresh int64, coordAddr string, collectFrequency int) (*TritonLBProxy, error) {
	localConn, err := dialClient(localAddr)
	if err != nil {
		return nil, fmt.Errorf("dial local triton: %w", err)
	}
	localClient := pb.NewGRPCInferenceServiceClient(localConn)

	idleClients := make([]pb.GRPCInferenceServiceClient, len(idleAddrs))
	for i, a := range idleAddrs {
		if a == "" {
			continue
		}
		// Use grpc.Dial directly without WithBlock for lazy connection
		conn, err := grpc.Dial(a,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(100*1024*1024),
				grpc.MaxCallSendMsgSize(100*1024*1024),
			),
		)
		if err != nil {
			log.Printf("[%s] warning: could not create lazy client for %s: %v", serverID, a, err)
			continue
		}
		idleClients[i] = pb.NewGRPCInferenceServiceClient(conn)
	}

	var coordClient coordpb.CoordinatorClient
	if coordAddr != "" {
		conn, err := dialClient(coordAddr)
		if err != nil {
			log.Printf("[%s] warning: dial coordinator %s failed: %v", serverID, coordAddr, err)
		} else {
			coordClient = coordpb.NewCoordinatorClient(conn)
		}
	}

	proxy := &TritonLBProxy{
		serverID:    serverID,
		localClient: localClient,
		idleClients: idleClients,
		metrics:     newAsyncMetrics("http://localhost:8002/metrics", qlenThresh, collectFrequency),
		coordClient: coordClient,
		coordAddr:   coordAddr,
	}
	proxy.mode.Store(ModeActive) // default active
	proxy.bounceServerID.Store("")  // default no bounce ID
	return proxy, nil
}

// ModelInfer: same core logic but checks queue -> bounce
func (p *TritonLBProxy) ModelInfer(ctx context.Context, req *pb.ModelInferRequest) (*pb.ModelInferResponse, error) {
	p.metrics.incActive()
	p.metrics.incTotal()

	// If queue length high and have idle backends -> bounce
	if p.metrics.triton.QueueLen() > p.metrics.qlenThresh && len(p.idleClients) > 0 {
		resp, err := p.BounceToIdle(ctx, req)
		p.metrics.decActive()
		return resp, err
	}

	resp, err := p.localClient.ModelInfer(ctx, req)
	p.metrics.decActive()
	return resp, err
}

func (p *TritonLBProxy) BounceToIdle(ctx context.Context, req *pb.ModelInferRequest) (*pb.ModelInferResponse, error) {
	n := len(p.idleClients)
	bounceId := p.bounceServerID.Load().(string)
	var idx int
	if bounceId != "" {
		// try to parse bounceId like "proxy-3"
		if strings.HasPrefix(bounceId, "proxy-") {
			numStr := bounceId[6:]
			num, err := strconv.Atoi(numStr)
			if err == nil && num >= 1 && num <= n {
				idx = num - 1
			}
		}
	} else {
		idx = 0
	}
	cli := p.idleClients[idx]
	p.metrics.incBounces()
	// log.Printf("[%s] bouncing request to idle proxy-%d", p.serverID, idx+1)
	resp, err := cli.ModelInfer(ctx, req)
	return resp, err
}

// -- Proxy exposes the Control service so coordinator can flip modes
type ControlServer struct {
	coordpb.UnimplementedProxyControlServer
	proxy *TritonLBProxy
}

func (s *ControlServer) SetMode(ctx context.Context, req *coordpb.ModeRequest) (*coordpb.ModeResponse, error) {
	mode := strings.ToUpper(strings.TrimSpace(req.Mode))
	if mode != ModeActive && mode != ModeIdle && mode != ModeFailOver {
		return &coordpb.ModeResponse{Ok: false, Message: "unknown mode"}, nil
	}
	s.proxy.mode.Store(mode)
	s.proxy.bounceServerID.Store(req.BounceServerId)
	log.Printf("[%s] SetMode -> %s (from coordinator=%s)", s.proxy.serverID, mode, req.CoordinatorId)
	return &coordpb.ModeResponse{Ok: true, Message: "ok"}, nil
}

func (p *TritonLBProxy) makeNotifyRequest(serverID, desire string, cpuUtil float64, queueLen int64) {
	req := &coordpb.NotifyRequest{
		ServerId:    serverID,
		Desire:      desire,
		CpuUtil:     cpuUtil,
		QueueLen:    queueLen,
		CurrentMode: p.mode.Load().(string),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := p.coordClient.NotifyState(ctx, req)
	if err != nil {
		log.Printf("[%s] failed NotifyState to coordinator (%s): %v", p.serverID, p.coordAddr, err)
	} else {
		// log.Printf("[%s] NotifyState -> %s (queue=%d)", p.serverID, desire, queueLen)
	}
}

// Utility: periodically evaluate metrics and notify coordinator of desire.
func (p *TritonLBProxy) startDesireReporter(ctx context.Context, interval int) {
	if p.coordClient == nil {
		log.Printf("[%s] no coordinator client configured, skipping desire reporter", p.serverID)
		return
	}
	// make initial notify
	// p.makeNotifyRequest(p.serverID, p.mode.Load().(string), p.metrics.triton.Util(), p.metrics.triton.QueueLen())
	go func() {
		t := time.NewTicker(time.Duration(interval) * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				currMode := p.mode.Load().(string)
				// log.Printf("Current mode=%s queue=%d", p.mode.Load().(string), p.metrics.triton.QueueLen())
				switch currMode {
				case ModeActive:
					if p.metrics.triton.queueHistoryCounter.Load() == 0 {
						// if queue history is all zero, we want idle
						p.makeNotifyRequest(p.serverID, ModeIdle, p.metrics.triton.Util(), p.metrics.triton.QueueLen())
					}
				case ModeIdle:
					if p.metrics.triton.queueHistoryCounter.Load() > QueueUpperBound {
						// if queue is high, we want active
						p.makeNotifyRequest(p.serverID, ModeActive, p.metrics.triton.Util(), p.metrics.triton.QueueLen())
					}
				case ModeFailOver:
					// if p.metrics.triton.queueHistoryCounter.Load() == 0 {
					// 	p.makeNotifyRequest(p.serverID, ModeIdle, p.metrics.triton.Util(), p.metrics.triton.QueueLen())
					// }  
					// if p.metrics.triton.queueHistoryCounter.Load() > QueueUpperBound {
					// 	p.makeNotifyRequest(p.serverID, ModeActive, p.metrics.triton.Util(), p.metrics.triton.QueueLen())
					// }
				}
				p.metrics.triton.queueHistoryCounter.Store(0)
			}
		}
	}()
}

func (p *TritonLBProxy) ServerLive(ctx context.Context, req *pb.ServerLiveRequest) (*pb.ServerLiveResponse, error) {
	return p.localClient.ServerLive(ctx, req)
}
func (p *TritonLBProxy) ServerReady(ctx context.Context, req *pb.ServerReadyRequest) (*pb.ServerReadyResponse, error) {
	return p.localClient.ServerReady(ctx, req)
}
func (p *TritonLBProxy) ModelReady(ctx context.Context, req *pb.ModelReadyRequest) (*pb.ModelReadyResponse, error) {
	return p.localClient.ModelReady(ctx, req)
}
func (p *TritonLBProxy) ServerMetadata(ctx context.Context, req *pb.ServerMetadataRequest) (*pb.ServerMetadataResponse, error) {
	return p.localClient.ServerMetadata(ctx, req)
}
func (p *TritonLBProxy) ModelMetadata(ctx context.Context, req *pb.ModelMetadataRequest) (*pb.ModelMetadataResponse, error) {
	return p.localClient.ModelMetadata(ctx, req)
}
func (p *TritonLBProxy) ModelConfig(ctx context.Context, req *pb.ModelConfigRequest) (*pb.ModelConfigResponse, error) {
	return p.localClient.ModelConfig(ctx, req)
}
func (p *TritonLBProxy) ModelStatistics(ctx context.Context, req *pb.ModelStatisticsRequest) (*pb.ModelStatisticsResponse, error) {
	return p.localClient.ModelStatistics(ctx, req)
}

func main() {
	serverID := flag.String("id", "proxy-1", "Server ID")
	port := flag.Int("port", 8050, "Proxy listening port (inference proxy)")
	controlPort := flag.Int("control-port", 8051, "Control listening port (for coordinator SetMode)")
	localTriton := flag.String("local-triton", "localhost:8001", "Local Triton gRPC address (host:port)")
	idleProxiesStr := flag.String("idle-proxies", "10.10.0.1:8050,10.10.0.2:8050,10.10.0.3:8050,10.10.0.4:8050,10.10.0.5:8050,10.10.0.6:8050,10.10.0.7:8050,10.10.0.8:8050,10.10.0.9:8050,10.10.0.10:8050,", "Comma-separated idle proxy addresses host:port")
	localModeFlag := flag.Bool("local-mode", false, "If true, use local decision making instead of coordinator")
	coordAddr := flag.String("coordinator", "10.10.0.11:9060", "Coordinator gRPC address (host:port) to notify")
	qlenThresh := flag.Int64("qthresh", QueuelenThresh, "Queue length threshold to trigger bouncing")
	collectFrequency := flag.Int("collect-freq", CollectFrequency, "Metrics collection frequency")
	reportFrequency := flag.Int("report-freq", ReportFrequency, "Desire report frequency to coordinator")
	flag.Parse()

	idleProxies := []string{}
	if *idleProxiesStr != "" {
		for _, p := range strings.Split(*idleProxiesStr, ",") {
			idleProxies = append(idleProxies, strings.TrimSpace(p))
		}
	}
	if *localModeFlag {
		*coordAddr = ""
		idleProxies = []string{"10.10.0.2:8001"}
		log.Printf("[%s] local-mode enabled, not using coordinator, idle proxies: %v", *serverID, idleProxies)
	}

	proxy, err := NewTritonLBProxy(*serverID, *localTriton, idleProxies, *qlenThresh, *coordAddr, *collectFrequency)
	if err != nil {
		log.Fatalf("new proxy: %v", err)
	}

	// GRPC server for inference + control
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(100*1024*1024),
		grpc.MaxSendMsgSize(100*1024*1024),
	)
	pb.RegisterGRPCInferenceServiceServer(grpcServer, proxy)

	// separate server for control API (so coordinator can call SetMode)
	controlLis, err := net.Listen("tcp", fmt.Sprintf(":%d", *controlPort))
	if err != nil {
		log.Fatalf("control listen: %v", err)
	}
	controlServer := grpc.NewServer()
	cs := &ControlServer{proxy: proxy}
	coordpb.RegisterProxyControlServer(controlServer, cs)

	// start control server
	go func() {
		log.Printf("[%s] control listening :%d", *serverID, *controlPort)
		if err := controlServer.Serve(controlLis); err != nil {
			log.Fatalf("control serve: %v", err)
		}
	}()

	// start inference server (blocking)
	go func() {
		log.Printf("[%s] inference proxy listening :%d (local triton=%s idle=%d)", *serverID, *port, *localTriton, len(idleProxies))
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	// start desire reporter to coordinator
	ctx := context.Background()
	proxy.startDesireReporter(ctx, *reportFrequency)

	// Example: if coordinator not present, we still periodically check local queue and flip local mode randomly (demo)
	if proxy.coordClient == nil {
		go func() {
			t := time.NewTicker(15 * time.Second)
			defer t.Stop()
			for range t.C {
				// naive local decision: if queue low -> active, else idle
				if proxy.metrics.triton.QueueLen() > proxy.metrics.qlenThresh {
					proxy.mode.Store(ModeIdle)
				} else {
					proxy.mode.Store(ModeActive)
				}
				log.Printf("[%s] local mode=%s (queue=%d)", *serverID, proxy.mode.Load().(string), proxy.metrics.triton.QueueLen())
			}
		}()
	}

	// block forever
	select {}
}
