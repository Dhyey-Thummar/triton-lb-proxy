package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"
	"bufio"
	"net/http"
	"strconv"
	// "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "triton-lb-proxy/proto"      // your existing triton inference pb
	coordpb "triton-lb-proxy/proto/coordpb" // generated from coordinator.proto
)

// MODE constants
const (
	ModeActive = "ACTIVE"
	ModeIdle   = "IDLE"

	DesireWantActive = "WANT_ACTIVE"
	DesireWantIdle   = "WANT_IDLE"
)

type TritonMetricsTracker struct {
	url           string
	cpuUtil       atomic.Uint64
	queueLen      atomic.Uint64
}

// ... keep your existing newTritonMetricsTracker and asyncMetrics code (omitted here for brevity).
// For clarity in this answer I'll re-include minimal metric fetcher and the asyncMetrics structure.

func newTritonMetricsTracker(metricsURL string, interval time.Duration) *TritonMetricsTracker {
	t := &TritonMetricsTracker{url: metricsURL}
	go func() {
		for {
			resp, err := http.Get(metricsURL)
			if err != nil {
				log.Printf("[METRICS] failed to GET %s: %v", metricsURL, err)
				time.Sleep(interval)
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
			time.Sleep(interval)
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
	triton    *TritonMetricsTracker
	qlenThresh int64
	active    uint64
	total     uint64
	bounces   uint64
}
func newAsyncMetrics(metricsURL string, qlenThresh int64) *asyncMetrics {
	m := &asyncMetrics{
		triton: newTritonMetricsTracker(metricsURL, 50*time.Millisecond),
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
	rrCounter uint32
	mode      atomic.Value // stores string "ACTIVE" or "IDLE"

	// coordinator client (to notify desire)
	coordClient coordpb.CoordinatorClient
	coordAddr   string
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

func NewTritonLBProxy(serverID, localAddr string, idleAddrs []string, qlenThresh int64, coordAddr string) (*TritonLBProxy, error) {
	localConn, err := dialClient(localAddr)
	if err != nil {
		return nil, fmt.Errorf("dial local triton: %w", err)
	}
	localClient := pb.NewGRPCInferenceServiceClient(localConn)

	idleClients := make([]pb.GRPCInferenceServiceClient, 0, len(idleAddrs))
	for _, a := range idleAddrs {
		c, err := dialClient(a)
		if err != nil {
			log.Printf("[%s] warning: dial idle %s failed: %v", serverID, a, err)
			continue
		}
		idleClients = append(idleClients, pb.NewGRPCInferenceServiceClient(c))
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
		metrics:     newAsyncMetrics("http://localhost:8002/metrics", qlenThresh),
		coordClient: coordClient,
		coordAddr:   coordAddr,
	}
	proxy.mode.Store(ModeActive) // default active
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
	if n == 0 {
		return p.localClient.ModelInfer(ctx, req)
	}
	idx := int(atomic.AddUint32(&p.rrCounter, 1)-1) % n
	cli := p.idleClients[idx]
	p.metrics.incBounces()
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
	if mode != ModeActive && mode != ModeIdle {
		return &coordpb.ModeResponse{Ok: false, Message: "unknown mode"}, nil
	}
	s.proxy.mode.Store(mode)
	log.Printf("[%s] SetMode -> %s (from coordinator=%s)", s.proxy.serverID, mode, req.CoordinatorId)
	return &coordpb.ModeResponse{Ok: true, Message: "ok"}, nil
}

// Utility: periodically evaluate metrics and notify coordinator of desire.
func (p *TritonLBProxy) startDesireReporter(ctx context.Context, interval time.Duration) {
	if p.coordClient == nil {
		log.Printf("[%s] no coordinator client configured, skipping desire reporter", p.serverID)
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				queue := p.metrics.triton.QueueLen()
				desire := DesireWantActive
				if queue > p.metrics.qlenThresh {
					desire = DesireWantIdle
				}
				req := &coordpb.NotifyRequest{
					ServerId: p.serverID,
					Desire:   desire,
					CpuUtil:  p.metrics.triton.Util(),
					QueueLen: int64(queue),
				}
				ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := p.coordClient.NotifyState(ctx2, req)
				cancel()
				if err != nil {
					log.Printf("[%s] failed NotifyState to coordinator (%s): %v", p.serverID, p.coordAddr, err)
				} else {
					log.Printf("[%s] NotifyState -> %s (queue=%d)", p.serverID, desire, queue)
				}
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
	idleProxiesStr := flag.String("idle-proxies", "", "Comma-separated idle proxy addresses host:port")
	coordAddr := flag.String("coordinator", "", "Coordinator gRPC address (host:port) to notify")
	qlenThresh := flag.Int64("qthresh", 5, "Queue length threshold to trigger bouncing")
	flag.Parse()

	idleProxies := []string{}
	if *idleProxiesStr != "" {
		for _, p := range strings.Split(*idleProxiesStr, ",") {
			idleProxies = append(idleProxies, strings.TrimSpace(p))
		}
	}

	proxy, err := NewTritonLBProxy(*serverID, *localTriton, idleProxies, *qlenThresh, *coordAddr)
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
	proxy.startDesireReporter(ctx, 5*time.Second)

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
