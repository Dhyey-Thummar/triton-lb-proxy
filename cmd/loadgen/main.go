package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "triton-lb-proxy/proto"
	coordpb "triton-lb-proxy/proto/coordpb"

	"google.golang.org/grpc"
)

type LoadgenState struct {
	allServers []string
	active     atomic.Value // stores []bool, length = len(allServers)
	rrCounter  uint64
}

func NewLoadgenState(servers []string) *LoadgenState {
	st := &LoadgenState{allServers: servers}
	st.active.Store(make([]bool, len(servers)))
	// initially mark first initServers as active
	for i := 0; i < len(servers); i++ {
		st.active.Load().([]bool)[i] = true
	}
	return st
}

func (st *LoadgenState) NextServer() string {
	active := st.active.Load().([]bool)

	var activeList []int
	for i, ok := range active {
		if ok {
			activeList = append(activeList, i)
		}
	}

	n := atomic.AddUint64(&st.rrCounter, 1)

	// // if no active servers, fall back to initial servers
	// if len(activeList) == 0 {
	// 	idx := (n - 1) % uint64(st.initServers)
	// 	return st.allServers[idx]
	// }

	idx := activeList[(n-1)%uint64(len(activeList))]
	return st.allServers[idx]
}

type LoadgenControlServer struct {
	state *LoadgenState
	nsCh  chan<- int
	coordpb.UnimplementedLoadgenControlServer
}

func (s *LoadgenControlServer) UpdateActiveServers(
	ctx context.Context,
	req *coordpb.UpdateActiveServersRequest,
) (*coordpb.UpdateActiveServersResponse, error) {

	newActive := make([]bool, len(s.state.allServers))
	// log.Printf("req active servers: %v", req.ServerActive)
	// log.Printf("cur servers: %v", s.state.allServers)
	for id, active := range req.ServerActive {
		if !strings.HasPrefix(id, "proxy-") {
			continue
		}
		num := id[6:]
		idx, err := strconv.Atoi(num)
		if err != nil || idx < 1 || idx > len(newActive) {
			log.Printf("[LOADGEN] warning: ignoring invalid server ID %s", id)
			continue
		}
		newActive[idx-1] = active
	}
	// log.Printf("new active servers: %v", newActive)

	s.state.active.Store(newActive)

	// log.Printf("[LOADGEN] updated active servers: %v", newActive)
	countActive := 0
	for _, v := range newActive {
		if v {
			countActive++
		}
	}
	s.nsCh <- countActive

	total := len(newActive)
	barWidth := 20 // width of the loading bar in characters
	filled := int(float64(countActive) / float64(total) * float64(barWidth))

	bar := "[" + strings.Repeat("█", filled) + strings.Repeat(" ", barWidth-filled) + "]"
	log.Printf("[LOADGEN] active servers: %d/%d %s", countActive, total, bar)

	return &coordpb.UpdateActiveServersResponse{Ok: true}, nil
}

// ───────────────────────── Input Tensor ─────────────────────────
func makeRandomImage(batch int) [][]byte {
	elems := batch * 3 * 224 * 224
	buf := make([]byte, elems*4)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tmp := make([]byte, 4)
	for i := 0; i < elems; i++ {
		f := rng.Float32()
		binary.LittleEndian.PutUint32(tmp, math.Float32bits(f))
		copy(buf[i*4:], tmp)
	}
	return [][]byte{buf}
}

//
// ───────────────────────── Server Pool ─────────────────────────
//

type ServerPool struct {
	clients map[string]pb.GRPCInferenceServiceClient
}

func newServerPool(urls []string) *ServerPool {
	m := make(map[string]pb.GRPCInferenceServiceClient)
	for _, u := range urls {
		conn, err := grpc.Dial(u, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("dial failed: %v", u)
		}
		m[u] = pb.NewGRPCInferenceServiceClient(conn)
	}
	return &ServerPool{clients: m}
}

func (sp *ServerPool) ClientFor(addr string) pb.GRPCInferenceServiceClient {
	return sp.clients[addr]
}

//
// ───────────────────────── Worker Pool ─────────────────────────
//

type WorkerPool struct {
	pool    *ServerPool
	model   string
	version string
	batch   int
	input   [][]byte
	latCh   chan time.Duration
	wg      sync.WaitGroup
	state   *LoadgenState
}

func newWorkerPool(n int, sp *ServerPool, model, version string, batch int, input [][]byte, latCh chan time.Duration, state *LoadgenState) *WorkerPool {
	wp := &WorkerPool{
		pool:    sp,
		model:   model,
		version: version,
		batch:   batch,
		input:   input,
		latCh:   latCh,
		state:   state,
	}

	wp.wg.Add(n)
	for i := 0; i < n; i++ {
		go wp.worker()
	}
	return wp
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for range requestQueue {
		serverAddr := wp.state.NextServer()
		client := wp.pool.ClientFor(serverAddr)

		req := &pb.ModelInferRequest{
			ModelName:    wp.model,
			ModelVersion: wp.version,
			Inputs: []*pb.ModelInferRequest_InferInputTensor{
				{Name: "INPUT__0", Datatype: "FP32", Shape: []int64{int64(wp.batch), 3, 224, 224}},
			},
			Outputs: []*pb.ModelInferRequest_InferRequestedOutputTensor{
				{Name: "OUTPUT__0"},
			},
			RawInputContents: wp.input,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		start := time.Now()
		_, err := client.ModelInfer(ctx, req)
		wp.latCh <- time.Since(start)
		cancel()

		if err != nil {
			log.Printf("infer error: %v", err)
			log.Printf("server addr: %s", serverAddr)
		} else {

		}
	}
}

var requestQueue = make(chan struct{}, 100000)

//
// ───────────────────────── Latency Log ─────────────────────────
//

func logLatency(latCh <-chan time.Duration) {
	f, err := os.Create("latency.log")
	if err != nil {
		log.Fatalf("failed latency.log: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 0, 8192)
	ticker := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case lat, ok := <-latCh:
			if !ok {
				return
			}
			buf = append(buf, fmt.Sprintf("%d,%d\n", time.Now().UnixNano(), lat.Nanoseconds())...)
			if len(buf) >= 8192 {
				f.Write(buf)
				buf = buf[:0]
			}

		case <-ticker.C:
			if len(buf) > 0 {
				f.Write(buf)
				buf = buf[:0]
			}
		}
	}
}

func logNumServers(ch <-chan int) {
	f, err := os.Create("num_servers.log")
	if err != nil {
		log.Fatalf("failed num_servers.log: %v", err)
	}
	defer f.Close()

	var buf strings.Builder
	// Flush to disk every 100ms
	ticker := time.NewTicker(100 * time.Millisecond)
	
	// Flush threshold (write early if we have too much data in RAM)
	const maxBufferBytes = 16 * 1024 // 16KB

	for {
		select {
		case n, ok := <-ch:
			if !ok {
				// Channel closed, flush remaining data and exit
				if buf.Len() > 0 {
					f.WriteString(buf.String())
				}
				return
			}
			// Append to memory buffer
			fmt.Fprintf(&buf, "%d,%d\n", time.Now().UnixNano(), n)

			// Safety flush if buffer gets too big before the ticker fires
			if buf.Len() >= maxBufferBytes {
				f.WriteString(buf.String())
				buf.Reset()
			}

		case <-ticker.C:
			// Time to write to disk
			if buf.Len() > 0 {
				f.WriteString(buf.String())
				buf.Reset()
			}
		}
	}
}

//
// ───────────────────────── Arrival Generators ─────────────────────────
//
var globalRNG = rand.New(rand.NewSource(0))

func Arrival(mode string, lambda float64) func() time.Duration {
	switch mode {
	case "constant":
		interval := time.Duration((1.0 / lambda) * float64(time.Second))
		return func() time.Duration {
			return interval
		}

	case "poisson":
		return func() time.Duration {
			u := globalRNG.Float64()  // same as dist(gen)
			inter := -math.Log(u) / lambda
			return time.Duration(inter * float64(time.Second))
		}

	case "point":
		return func() time.Duration {
			return 0
		}

	default:
		log.Fatalf("unknown mode: %s", mode)
		return nil
	}
}

//
// ───────────────────────── Orchestrator ─────────────────────────
//

// func runLoadgen(mode string, lambda float64, _ *ServerPool) {
func runLoadgen(mode string, lambda float64, useRange bool, start, end, step float64, intervalSec int) {

    rpsLog, _ := os.Create("rps.log")
    defer rpsLog.Close()

    var next time.Time

    currentLambda := lambda
    if useRange {
        currentLambda = start
        log.Printf("[LOADGEN] RPS range mode: start=%.1f end=%.1f step=%.1f interval=%ds",
            start, end, step, intervalSec)
    }

    interGen := Arrival(mode, currentLambda)
    nextIncrease := time.Now().Add(time.Duration(intervalSec) * time.Second)

    count := 0
    lastSec := time.Now()
    lastSecPoint := time.Now()

    for {
        // --- Increment RPS ---
        if useRange && time.Now().After(nextIncrease) {
            currentLambda += step
            if currentLambda > end {
                currentLambda = end
            }
            log.Printf("[LOADGEN] increasing RPS to %.1f", currentLambda)

            interGen = Arrival(mode, currentLambda)
            nextIncrease = time.Now().Add(time.Duration(intervalSec) * time.Second)
        }

		switch mode {

		case "constant", "poisson":
			// absolute time scheduling
			if next.IsZero() {
				next = time.Now()
			}
			next = next.Add(interGen())
			time.Sleep(time.Until(next))
			requestQueue <- struct{}{}
			count++

		case "point":
			// fire all requests at start of each second
			now := time.Now()
			if time.Since(lastSecPoint) >= time.Second {
				for i := 0; i < int(lambda); i++ {
					requestQueue <- struct{}{}
					count++
				}
				lastSecPoint = now
			}
			time.Sleep(1 * time.Millisecond)
		}

		// log achieved RPS
		if time.Since(lastSec) >= time.Second {
			rpsLog.WriteString(fmt.Sprintf("%d,%d\n", time.Now().UnixNano(), count))
			count = 0
			lastSec = time.Now()
		}
	}
}

//
// ───────────────────────── Main (clean) ─────────────────────────
//

func main() {
	urlsFlag := flag.String("u", "localhost:8001", "Comma-separated Triton gRPC URLs")
	model := flag.String("m", "resnet50", "Model")
	version := flag.String("x", "", "Model version")
	arrival := flag.String("dist", "poisson", "Arrival distribution: poisson | constant | point")
	rps := flag.Float64("rps", 10, "Target requests per second")
	batch := flag.Int("b", 1, "Batch size")
	workers := flag.Int("w", 32, "Worker pool size")
	rangeFlag := flag.String("range", "", "RPS range: start:end:step (e.g. 10:30:10)")
	incInterval := flag.Int("interval", 10, "Seconds between RPS increases when using range")

	flag.Parse()


	var useRange bool
	var start, end, step float64

	if *rangeFlag != "" {
		parts := strings.Split(*rangeFlag, ":")
		if len(parts) != 3 {
			log.Fatalf("invalid -range format, want start:end:step")
		}
		a, _ := strconv.ParseFloat(parts[0], 64)
		b, _ := strconv.ParseFloat(parts[1], 64)
		c, _ := strconv.ParseFloat(parts[2], 64)
		start, end, step = a, b, c
		useRange = true
	}


	urls := strings.Split(*urlsFlag, ",")

	// 1. server pool
	sp := newServerPool(urls)

	// 2. tensor
	input := makeRandomImage(*batch)

	// 3. latency logger
	latCh := make(chan time.Duration, 100000)
	go logLatency(latCh)
	nsCh := make(chan int, 10000) 
	go logNumServers(nsCh)
	nsCh <- len(urls) // Log initial state

	// 4. worker pool
	state := NewLoadgenState(urls)
	lis, _ := net.Listen("tcp", ":9050")
	grpcServer := grpc.NewServer()
	coordpb.RegisterLoadgenControlServer(grpcServer, &LoadgenControlServer{state: state, UnimplementedLoadgenControlServer: coordpb.UnimplementedLoadgenControlServer{}, nsCh: nsCh})
	go grpcServer.Serve(lis)
	wp := newWorkerPool(*workers, sp, *model, *version, *batch, input, latCh, state)

	// 5. run orchestrator
	log.Printf("Loadgen → dist=%s rps=%.2f workers=%d servers=%d", *arrival, *rps, *workers, len(urls))
	var _ *WorkerPool = wp
	runLoadgen(*arrival, *rps, useRange, start, end, step, *incInterval)
}
