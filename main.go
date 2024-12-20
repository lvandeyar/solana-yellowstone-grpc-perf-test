package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/joho/godotenv"
	"github.com/mr-tron/base58"
	pb "github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Endpoints  []*Endpoint
	Commitment string
	Insecure   bool
	Interval   time.Duration
}

type EndpointStats struct {
	name                string
	mu                  sync.Mutex
	startTime           time.Time
	timeToFirstResponse time.Duration
	gotFirstResponse    bool

	// Message counts and bytes
	msgCount           atomic.Int64
	totalBytes         atomic.Int64
	lastBytes          atomic.Int64
	lastRateUpdateTime time.Time

	// RPS tracking
	rpsHistory []float64
	minRPS     float64
	maxRPS     float64

	// Transaction tracking
	uniqueCount atomic.Int64
	matchCount  atomic.Int64
	skipCount   atomic.Int64
	errorCount  atomic.Int64

	// Latency tracking
	totalLatencySum   atomic.Int64 // in nanoseconds
	totalLatencyCount atomic.Int64
	rollingLatencies  []time.Duration
	rollingLatencyIdx int32

	totalTransactions  atomic.Int64
	transactionsPerSec float64
	droppedMsgs        atomic.Int64
}

func NewEndpointStats() *EndpointStats {
	return &EndpointStats{
		startTime:          time.Now(),
		lastRateUpdateTime: time.Now(),
		rpsHistory:         make([]float64, 0, 20),
		minRPS:             math.MaxFloat64,
		rollingLatencies:   make([]time.Duration, 1000),
	}
}

func (s *EndpointStats) RecordResponse(size int) {
	s.mu.Lock()
	if !s.gotFirstResponse {
		s.gotFirstResponse = true
		s.timeToFirstResponse = time.Since(s.startTime)
	}
	s.mu.Unlock()

	s.msgCount.Add(1)
	s.totalBytes.Add(int64(size))
}

func (s *EndpointStats) AddSample(count int64, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rps := float64(count) / duration.Seconds()
	s.rpsHistory = append(s.rpsHistory, rps)
	if len(s.rpsHistory) > 20 {
		s.rpsHistory = s.rpsHistory[1:]
	}

	if rps < s.minRPS {
		s.minRPS = rps
	}
	if rps > s.maxRPS {
		s.maxRPS = rps
	}
}

func (s *EndpointStats) RecordLatency(d time.Duration) {
	idx := atomic.AddInt32(&s.rollingLatencyIdx, 1) % int32(len(s.rollingLatencies))
	s.rollingLatencies[idx] = d
	s.totalLatencySum.Add(int64(d))
	s.totalLatencyCount.Add(1)
}

func (s *EndpointStats) GetMetrics() map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(s.lastRateUpdateTime).Seconds()

	// Calculate bytes per second
	var bytesPerSec float64
	if elapsed > 0 {
		deltaBytes := s.totalBytes.Load() - s.lastBytes.Load()
		bytesPerSec = float64(deltaBytes) / elapsed
		s.lastBytes.Store(s.totalBytes.Load())
		s.lastRateUpdateTime = now
	}

	// Calculate RPS metrics
	var avgRPS, p95RPS float64
	if len(s.rpsHistory) > 0 {
		// Calculate average RPS
		sum := 0.0
		for _, v := range s.rpsHistory {
			sum += v
		}
		avgRPS = sum / float64(len(s.rpsHistory))

		// Calculate 95th percentile RPS
		cpy := append([]float64(nil), s.rpsHistory...)
		sort.Float64s(cpy)
		idx := int(math.Ceil(0.95 * float64(len(cpy))))
		if idx >= len(cpy) {
			idx = len(cpy) - 1
		}
		p95RPS = cpy[idx]
	}

	// Calculate latency metrics
	var avgLatency time.Duration
	if count := s.totalLatencyCount.Load(); count > 0 {
		avgLatency = time.Duration(s.totalLatencySum.Load()) / time.Duration(count)
	}

	return map[string]interface{}{
		"total_messages":   s.msgCount.Load(),
		"total_bytes":      s.totalBytes.Load(),
		"bytes_per_second": bytesPerSec,
		"min_rps":          s.minRPS,
		"max_rps":          s.maxRPS,
		"avg_rps":          avgRPS,
		"p95_rps":          p95RPS,
		"unique_txns":      s.uniqueCount.Load(),
		"matched_txns":     s.matchCount.Load(),
		"skipped_txns":     s.skipCount.Load(),
		"errors":           s.errorCount.Load(),
		"avg_latency":      avgLatency,
		"time_to_first":    s.timeToFirstResponse,
	}
}

func (s *EndpointStats) IncrementError() {
	s.errorCount.Add(1)
}

func (s *EndpointStats) IncrementUnique() {
	s.uniqueCount.Add(1)
}

func (s *EndpointStats) DecrementUnique() {
	s.uniqueCount.Add(-1)
}

func (s *EndpointStats) IncrementMatched() {
	s.matchCount.Add(1)
}

func (s *EndpointStats) IncrementSkipped() {
	s.skipCount.Add(1)
}

func (s *EndpointStats) DecrementSkipped() {
	s.skipCount.Add(-1)
}

func (s *EndpointStats) calculateTPS() float64 {
	elapsed := time.Since(s.startTime).Seconds()
	if elapsed > 0 {
		return float64(s.totalTransactions.Load()) / elapsed
	}
	return 0
}

func (s *EndpointStats) GetDropRate(duration time.Duration) float64 {
	return float64(s.droppedMsgs.Load()) / duration.Seconds()
}

type Endpoint struct {
	Name       string
	Host       string
	Port       string
	Token      string
	UseTLS     bool
	Stats      *EndpointStats
	Client     pb.GeyserClient
	Connection *grpc.ClientConn
}

// Add this new type for token authentication
type tokenAuth struct {
	token string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if t.token == "" {
		return map[string]string{}, nil
	}
	return map[string]string{"x-token": t.token}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool { return true }

func (ep *Endpoint) Connect() error {
	var opts []grpc.DialOption

	if ep.UseTLS {
		creds := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	opts = append(opts, []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             20 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1024*1024*1024), // 1GB
			grpc.UseCompressor(gzip.Name),
		),
	}...)

	// Use the new tokenAuth type
	if ep.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenAuth{token: ep.Token}))
	}

	return ep.connectWithRetry(opts, MaxRetryAttempts)
}

func (ep *Endpoint) connectWithRetry(opts []grpc.DialOption, maxAttempts int) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		address := fmt.Sprintf("%s:%s", ep.Host, ep.Port)
		log.Printf("Connecting to %s at %s (Attempt %d/%d)", ep.Name, address, attempt, maxAttempts)

		if conn, err := grpc.Dial(address, opts...); err == nil {
			ep.Connection = conn
			ep.Client = pb.NewGeyserClient(conn)
			log.Printf("Successfully connected to %s", ep.Name)
			return nil
		} else {
			lastErr = err
			if attempt < maxAttempts {
				log.Printf("Failed to connect to %s: %v. Retrying in %v...", ep.Name, err, RetryDelay)
				time.Sleep(RetryDelay)
			}
		}
	}
	return fmt.Errorf("failed to connect after %d attempts: %v", maxAttempts, lastErr)
}

func (ep *Endpoint) SubscribeToRaydiumPool(ctx context.Context) (pb.Geyser_SubscribeClient, error) {
	poolAddress := "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
	commitment := pb.CommitmentLevel_CONFIRMED

	req := &pb.SubscribeRequest{
		Commitment: &commitment,
		Accounts:   map[string]*pb.SubscribeRequestFilterAccounts{},
		Slots:      map[string]*pb.SubscribeRequestFilterSlots{},
		Transactions: map[string]*pb.SubscribeRequestFilterTransactions{
			"pools": {
				AccountInclude:  []string{poolAddress},
				AccountRequired: []string{poolAddress},
			},
		},
		Entry:              map[string]*pb.SubscribeRequestFilterEntry{},
		Blocks:             map[string]*pb.SubscribeRequestFilterBlocks{},
		BlocksMeta:         map[string]*pb.SubscribeRequestFilterBlocksMeta{},
		AccountsDataSlice:  []*pb.SubscribeRequestAccountsDataSlice{},
		TransactionsStatus: map[string]*pb.SubscribeRequestFilterTransactions{},
	}

	stream, err := ep.Client.Subscribe(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription stream: %v", err)
	}

	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("failed to send subscription request: %v", err)
	}

	return stream, nil
}

func (ep *Endpoint) Receive(wg *sync.WaitGroup, txTracker *TxTracker) {
	defer wg.Done()
	defer ep.Connection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TestDurationSec)*time.Second)
	defer cancel()

	stream, err := ep.SubscribeToRaydiumPool(ctx)
	if err != nil {
		log.Printf("❌ [GRPC] Failed to subscribe to %s: %v", ep.Name, err)
		return
	}

	msgChan := make(chan *pb.SubscribeUpdate, MsgBufferSize)

	// Start message processors
	numProcessors := runtime.NumCPU()
	for i := 0; i < numProcessors; i++ {
		go func() {
			for {
				select {
				case msg, ok := <-msgChan:
					if !ok {
						return
					}
					if msg != nil {
						if tx := msg.GetTransaction(); tx != nil {
							ep.Stats.totalTransactions.Add(1)
							sig := base58.Encode(tx.GetTransaction().GetSignature())

							// Only log every 100th transaction with key details
							if ep.Stats.totalTransactions.Load()%100 == 0 {
								log.Printf("[%s] Tx %d: Sig=%s",
									ep.Name,
									ep.Stats.totalTransactions.Load(),
									sig[:8])
							}

							txTracker.AddTransactionFrom(ep.Name, sig, proto.Size(tx))
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Message receiver
	go func() {
		defer close(msgChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := stream.Recv()
				if err != nil {
					ep.Stats.IncrementError()
					log.Printf("Failed to receive message from %s: %v (type: %T)", ep.Name, err, err)
					return
				}

				select {
				case msgChan <- msg:
				default:
					ep.Stats.droppedMsgs.Add(1)
					if ep.Stats.droppedMsgs.Load()%1000 == 0 {
						log.Printf("Warning: dropped %d messages due to backpressure on %s",
							ep.Stats.droppedMsgs.Load(), ep.Name)
					}
				}
			}
		}
	}()

	<-ctx.Done()
	stream.CloseSend()
}

type TransactionState int

const (
	StateUnique TransactionState = iota
	StateMatched
	StateExpired
)

type TransactionSeen struct {
	firstEndpoint  string
	firstTime      time.Time
	seenBy         map[string]time.Time
	state          TransactionState
	skippedCounted bool
	size           int // Track transaction size
}

type TxTracker struct {
	mu          sync.RWMutex
	txMap       map[string]*TransactionSeen
	endpoints   []*Endpoint
	epNames     []string
	timeout     time.Duration
	cleanupTick time.Duration

	// Performance optimizations
	endpointMap map[string]*Endpoint // Fast endpoint lookup
	metrics     *TxTrackerMetrics
}

type TxTrackerMetrics struct {
	mu              sync.RWMutex
	totalTx         int64
	uniqueTx        int64
	matchedTx       int64
	expiredTx       int64
	avgLatency      time.Duration
	latencyCount    int64
	latencySum      time.Duration
	cleanupCount    int64
	lastCleanupTime time.Time
}

func NewTxTracker(endpoints []*Endpoint, timeout time.Duration) *TxTracker {
	endpointMap := make(map[string]*Endpoint, len(endpoints))
	epNames := make([]string, len(endpoints))

	for i, ep := range endpoints {
		epNames[i] = ep.Name
		endpointMap[ep.Name] = ep
	}

	tracker := &TxTracker{
		txMap:       make(map[string]*TransactionSeen),
		endpoints:   endpoints,
		epNames:     epNames,
		endpointMap: endpointMap,
		timeout:     timeout,
		cleanupTick: time.Second,
		metrics:     &TxTrackerMetrics{lastCleanupTime: time.Now()},
	}

	// Start cleanup goroutine
	go tracker.cleanupLoop()

	return tracker
}

func (t *TxTracker) cleanupLoop() {
	ticker := time.NewTicker(t.cleanupTick)
	defer ticker.Stop()

	for range ticker.C {
		t.FinalizeOldTransactions()
	}
}

func (t *TxTracker) AddTransactionFrom(endpoint, sig string, size int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.txMap[sig]
	now := time.Now()

	if !exists {
		// First time seeing this transaction
		entry = &TransactionSeen{
			firstEndpoint: endpoint,
			firstTime:     now,
			seenBy:        make(map[string]time.Time, len(t.endpoints)),
			state:         StateUnique,
			size:          size,
		}
		entry.seenBy[endpoint] = now
		t.txMap[sig] = entry

		if ep := t.endpointMap[endpoint]; ep != nil {
			ep.Stats.IncrementUnique()
		}
		t.metrics.totalTx++
		t.metrics.uniqueTx++

	} else {
		if _, seen := entry.seenBy[endpoint]; !seen {
			entry.seenBy[endpoint] = now
			latency := now.Sub(entry.firstTime)

			switch entry.state {
			case StateUnique:
				t.handleUniqueToMatched(entry, endpoint, latency)
			case StateMatched:
				t.handleAdditionalMatch(entry, endpoint, latency)
			}
		}
	}

	// Cleanup if all endpoints have seen it
	if len(entry.seenBy) == len(t.endpoints) {
		delete(t.txMap, sig)
	}
}

func (t *TxTracker) handleUniqueToMatched(entry *TransactionSeen, endpoint string, latency time.Duration) {
	// Update first endpoint stats
	if firstEp := t.endpointMap[entry.firstEndpoint]; firstEp != nil {
		firstEp.Stats.DecrementUnique()
		firstEp.Stats.IncrementMatched()
	}

	// Update current endpoint stats
	if currentEp := t.endpointMap[endpoint]; currentEp != nil {
		currentEp.Stats.IncrementMatched()
		currentEp.Stats.RecordLatency(latency)
	}

	// Update skipped counts for other endpoints
	for name, ep := range t.endpointMap {
		if name != entry.firstEndpoint && name != endpoint {
			ep.Stats.IncrementSkipped()
		}
	}

	// Update metrics
	t.metrics.mu.Lock()
	t.metrics.matchedTx++
	t.metrics.uniqueTx--
	t.metrics.latencySum += latency
	t.metrics.latencyCount++
	t.metrics.avgLatency = t.metrics.latencySum / time.Duration(t.metrics.latencyCount)
	t.metrics.mu.Unlock()

	entry.skippedCounted = true
	entry.state = StateMatched
}

func (t *TxTracker) handleAdditionalMatch(entry *TransactionSeen, endpoint string, latency time.Duration) {
	if currentEp := t.endpointMap[endpoint]; currentEp != nil {
		currentEp.Stats.DecrementSkipped()
		currentEp.Stats.IncrementMatched()
		currentEp.Stats.RecordLatency(latency)
	}

	// Update metrics
	t.metrics.mu.Lock()
	t.metrics.latencySum += latency
	t.metrics.latencyCount++
	t.metrics.avgLatency = t.metrics.latencySum / time.Duration(t.metrics.latencyCount)
	t.metrics.mu.Unlock()
}

func (t *TxTracker) GetMetrics() map[string]interface{} {
	t.metrics.mu.RLock()
	defer t.metrics.mu.RUnlock()

	return map[string]interface{}{
		"total_transactions":   t.metrics.totalTx,
		"unique_transactions":  t.metrics.uniqueTx,
		"matched_transactions": t.metrics.matchedTx,
		"expired_transactions": t.metrics.expiredTx,
		"average_latency":      t.metrics.avgLatency,
		"cleanup_count":        t.metrics.cleanupCount,
		"last_cleanup":         time.Since(t.metrics.lastCleanupTime),
	}
}

func (t *TxTracker) FinalizeOldTransactions() {
	t.mu.Lock()
	now := time.Now()
	var toDelete []string

	for sig, tx := range t.txMap {
		if now.Sub(tx.firstTime) > t.timeout {
			toDelete = append(toDelete, sig)
			if tx.state == StateUnique {
				if ep := t.endpointMap[tx.firstEndpoint]; ep != nil {
					ep.Stats.DecrementUnique()
				}
				t.metrics.uniqueTx--
				t.metrics.expiredTx++
			}
		}
	}

	for _, sig := range toDelete {
		delete(t.txMap, sig)
	}

	t.metrics.cleanupCount++
	t.metrics.lastCleanupTime = now
	t.mu.Unlock()
}

const (
	MaxRetryAttempts = 3
	RetryDelay       = 5 * time.Second
	TestDurationSec  = 30    // Test duration in seconds
	MsgBufferSize    = 50000 // Increased buffer size
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	testDuration := time.Duration(TestDurationSec) * time.Second
	timeoutDuration := testDuration / 4

	cfg := Config{
		Endpoints: []*Endpoint{
			{
				Name:   "Provider1",
				Host:   os.Getenv("PROVIDER1_HOST"),
				Port:   os.Getenv("PROVIDER1_PORT"),
				Token:  os.Getenv("PROVIDER1_TOKEN"),
				UseTLS: os.Getenv("PROVIDER1_USE_TLS") == "true",
				Stats:  NewEndpointStats(),
			},
			{
				Name:   "Quicknode",
				Host:   os.Getenv("QUICKNODE_HOST"),
				Port:   os.Getenv("QUICKNODE_PORT"),
				Token:  os.Getenv("QUICKNODE_TOKEN"),
				UseTLS: os.Getenv("QUICKNODE_USE_TLS") == "true",
				Stats:  NewEndpointStats(),
			},
		},
		Interval:   time.Second,
		Commitment: "finalized",
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(TestDurationSec)*time.Second)
	defer cancel()

	txTracker := NewTxTracker(cfg.Endpoints, timeoutDuration)
	testEnd := make(chan bool)

	// Update metrics reporting for two providers
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				continue
			case <-testEnd:
				metrics := txTracker.GetMetrics()
				p1 := cfg.Endpoints[0].Stats
				qn := cfg.Endpoints[1].Stats

				fmt.Printf("\n🏁 FINAL PERFORMANCE REPORT (%ds test)\n", TestDurationSec)
				fmt.Println("═══════════════════════════════════════")
				fmt.Printf("\n%-20s │ %-15s │ %-15s │ %-15s\n",
					"Metric", "Provider1", "Quicknode", "Difference")
				fmt.Println("──────────────────────────────────────────────────────────")

				// TPS
				p1TPS := p1.calculateTPS()
				qnTPS := qn.calculateTPS()
				fmt.Printf("%-20s │ %-15.2f │ %-15.2f │ %+15.2f\n",
					"TPS", p1TPS, qnTPS, p1TPS-qnTPS)

				// Total Transactions
				p1Tx := p1.totalTransactions.Load()
				qnTx := qn.totalTransactions.Load()
				fmt.Printf("%-20s │ %-15d │ %-15d │ %+15d\n",
					"Total Transactions", p1Tx, qnTx, p1Tx-qnTx)

				// Unique Transactions
				fmt.Printf("%-20s │ %-15d │ %-15d │ %+15d\n",
					"Unique", p1.uniqueCount.Load(), qn.uniqueCount.Load(),
					p1.uniqueCount.Load()-qn.uniqueCount.Load())

				// Matched Transactions
				fmt.Printf("%-20s │ %-15d │ %-15d │ %+15d\n",
					"Matched", p1.matchCount.Load(), qn.matchCount.Load(),
					p1.matchCount.Load()-qn.matchCount.Load())

				// Average Latency
				fmt.Printf("%-20s │ %-15v │ %-15s │ %-15s\n",
					"Avg Latency", metrics["average_latency"], "-", "-")

				// Drop Rate/s
				fmt.Printf("%-20s │ %-15.2f │ %-15.2f │ %+15.2f\n",
					"Drop Rate/s", p1.GetDropRate(testDuration), qn.GetDropRate(testDuration),
					p1.GetDropRate(testDuration)-qn.GetDropRate(testDuration))

				// Error Count
				fmt.Printf("%-20s │ %-15d │ %-15d │ %+15d\n",
					"Error Count", p1.errorCount.Load(), qn.errorCount.Load(),
					p1.errorCount.Load()-qn.errorCount.Load())

				return
			}
		}
	}()

	// Start cleanup goroutine
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			txTracker.FinalizeOldTransactions()
		}
	}()

	// Connect and start receiving from both endpoints in parallel
	for _, ep := range cfg.Endpoints {
		wg.Add(1)
		go func(endpoint *Endpoint) {
			if err := endpoint.Connect(); err != nil {
				log.Printf("Error connecting to %s: %v", endpoint.Name, err)
				wg.Done()
				return
			}
			endpoint.Receive(&wg, txTracker)
		}(ep)
	}

	<-ctx.Done() // Wait for context timeout
	close(testEnd)
	wg.Wait()
}
