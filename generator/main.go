package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coinbase-o2/generator/internal"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	bootstrap := flag.String("bootstrap", "localhost:9094", "Kafka bootstrap servers")
	rate := flag.Int("rate", 100, "Events per second (total across all topics)")
	duration := flag.Duration("duration", 0, "Run duration (0 = infinite)")
	flag.Parse()

	log.Printf("O2 Event Generator — bootstrap=%s rate=%d/s", *bootstrap, *rate)

	// Create Kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(*bootstrap),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// Verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Ping(ctx)
	cancel()
	if err != nil {
		log.Fatalf("Cannot reach Kafka at %s: %v", *bootstrap, err)
	}
	log.Println("Connected to Kafka")

	// Setup graceful shutdown
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Shutting down...")
		cancel()
	}()

	// Optional duration limit
	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			log.Printf("Duration %v reached, shutting down", *duration)
			cancel()
		}()
	}

	// Distribute rate across 4 topics (weighted)
	// transactions: 40%, logins: 25%, transfers: 20%, claims: 15%
	txRate := *rate * 40 / 100
	loginRate := *rate * 25 / 100
	transferRate := *rate * 20 / 100
	claimRate := *rate - txRate - loginRate - transferRate // remainder

	var totalSent atomic.Int64
	var wg sync.WaitGroup

	// Launch goroutines for each topic
	generators := []struct {
		name string
		fn   func() *kgo.Record
		rate int
	}{
		{"transactions", internal.GenTransaction, txRate},
		{"logins", internal.GenLogin, loginRate},
		{"transfers", internal.GenTransfer, transferRate},
		{"claims", internal.GenClaim, claimRate},
	}

	for _, g := range generators {
		wg.Add(1)
		go func(name string, genFn func() *kgo.Record, ratePerSec int) {
			defer wg.Done()
			if ratePerSec <= 0 {
				ratePerSec = 1
			}
			interval := time.Second / time.Duration(ratePerSec)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			log.Printf("  [%s] generating at %d events/sec", name, ratePerSec)

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					record := genFn()
					client.Produce(ctx, record, func(r *kgo.Record, err error) {
						if err != nil {
							log.Printf("  [%s] produce error: %v", name, err)
						} else {
							totalSent.Add(1)
						}
					})
				}
			}
		}(g.name, g.fn, g.rate)
	}

	// Stats printer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sent := totalSent.Load()
				elapsed := time.Since(start).Seconds()
				fmt.Printf("\r[%s] Total: %d events | Avg: %.0f/s    ",
					time.Now().Format("15:04:05"), sent, float64(sent)/elapsed)
			}
		}
	}()

	wg.Wait()
	log.Printf("\nDone. Total events sent: %d", totalSent.Load())
}
