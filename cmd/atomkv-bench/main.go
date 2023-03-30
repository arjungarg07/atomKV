package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"atomkv"
)

const (
	numGoroutines = 10
	totalOps      = 100000
)

func main() {
	os.Remove("bench.db")

	db, err := atomkv.Open("bench.db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		db.Close()
		os.Remove("bench.db")
	}()

	opsPerGoroutine := totalOps / numGoroutines

	fmt.Printf("Benchmark: %d goroutines, %d total writes\n", numGoroutines, totalOps)
	fmt.Println("---")

	// Write benchmark
	var wg sync.WaitGroup
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				value := fmt.Sprintf("value-%d-%d", id, i)
				if err := db.Set(key, value); err != nil {
					fmt.Fprintf(os.Stderr, "write error: %v\n", err)
				}
			}
		}(g)
	}

	wg.Wait()
	writeDuration := time.Since(start)
	writeOPS := float64(totalOps) / writeDuration.Seconds()

	fmt.Printf("Write: %d ops in %v\n", totalOps, writeDuration)
	fmt.Printf("Write OPS: %.0f ops/sec\n", writeOPS)
	fmt.Println("---")

	// Concurrent read benchmark
	start = time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				db.Get(key)
			}
		}(g)
	}

	wg.Wait()
	readDuration := time.Since(start)
	readOPS := float64(totalOps) / readDuration.Seconds()

	fmt.Printf("Read: %d ops in %v\n", totalOps, readDuration)
	fmt.Printf("Read OPS: %.0f ops/sec\n", readOPS)
	fmt.Println("---")

	// File size
	info, _ := os.Stat("bench.db")
	fmt.Printf("File size: %.2f MB\n", float64(info.Size())/(1024*1024))
}
