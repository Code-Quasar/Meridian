package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/Code-Quasar/Meridian/internal/queue"
	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("⏳ Initializing consumer channel...")
	c := queue.ReadFromQueue("jobs-small", "jobs")

	// Consumer background loop
	go func() {
		for msg := range c {
			fmt.Printf("[CONSUMER] Received message: %s\n", string(msg.Key))
			time.Sleep(1 * time.Second)
		}
	}()

	// Inside your client.go file
	sharedWriter := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9093"),
		Topic:    "jobs-small",
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}

	defer sharedWriter.Close()

	producerWorkers := 10
	var wg sync.WaitGroup

	fmt.Println("Starting producer workers...")
	startTime := time.Now()

	for i := 0; i < producerWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				queue.WriteToQueue(
					sharedWriter,
					fmt.Sprintf("worker-%d-msg-%d", workerID, j),
					"{content:content}",
				)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Workers completed after %v ms. \n", time.Since(startTime))

	// Keep main alive to watch the consumer stream the output
	select {}
}
