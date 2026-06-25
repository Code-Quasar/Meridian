package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func WriteToQueue(w *kafka.Writer, key string, value string) {

	err := w.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(value)})

	if err != nil {
		fmt.Printf("❌ IMMEDIATE WRITE ERROR: %v\n", err)
		return
	}
}
