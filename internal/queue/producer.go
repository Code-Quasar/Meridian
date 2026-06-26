package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Code-Quasar/Meridian/internal/grpc/gen/solver"
	"github.com/segmentio/kafka-go"
)

func WriteToQueue(w *kafka.Writer, key string, req *solver.SolveRequest) {

	value, err := json.Marshal(req)
	if err != nil {
		fmt.Printf("unable to marshal: %v\n", err)
		return
	}

	err = w.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: value,
	})

	if err != nil {
		fmt.Printf("unable to write: %v\n", err)
		return
	}
}
