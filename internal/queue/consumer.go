package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func sendToChannel(reader *kafka.Reader, callback func(kafka.Message)) {
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			continue
		}
		callback(m)
	}
}

func ReadFromQueue(topic string, groupID string, callback func(kafka.Message)) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:9093"},
		Topic:   topic,
		GroupID: groupID,
	})

	go func() {
		defer r.Close()
		sendToChannel(r, callback)
	}()
}
