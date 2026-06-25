package queue

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func sendToChannel(reader *kafka.Reader, channel chan<- kafka.Message) {
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
			continue
		}
		channel <- m
	}
}

func ReadFromQueue(topic string, groupID string) <-chan kafka.Message {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:9093"},
		Topic:   topic,
		GroupID: groupID,
	})

	messages := make(chan kafka.Message, 200)

	go func() {
		defer r.Close()
		sendToChannel(r, messages)
	}()

	return messages
}
