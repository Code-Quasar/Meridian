package gateway

import (
	"log"
	"net"

	"github.com/Code-Quasar/Meridian/internal/registry"
	"github.com/segmentio/kafka-go"
)

func EstablishTCP(port string, kafkaPort string, topic string) *registry.ConnRegistry {

	lst, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalln("Unable to create server")
	}

	reg := registry.NewRegistry()
	producer := &kafka.Writer{Addr: kafka.TCP(kafkaPort), Topic: topic, Balancer: &kafka.LeastBytes{}}

	go func() {
		for {
			conn, err := lst.Accept()
			if err != nil {
				log.Println("Unable to establish connection")
				continue
			}
			go handleConnection(conn, reg, producer)
		}
	}()

	return reg
}
