package gateway

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/Code-Quasar/Meridian/internal/queue"
	"github.com/Code-Quasar/Meridian/internal/registry"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func IsValidReq(req string) bool {
	return true
}
func handleConnection(conn net.Conn, reg *registry.ConnRegistry, producer *kafka.Writer) {

	// Handle Auth

	// Add to registry (create unique ID)
	id, _ := uuid.NewUUID() // must read UUID doc
	msg := make(chan string, 200)
	connInfo := registry.ConnInfo{Conn: conn, Response: msg}
	reg.AddConnection(id.String(), connInfo)

	// Clean when complete
	defer conn.Close()
	defer reg.DeleteConnection(id.String())

	// handle messages
	err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		log.Println("Failed to set deadline:", err)
		return
	}

	scanner := bufio.NewScanner(conn)
	var validReq string

	// scan if valid do the work and
	for scanner.Scan() {
		msg := scanner.Text()

		if IsValidReq(msg) {
			validReq = msg
			err := conn.SetReadDeadline(time.Time{})
			if err != nil {
				return
			}
			queue.WriteToQueue(producer, id.String(), validReq)
			break
		} else {
			fmt.Fprintf(conn, "Client %s sent an invalid request payload\n", id.String())
		}
	}

	// send back results to the client
	for msg := range connInfo.Response {
		_, err := fmt.Fprintf(conn, msg)
		if err != nil {
			log.Printf("Failed to write to client %s, connection likely lost: %v\n", id.String(), err)
			break
		}
	}
}
