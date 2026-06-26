package registry

import (
	"net"
	"sync"
)

type ConnRegistry struct {
	connexions map[string]net.Conn
	mu         sync.RWMutex
}

func CreateUniqueID() string {
	return "id"
}

func NewRegistry() *ConnRegistry {
	return &ConnRegistry{connexions: make(map[string]net.Conn)}
}

func (cr *ConnRegistry) AddConnection(id string, conn net.Conn) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.connexions[id] = conn
}

func (cr *ConnRegistry) DeleteConnection(conID string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	delete(cr.connexions, conID)
}

func (cr *ConnRegistry) GetConnection(connID string) (net.Conn, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	conn, exist := cr.connexions[connID]
	return conn, exist
}
