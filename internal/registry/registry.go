package registry

import (
	"net"
	"sync"
)

type ConnInfo struct {
	Conn     net.Conn
	Response chan string
}

type ConnRegistry struct {
	connexions map[string]ConnInfo
	mu         sync.RWMutex
}

func NewRegistry() *ConnRegistry {
	return &ConnRegistry{connexions: make(map[string]ConnInfo)}
}

func (cr *ConnRegistry) AddConnection(id string, connInfo ConnInfo) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.connexions[id] = connInfo
}

func (cr *ConnRegistry) DeleteConnection(conID string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	delete(cr.connexions, conID)
}

func (cr *ConnRegistry) GetConnection(connID string) (ConnInfo, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	conn, exist := cr.connexions[connID]
	return conn, exist
}
