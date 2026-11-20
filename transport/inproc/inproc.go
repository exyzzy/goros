// Package inproc provides an in-memory transport connecting participants
// that live in the same process. It is mostly useful for tests and for
// composing several Contexts in one binary; a single Context already
// delivers intra-process traffic directly without any transport.
package inproc

import (
	"sync"

	"bitbucket.org/goros/transport"
)

// Bus is a broadcast domain shared by any number of endpoints.
type Bus struct {
	mu   sync.Mutex
	eps  map[*Endpoint]struct{}
	size int
}

// NewBus creates a bus; queue is the per-endpoint receive buffer length.
func NewBus(queue int) *Bus {
	if queue <= 0 {
		queue = 64
	}
	return &Bus{eps: map[*Endpoint]struct{}{}, size: queue}
}

// Endpoint is one participant's connection to a Bus.
type Endpoint struct {
	bus  *Bus
	ch   chan []byte
	once sync.Once
	done chan struct{}
}

// Join creates a new endpoint on the bus.
func (b *Bus) Join() *Endpoint {
	ep := &Endpoint{bus: b, ch: make(chan []byte, b.size), done: make(chan struct{})}
	b.mu.Lock()
	b.eps[ep] = struct{}{}
	b.mu.Unlock()
	return ep
}

func (e *Endpoint) Name() string { return "inproc" }

func (e *Endpoint) Send(buf []byte) error {
	select {
	case <-e.done:
		return transport.ErrClosed
	default:
	}
	e.bus.mu.Lock()
	targets := make([]*Endpoint, 0, len(e.bus.eps))
	for t := range e.bus.eps {
		if t != e {
			targets = append(targets, t)
		}
	}
	e.bus.mu.Unlock()
	for _, t := range targets {
		cp := make([]byte, len(buf))
		copy(cp, buf)
		select {
		case t.ch <- cp:
		case <-t.done:
		default: // receiver congested: drop (best effort)
		}
	}
	return nil
}

func (e *Endpoint) Recv() ([]byte, error) {
	select {
	case b := <-e.ch:
		return b, nil
	case <-e.done:
		return nil, transport.ErrClosed
	}
}

func (e *Endpoint) Close() error {
	e.once.Do(func() {
		e.bus.mu.Lock()
		delete(e.bus.eps, e)
		e.bus.mu.Unlock()
		close(e.done)
	})
	return nil
}
