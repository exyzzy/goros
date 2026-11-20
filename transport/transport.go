// Package transport defines how goros participants exchange frames.
//
// The core library is transport-agnostic: anything that can carry opaque,
// bounded byte frames between participants can be plugged in. The wire
// format of those frames (see Frame) is a small, CDR-encoded envelope that
// carries discovery announcements, topic data and service traffic.
//
// The frames themselves are not DDS-RTPS. The rtps sub-package is a
// Transport that translates them to and from RTPS on the fly, which is how
// goros participants join an rmw_fastrtps/rmw_cyclonedds graph; the udp,
// serial and inproc transports carry the frames verbatim.
package transport

import "errors"

// Transport carries frames to and from every other participant reachable
// over the same medium (multicast group, serial link, process, ...).
type Transport interface {
	// Send broadcasts a frame. Implementations must not retain buf.
	Send(buf []byte) error
	// Recv blocks until a frame arrives and returns it, or returns an error
	// once the transport is closed. The returned slice is owned by the
	// caller.
	Recv() ([]byte, error)
	// Close releases resources and unblocks pending Recv calls.
	Close() error
	// Name returns a short human readable description.
	Name() string
}

// ErrClosed is returned by Recv/Send after Close.
var ErrClosed = errors.New("transport: closed")

// MaxFrame is the largest frame size the built-in transports accept.
// UDP is limited by the datagram size; serial links are limited by RAM on
// the far end.
const MaxFrame = 65000
