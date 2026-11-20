//go:build tinygo

package goros

import "bitbucket.org/goros/transport"

// defaultTransport: TinyGo targets have no multicast networking; attach a
// transport explicitly (transport/serial over UART, or transport/udp with a
// netdev driver and unicast peers).
func defaultTransport(domain int) (transport.Transport, error) { return nil, nil }
