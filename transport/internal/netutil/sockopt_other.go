//go:build !tinygo && !(darwin || linux || freebsd || netbsd || openbsd || dragonfly)

package netutil

import "net"

// configureMulticast is a no-op where the socket options are not portable;
// same-host participants may not see each other on these platforms, so use
// Peers for them.
func ConfigureMulticast(conn *net.UDPConn, ifi *net.Interface) error { return nil }
