//go:build !tinygo && (darwin || linux || freebsd || netbsd || openbsd || dragonfly)

package netutil

import (
	"net"
	"syscall"
)

func ConfigureMulticast(conn *net.UDPConn, ifi *net.Interface) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	err = raw.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, 1)
		if serr != nil {
			return
		}
		if ip := InterfaceIPv4(ifi); ip != [4]byte{} {
			serr = syscall.SetsockoptInet4Addr(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_IF, ip)
		}
	})
	if err != nil {
		return err
	}
	return serr
}
