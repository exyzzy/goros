//go:build !tinygo

// Package netutil holds the interface and socket helpers shared by the
// host network transports.
package netutil

import "net"

// PickInterface chooses a multicast-capable IPv4 interface, preferring real
// NICs over loopback so LAN peers are reachable; loopback still works for
// same-host traffic because of IP_MULTICAST_LOOP.
func PickInterface() *net.Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var loop *net.Interface
	for i := range ifaces {
		ifi := &ifaces[i]
		if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 {
			continue
		}
		if InterfaceIPv4(ifi) == [4]byte{} {
			continue
		}
		if ifi.Flags&net.FlagLoopback != 0 {
			if loop == nil {
				loop = ifi
			}
			continue
		}
		if ifi.Flags&net.FlagPointToPoint != 0 {
			continue
		}
		return ifi
	}
	return loop
}

// InterfaceIPv4 returns the first IPv4 address of ifi, or zero.
func InterfaceIPv4(ifi *net.Interface) [4]byte {
	if ifi == nil {
		return [4]byte{}
	}
	addrs, err := ifi.Addrs()
	if err != nil {
		return [4]byte{}
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			if ip4 := ipn.IP.To4(); ip4 != nil {
				return [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]}
			}
		}
	}
	return [4]byte{}
}

// LocalIPv4s lists the IPv4 addresses of all up interfaces, the preferred
// interface (see PickInterface) first and loopback last.
func LocalIPv4s(preferred *net.Interface) []net.IP {
	var out []net.IP
	seen := map[string]bool{}
	add := func(ip net.IP) {
		if ip4 := ip.To4(); ip4 != nil && !seen[ip4.String()] {
			seen[ip4.String()] = true
			out = append(out, ip4)
		}
	}
	if preferred != nil {
		if ip := InterfaceIPv4(preferred); ip != [4]byte{} {
			add(net.IP(ip[:]))
		}
	}
	ifaces, _ := net.Interfaces()
	var loop []net.IP
	for i := range ifaces {
		ifi := &ifaces[i]
		if ifi.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ipn.IP.IsLoopback() {
				loop = append(loop, ipn.IP)
			} else if ifi.Flags&net.FlagPointToPoint == 0 && !ipn.IP.IsLinkLocalUnicast() {
				add(ipn.IP)
			}
		}
	}
	for _, ip := range loop {
		add(ip)
	}
	return out
}
