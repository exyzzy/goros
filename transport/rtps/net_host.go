//go:build !tinygo

package rtps

import (
	"errors"
	"net"
	"strconv"

	"bitbucket.org/goros/transport/internal/netutil"
)

func (t *Transport) openSockets() error {
	ifi := t.opts.Interface
	if ifi == nil {
		ifi = netutil.PickInterface()
	}
	// Unicast socket on the first free metatraffic port; it carries
	// metatraffic and user data alike.
	first, last := 0, maxPartIDs-1
	if t.opts.ParticipantID > 0 {
		first, last = t.opts.ParticipantID, t.opts.ParticipantID
	}
	var err error
	for pid := first; pid <= last; pid++ {
		port := MetatrafficUnicastPort(t.domain, pid)
		listen := t.opts.Listen
		if listen == "" {
			listen = ":" + strconv.Itoa(port)
		}
		var laddr *net.UDPAddr
		laddr, err = net.ResolveUDPAddr("udp4", listen)
		if err != nil {
			return err
		}
		t.uni, err = net.ListenUDP("udp4", laddr)
		if err == nil {
			t.uniPort = t.uni.LocalAddr().(*net.UDPAddr).Port
			break
		}
	}
	if t.uni == nil {
		return errors.New("rtps: no free participant port: " + err.Error())
	}
	_ = t.uni.SetReadBuffer(4 << 20)
	_ = t.uni.SetWriteBuffer(1 << 20)
	if !t.opts.NoMulticast {
		gaddr := &net.UDPAddr{IP: net.ParseIP(DefaultGroup), Port: SPDPMulticastPort(t.domain)}
		t.multi, err = net.ListenMulticastUDP("udp4", ifi, gaddr)
		if err != nil {
			t.uni.Close()
			return err
		}
		_ = t.multi.SetReadBuffer(1 << 20)
		if err := netutil.ConfigureMulticast(t.multi, ifi); err != nil {
			t.closeSockets()
			return err
		}
		t.multiAddr = gaddr
	}
	// Advertise one address: the preferred interface's. Peers pick a
	// locator from the list more or less arbitrarily (Cyclone DDS takes
	// the first it scores equal), and link-local or VPN addresses are
	// often unreachable even from the same host. Options.Advertise
	// overrides this on multi-homed robots.
	ips := t.opts.Advertise
	if len(ips) == 0 {
		if ip := netutil.InterfaceIPv4(ifi); ip != [4]byte{} {
			ips = []net.IP{net.IP(ip[:])}
		}
	}
	if len(ips) == 0 {
		ips = []net.IP{net.IPv4(127, 0, 0, 1)}
	}
	for _, ip := range ips {
		t.locUni = append(t.locUni, udpv4Locator(ip, t.uniPort))
	}
	return nil
}
