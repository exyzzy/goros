//go:build !tinygo

package udp

import (
	"net"

	"bitbucket.org/goros/transport"
	"bitbucket.org/goros/transport/internal/netutil"
)

// New opens a multicast transport for opts.
func New(opts Options) (*Transport, error) {
	group := opts.Group
	if group == "" {
		group = DefaultGroup
	}
	gaddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(group, portString(opts.Domain)))
	if err != nil {
		return nil, err
	}
	ifi := opts.Interface
	if ifi == nil {
		ifi = netutil.PickInterface()
	}
	conn, err := net.ListenMulticastUDP("udp4", ifi, gaddr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(1 << 20)
	// Go disables multicast loopback by default, which would hide other
	// participants on the same host. Re-enable it and pin the outgoing
	// interface to the one we joined on.
	if err := netutil.ConfigureMulticast(conn, ifi); err != nil {
		conn.Close()
		return nil, err
	}
	t := &Transport{conn: conn, group: gaddr, open: true, buf: make([]byte, transport.MaxFrame+512), name: "udp:" + gaddr.String()}
	if t.peers, err = resolvePeers(opts.Peers); err != nil {
		conn.Close()
		return nil, err
	}
	return t, nil
}
