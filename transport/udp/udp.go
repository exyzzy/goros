// Package udp implements a UDP transport carrying goros frames verbatim:
// lighter than RTPS, but only other goros participants understand it. Use
// it for goros-only meshes and for boards; transport/rtps is the host
// default and joins the ROS 2 graph.
//
// On hosts (standard Go) it uses IPv4 multicast. One multicast group and
// port is used per ROS domain ID, following the RTPS port formula
// (7400 + 250*domain) with an offset chosen not to collide with DDS.
//
// Under TinyGo the net package has no multicast, so the transport binds a
// unicast socket and exchanges frames with an explicit list of peers; this
// needs a board with a network driver (Pico W, ESP32, ...) configured via
// tinygo.org/x/drivers/netlink.
package udp

import (
	"net"
	"strconv"
	"sync"

	"bitbucket.org/goros/transport"
)

const (
	DefaultGroup = "239.255.0.1"
	portBase     = 7400
	portGain     = 250
	portOffset   = 5
)

// Port returns the UDP port used for a domain.
func Port(domain int) int { return portBase + portGain*domain + portOffset }

// Options configure the transport.
type Options struct {
	Domain int
	// Group is the multicast group (hosts only); default DefaultGroup.
	Group string
	// Interface selects the multicast interface (hosts only); nil picks the
	// first multicast-capable interface with an IPv4 address, preferring
	// non-loopback ones.
	Interface *net.Interface
	// Peers are additional unicast destinations (host:port) for networks
	// without multicast (VPNs, some Wi-Fi) and the only destinations under
	// TinyGo. Frames are sent to the group (if any) and to every peer.
	Peers []string
	// Listen overrides the local bind address under TinyGo (default
	// 0.0.0.0:<domain port>).
	Listen string
}

// Transport is a UDP transport.
type Transport struct {
	conn  *net.UDPConn
	group *net.UDPAddr // nil = unicast only
	peers []*net.UDPAddr
	mu    sync.Mutex
	open  bool
	buf   []byte
	name  string
	learn bool // add senders to peers (unicast mode)
}

func (t *Transport) Name() string { return t.name }

func (t *Transport) Send(buf []byte) error {
	t.mu.Lock()
	open := t.open
	t.mu.Unlock()
	if !open {
		return transport.ErrClosed
	}
	var first error
	if t.group != nil {
		if _, err := t.conn.WriteToUDP(buf, t.group); err != nil {
			first = err
		}
	}
	t.mu.Lock()
	peers := t.peers
	t.mu.Unlock()
	for _, p := range peers {
		if _, err := t.conn.WriteToUDP(buf, p); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (t *Transport) Recv() ([]byte, error) {
	n, from, err := t.conn.ReadFromUDP(t.buf)
	if err == nil && t.learn && from != nil {
		t.learnPeer(from)
	}
	if err != nil {
		t.mu.Lock()
		open := t.open
		t.mu.Unlock()
		if !open {
			return nil, transport.ErrClosed
		}
		return nil, err
	}
	out := make([]byte, n)
	copy(out, t.buf[:n])
	return out, nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.open = false
	t.mu.Unlock()
	return t.conn.Close()
}

func (t *Transport) learnPeer(a *net.UDPAddr) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.peers {
		if p.IP.Equal(a.IP) && p.Port == a.Port {
			return
		}
	}
	t.peers = append(t.peers, &net.UDPAddr{IP: a.IP, Port: a.Port})
}

func resolvePeers(peers []string) ([]*net.UDPAddr, error) {
	var out []*net.UDPAddr
	for _, p := range peers {
		a, err := net.ResolveUDPAddr("udp4", p)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

func portString(domain int) string { return strconv.Itoa(Port(domain)) }

// NewUnicast opens a plain UDP socket on listen (e.g. ":7405") that sends
// every frame to the given peers. Peers that send to us are learned
// dynamically, so a board on Wi-Fi can reach a host bridge without the
// host knowing the board's address in advance. Works on hosts and on
// TinyGo boards with a network driver.
func NewUnicast(listen string, peers []string) (*Transport, error) {
	laddr, err := net.ResolveUDPAddr("udp4", listen)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return nil, err
	}
	t := &Transport{conn: conn, open: true, buf: make([]byte, transport.MaxFrame+512), name: "udp-unicast:" + listen, learn: true}
	if t.peers, err = resolvePeers(peers); err != nil {
		conn.Close()
		return nil, err
	}
	return t, nil
}
