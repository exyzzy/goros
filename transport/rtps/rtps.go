// Package rtps implements the DDS-RTPS wire protocol as a goros transport,
// so goros participants appear directly in a ROS 2 graph next to rclcpp
// and rclpy nodes without a bridge.
//
// It speaks RTPS 2.3 over UDPv4: SPDP participant discovery on the
// standard multicast group and ports, SEDP endpoint discovery, DATA /
// DATA_FRAG samples, and the reliable HEARTBEAT / ACKNACK / GAP machinery.
// ROS 2 naming conventions (rt/, rq/, rr/ topic prefixes, pkg::msg::dds_::
// type names) and the ros_discovery_info node announcement are applied so
// `ros2 topic list`, `ros2 node list`, `ros2 topic echo` and `ros2 service
// call` all work against goros nodes.
//
// Services follow the rmw_fastrtps convention (request correlation through
// the RTPS sample identity, reply carries PID_RELATED_SAMPLE_IDENTITY),
// which is also what rmw_connextdds uses. rmw_cyclonedds embeds a different
// request header in the payload and is not interoperable for services, as
// it is not with Fast DDS itself; topics work with every vendor.
//
// Internally the transport is a small RTPS participant that hosts every
// goros participant announcing through it: one Context normally, or every
// board behind a `goros bridge`. goros frames arriving on Send are turned
// into RTPS writers/readers and samples; discovery and samples from DDS
// peers are turned back into goros Hello/Data/Request/Response frames on
// Recv.
package rtps

import (
	"crypto/rand"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"bitbucket.org/goros/cdr"
	"bitbucket.org/goros/transport"
)

// Port formula constants (RTPS 9.6.1.1).
const (
	portBase     = 7400
	domainGain   = 250
	partGain     = 2
	d0           = 0  // SPDP multicast
	d1           = 10 // metatraffic unicast
	maxPartIDs   = 120
	DefaultGroup = "239.255.0.1"

	announcePeriod   = time.Second
	heartbeatPeriod  = time.Second
	tickPeriod       = 200 * time.Millisecond
	ownerLease       = 15 * time.Second
	defaultLease     = 12 * time.Second
	spdpLeaseDefault = 100 * time.Second
	maxDatagram      = 65000
	fragSize         = 60000
	fragTimeout      = 5 * time.Second
	pendingTimeout   = 5 * time.Minute
	outQueue         = 2048
)

// SPDPMulticastPort returns the SPDP multicast port of a domain.
func SPDPMulticastPort(domain int) int { return portBase + domainGain*domain + d0 }

// MetatrafficUnicastPort returns the metatraffic unicast port of a
// participant id in a domain.
func MetatrafficUnicastPort(domain, participant int) int {
	return portBase + domainGain*domain + d1 + partGain*participant
}

// Options configure New.
type Options struct {
	Domain int
	// Interface selects the multicast interface (hosts only); nil picks the
	// first multicast-capable non-loopback IPv4 interface.
	Interface *net.Interface
	// Peers are hosts (or host:port) that receive unicast participant
	// announcements in addition to the multicast group, for networks
	// without multicast; the equivalent of ROS_STATIC_PEERS. A bare host
	// is announced to on the SPDP port and the first four metatraffic
	// unicast ports of the domain.
	Peers []string
	// NoMulticast disables the multicast socket (hosts).
	NoMulticast bool
	// ParticipantID fixes the participant id (and so the unicast port);
	// -1 or 0 with Auto picks the first free one.
	ParticipantID int
	// Advertise lists the addresses put in our locators. Default: every
	// local IPv4 address, preferred interface first. Required under
	// TinyGo.
	Advertise []net.IP
	// Listen overrides the unicast bind address (TinyGo: ":port").
	Listen string
	// Name is the participant name shown to DDS tools (default "goros").
	Name string
	// Lease is the participant lease duration we advertise (default 12s;
	// announcements go out every second).
	Lease time.Duration
	// Debug receives protocol trace lines when set.
	Debug func(string)
}

// Transport is an RTPS participant acting as a goros transport.
type Transport struct {
	opts   Options
	domain int
	prefix guidPrefix
	guid   transport.GUID
	name   string

	multi     *net.UDPConn // SPDP multicast listener (may be nil)
	multiAddr *net.UDPAddr // SPDP multicast destination (nil without multicast)
	uni       *net.UDPConn
	uniPort   int
	locUni    []locator
	peers     []*net.UDPAddr

	mu   sync.Mutex
	enc  *cdr.Encoder
	mb   mbuf
	out  chan []byte
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	nextEntity    uint32
	writers       map[writerKey]*writer
	writersByGUID map[transport.GUID]*writer
	readers       map[readerKey]*reader
	readersByGUID map[transport.GUID]*reader
	owners        map[transport.GUID]*owner
	remotes       map[guidPrefix]*remoteParticipant
	proxies       map[transport.GUID]*writerProxy
	frags         map[fragKey]*fragBuf
	inbound       map[uint64]*inboundRequest
	inboundSeq    uint64

	spdpWriter *writer
	sedpPub    *writer
	sedpSub    *writer
	infoWriter *writer
	infoReader *reader
	spdpSN     int64

	lastAnnounce   time.Time
	lastHeartbeat  time.Time
	discoveryDirty bool
}

// New creates an RTPS participant in opts.Domain.
func New(opts Options) (*Transport, error) {
	if opts.Lease <= 0 {
		opts.Lease = defaultLease
	}
	if opts.Name == "" {
		opts.Name = "goros"
	}
	t := &Transport{
		opts:          opts,
		domain:        opts.Domain,
		enc:           cdr.NewEncoder(),
		out:           make(chan []byte, outQueue),
		done:          make(chan struct{}),
		writers:       map[writerKey]*writer{},
		writersByGUID: map[transport.GUID]*writer{},
		readers:       map[readerKey]*reader{},
		readersByGUID: map[transport.GUID]*reader{},
		owners:        map[transport.GUID]*owner{},
		remotes:       map[guidPrefix]*remoteParticipant{},
		proxies:       map[transport.GUID]*writerProxy{},
		frags:         map[fragKey]*fragBuf{},
		inbound:       map[uint64]*inboundRequest{},
	}
	copy(t.prefix[:2], vendorID[:])
	if _, err := rand.Read(t.prefix[2:]); err != nil {
		x := uint64(time.Now().UnixNano()) | 1
		for i := 2; i < len(t.prefix); i++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			t.prefix[i] = byte(x)
		}
	}
	t.guid = makeGUID(t.prefix, entityParticipant)
	if err := t.openSockets(); err != nil {
		return nil, err
	}
	t.name = "rtps:domain" + strconv.Itoa(t.domain) + ":" + strconv.Itoa(t.uniPort)
	for _, p := range opts.Peers {
		addrs, err := resolvePeer(p, t.domain)
		if err != nil {
			t.closeSockets()
			return nil, err
		}
		t.peers = append(t.peers, addrs...)
	}
	t.initBuiltin()
	if t.multi != nil {
		t.wg.Add(1)
		go t.recvLoop(t.multi)
	}
	t.wg.Add(2)
	go t.recvLoop(t.uni)
	go t.tickLoop()
	t.mu.Lock()
	t.announce()
	t.mu.Unlock()
	return t, nil
}

func resolvePeer(p string, domain int) ([]*net.UDPAddr, error) {
	if _, _, err := net.SplitHostPort(p); err == nil {
		a, err := net.ResolveUDPAddr("udp4", p)
		if err != nil {
			return nil, err
		}
		return []*net.UDPAddr{a}, nil
	}
	ips, err := net.LookupIP(p)
	if err != nil {
		return nil, err
	}
	var out []*net.UDPAddr
	for _, ip := range ips {
		if ip.To4() == nil {
			continue
		}
		out = append(out, &net.UDPAddr{IP: ip, Port: SPDPMulticastPort(domain)})
		for pid := 0; pid < 4; pid++ {
			out = append(out, &net.UDPAddr{IP: ip, Port: MetatrafficUnicastPort(domain, pid)})
		}
		break
	}
	if len(out) == 0 {
		return nil, errors.New("rtps: no IPv4 address for peer " + p)
	}
	return out, nil
}

// GUID returns the RTPS participant GUID.
func (t *Transport) GUID() transport.GUID { return t.guid }

// Name implements transport.Transport.
func (t *Transport) Name() string { return t.name }

// Send implements transport.Transport: goros frames become RTPS traffic.
func (t *Transport) Send(buf []byte) error {
	select {
	case <-t.done:
		return transport.ErrClosed
	default:
	}
	f, err := transport.Decode(buf)
	if err != nil {
		return nil // not a goros frame; nothing to translate
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch f.Kind {
	case transport.KindHello:
		if f.Participant != nil && int(f.Participant.Domain) == t.domain {
			t.applyHello(f.From, f.Participant)
		}
	case transport.KindBye:
		t.removeOwner(f.From)
	case transport.KindData:
		if w := t.writers[writerKey{f.From, ddsTopic(f.Topic)}]; w != nil {
			t.write(w, f.Payload, nil, nil)
		}
	case transport.KindDataReplay:
		// Transient-local history is replayed by the RTPS writer itself.
	case transport.KindRequest:
		if w := t.writers[writerKey{f.From, ddsRequestTopic(f.Topic)}]; w != nil {
			sn := t.write(w, f.Payload, nil, nil)
			w.pending[sn] = pendingRequest{owner: f.From, seq: f.Seq, at: time.Now()}
		}
	case transport.KindResponse:
		t.sendResponse(f)
	}
	return nil
}

// Recv implements transport.Transport: DDS traffic becomes goros frames.
func (t *Transport) Recv() ([]byte, error) {
	select {
	case b := <-t.out:
		return b, nil
	case <-t.done:
		return nil, transport.ErrClosed
	}
}

// Close implements transport.Transport. It announces the participant's
// departure first.
func (t *Transport) Close() error {
	t.once.Do(func() {
		t.mu.Lock()
		t.announceDispose()
		t.mu.Unlock()
		close(t.done)
		t.closeSockets()
	})
	t.wg.Wait()
	return nil
}

func (t *Transport) closeSockets() {
	if t.multi != nil {
		t.multi.Close()
	}
	if t.uni != nil {
		t.uni.Close()
	}
}

func (t *Transport) debugf(parts ...string) {
	if t.opts.Debug == nil {
		return
	}
	s := ""
	for _, p := range parts {
		s += p
	}
	t.opts.Debug(s)
}

// emit queues a goros frame for Recv.
func (t *Transport) emit(f *transport.Frame) {
	b := f.Encode(t.enc)
	cp := make([]byte, len(b))
	copy(cp, b)
	select {
	case t.out <- cp:
	default:
		t.debugf("recv queue full, dropping ", strconv.Itoa(int(f.Kind)))
	}
}

func (t *Transport) recvLoop(c *net.UDPConn) {
	defer t.wg.Done()
	buf := make([]byte, 65536)
	for {
		n, src, err := c.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-t.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		t.mu.Lock()
		if t.opts.Debug != nil {
			t.debugf("rx on ", c.LocalAddr().String())
		}
		t.handlePacket(buf[:n], src)
		t.mu.Unlock()
	}
}

func (t *Transport) tickLoop() {
	defer t.wg.Done()
	tk := time.NewTicker(tickPeriod)
	defer tk.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-tk.C:
		}
		t.mu.Lock()
		t.tick(time.Now())
		t.mu.Unlock()
	}
}

func (t *Transport) tick(now time.Time) {
	if now.Sub(t.lastAnnounce) >= announcePeriod {
		t.lastAnnounce = now
		t.announce()
		t.emitHellos(true)
	} else if t.discoveryDirty {
		t.emitHellos(false)
	}
	if now.Sub(t.lastHeartbeat) >= heartbeatPeriod {
		t.lastHeartbeat = now
		for _, w := range t.writersByGUID {
			t.heartbeatAll(w)
		}
	}
	for p, rp := range t.remotes {
		if now.Sub(rp.lastSeen) > rp.lease {
			t.debugf("participant ", rp.guid.String(), " lease expired")
			t.removeRemote(p)
		}
	}
	for g, o := range t.owners {
		if now.Sub(o.lastSeen) > ownerLease {
			t.removeOwner(g)
		}
	}
	for k, fb := range t.frags {
		if now.After(fb.deadline) {
			delete(t.frags, k)
		}
	}
	for seq, in := range t.inbound {
		if now.Sub(in.at) > pendingTimeout {
			delete(t.inbound, seq)
		}
	}
	for _, w := range t.writersByGUID {
		for sn, p := range w.pending {
			if now.Sub(p.at) > pendingTimeout {
				delete(w.pending, sn)
			}
		}
	}
}

// sendTo transmits one datagram.
func (t *Transport) sendTo(addr *net.UDPAddr, b []byte) {
	if addr == nil {
		return
	}
	c := t.uni
	if t.multi != nil && addr.IP.IsMulticast() {
		c = t.multi
	}
	if _, err := c.WriteToUDP(b, addr); err != nil {
		t.debugf("send to ", addr.String(), ": ", err.Error())
	}
}

func (t *Transport) newEntity(kind byte) entityID {
	t.nextEntity++
	n := t.nextEntity
	return entityID{byte(n >> 16), byte(n >> 8), byte(n), kind}
}
