// Package goros is a ROS 2 client library written in Go that builds with
// both the standard Go toolchain and TinyGo.
//
// It implements the ROS 2 programming model: contexts, nodes, topics with
// typed publishers and subscriptions, services with clients, timers,
// executors, parameters, QoS profiles, loggers and graph introspection,
// using CDR-serialized messages generated from standard .msg/.srv files.
//
// Participants discover each other and exchange samples over pluggable
// transports. On hosts the default is DDS-RTPS (package transport/rtps), so
// goros nodes join the graph of rclcpp and rclpy nodes directly; on
// microcontrollers a framed serial link or unicast UDP carries goros' own
// compact frames, which a host bridges into the RTPS domain.
//
// A minimal talker:
//
//	ctx, _ := goros.Init()
//	defer ctx.Shutdown()
//	node, _ := ctx.NewNode("talker")
//	pub, _ := goros.NewPublisher[*std_msgs.String](node, "chatter", goros.QoSDefault)
//	node.NewTimer(500*time.Millisecond, func() {
//	    pub.Publish(&std_msgs.String{Data: "Hello"})
//	})
//	goros.Spin(node)
package goros

import (
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"bitbucket.org/goros/cdr"
	"bitbucket.org/goros/transport"
)

var (
	ErrShutdown        = errors.New("goros: context is shut down")
	ErrTypeMismatch    = errors.New("goros: endpoint already exists with a different type")
	ErrServiceExists   = errors.New("goros: service already exists")
	ErrNoService       = errors.New("goros: service not available")
	ErrTimeout         = errors.New("goros: timed out")
	ErrQueueFull       = errors.New("goros: callback queue full")
	ErrAlreadySpinning = errors.New("goros: node already added to an executor")
)

// Option configures Init.
type Option func(*options)

type options struct {
	domain      int
	domainSet   bool
	transports  []transport.Transport
	noDefault   bool
	args        []string
	helloPeriod time.Duration
	logWriter   io.Writer
	logLevel    LogLevel
	queueSize   int
}

// WithDomain sets the ROS domain ID (default: $ROS_DOMAIN_ID or 0).
func WithDomain(id int) Option { return func(o *options) { o.domain, o.domainSet = id, true } }

// WithTransport attaches a transport. May be repeated.
func WithTransport(t transport.Transport) Option {
	return func(o *options) { o.transports = append(o.transports, t) }
}

// WithoutDefaultTransport disables the platform default transport (UDP
// multicast on hosts). Intra-context communication still works.
func WithoutDefaultTransport() Option { return func(o *options) { o.noDefault = true } }

// WithArgs passes the command line so --ros-args remappings and parameter
// overrides are honoured. Usually os.Args[1:].
func WithArgs(args []string) Option { return func(o *options) { o.args = args } }

// WithHelloPeriod sets how often discovery announcements are sent.
func WithHelloPeriod(d time.Duration) Option { return func(o *options) { o.helloPeriod = d } }

// WithLogWriter redirects console logging (nil disables it).
func WithLogWriter(w io.Writer) Option { return func(o *options) { o.logWriter = w } }

// WithLogLevel sets the default logger threshold.
func WithLogLevel(l LogLevel) Option { return func(o *options) { o.logLevel = l } }

// WithQueueSize sets the default per-node callback queue length.
func WithQueueSize(n int) Option { return func(o *options) { o.queueSize = n } }

// Context is one participant in the ROS graph. It owns transports, the
// discovery state and all nodes created from it (rclcpp::Context +
// the rmw participant).
type Context struct {
	guid        transport.GUID
	domain      int
	transports  []transport.Transport
	helloPeriod time.Duration
	queueSize   int
	args        parsedArgs
	restArgs    []string

	logW     io.Writer
	logMu    sync.Mutex
	logLevel LogLevel
	logger   *Logger

	encMu sync.Mutex
	enc   *cdr.Encoder

	mu       sync.RWMutex
	nodes    []*Node
	subs     map[string][]*Subscription
	pubs     map[string][]*publisherBase
	services map[string]*serviceBase
	remotes  map[transport.GUID]*remoteParticipant
	graphGen chan struct{} // closed and replaced on every graph change

	pendMu  sync.Mutex
	pending map[uint64]*pendingCall
	seq     uint64

	helloKick chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	shutdown  atomic.Bool
}

type remoteParticipant struct {
	info     transport.ParticipantInfo
	lastSeen time.Time
}

type pendingCall struct {
	ch chan *transport.Frame
}

// Init creates a Context, like rclcpp::init.
func Init(opts ...Option) (*Context, error) {
	o := options{helloPeriod: time.Second, logWriter: os.Stdout, logLevel: LogInfo, queueSize: 256}
	for _, opt := range opts {
		opt(&o)
	}
	if !o.domainSet {
		if v := os.Getenv("ROS_DOMAIN_ID"); v != "" {
			if id, err := strconv.Atoi(v); err == nil {
				o.domain = id
			}
		}
	}
	pa, rest, err := parseArgs(o.args)
	if err != nil {
		return nil, err
	}
	if pa.hasLog {
		o.logLevel = pa.logLevel
	}
	c := &Context{
		guid:        newGUID(),
		domain:      o.domain,
		helloPeriod: o.helloPeriod,
		queueSize:   o.queueSize,
		args:        pa,
		restArgs:    rest,
		logW:        o.logWriter,
		logLevel:    o.logLevel,
		enc:         cdr.NewEncoder(),
		subs:        map[string][]*Subscription{},
		pubs:        map[string][]*publisherBase{},
		services:    map[string]*serviceBase{},
		remotes:     map[transport.GUID]*remoteParticipant{},
		graphGen:    make(chan struct{}),
		pending:     map[uint64]*pendingCall{},
		helloKick:   make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	c.logger = &Logger{name: "goros", ctx: c, level: o.logLevel}
	c.transports = append(c.transports, o.transports...)
	if !o.noDefault {
		if t, err := defaultTransport(o.domain); err != nil {
			c.logger.Warn("default transport unavailable: " + err.Error())
		} else if t != nil {
			c.transports = append(c.transports, t)
		}
	}
	for _, t := range c.transports {
		c.wg.Add(1)
		go c.recvLoop(t)
	}
	c.wg.Add(1)
	go c.helloLoop()
	return c, nil
}

// GUID returns this participant's identifier.
func (c *Context) GUID() transport.GUID { return c.guid }

// Domain returns the ROS domain ID.
func (c *Context) Domain() int { return c.domain }

// Args returns the non-ROS command line arguments left after --ros-args
// processing.
func (c *Context) Args() []string { return c.restArgs }

// Logger returns the context-level logger.
func (c *Context) Logger() *Logger { return c.logger }

// OK reports whether the context is still running (rclcpp::ok).
func (c *Context) OK() bool { return !c.shutdown.Load() }

// Done is closed on Shutdown.
func (c *Context) Done() <-chan struct{} { return c.done }

// Shutdown announces departure, closes transports and stops all nodes.
func (c *Context) Shutdown() {
	c.closeOnce.Do(func() {
		c.shutdown.Store(true)
		c.sendFrame(&transport.Frame{Kind: transport.KindBye})
		close(c.done)
		c.mu.Lock()
		nodes := append([]*Node(nil), c.nodes...)
		c.mu.Unlock()
		for _, n := range nodes {
			n.Destroy()
		}
		for _, t := range c.transports {
			t.Close()
		}
		// Transports must unblock Recv on Close; bound the wait anyway so a
		// misbehaving one cannot hang the process.
		waited := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(waited)
		}()
		select {
		case <-waited:
		case <-time.After(2 * time.Second):
			c.logger.Warn("shutdown: transport goroutines did not exit")
		}
	})
}

func (c *Context) logWriter() io.Writer { return c.logW }

// --- wire I/O ---------------------------------------------------------------

func (c *Context) sendFrame(f *transport.Frame) {
	if len(c.transports) == 0 {
		return
	}
	c.encMu.Lock()
	c.sendFrameLocked(f)
	c.encMu.Unlock()
}

func (c *Context) sendFrameLocked(f *transport.Frame) {
	f.From = c.guid
	buf := f.Encode(c.enc)
	for _, t := range c.transports {
		if err := t.Send(buf); err != nil && err != transport.ErrClosed {
			c.logger.Debug("send on " + t.Name() + ": " + err.Error())
		}
	}
}

// sendHello announces the current graph. The snapshot is taken under the
// send lock so that announcements reach transports in snapshot order; a
// stale snapshot overtaking a newer one would make a transport tear an
// endpoint down and recreate it.
func (c *Context) sendHello() {
	if len(c.transports) == 0 {
		return
	}
	c.encMu.Lock()
	c.sendFrameLocked(&transport.Frame{Kind: transport.KindHello, Participant: c.participantInfo()})
	c.encMu.Unlock()
}

func (c *Context) recvLoop(t transport.Transport) {
	defer c.wg.Done()
	for {
		buf, err := t.Recv()
		if err != nil {
			if err == transport.ErrClosed || c.shutdown.Load() {
				return
			}
			c.logger.Debug("recv on " + t.Name() + ": " + err.Error())
			continue
		}
		f, err := transport.Decode(buf)
		if err != nil {
			continue // not ours / corrupted
		}
		if f.From == c.guid {
			continue // multicast loopback
		}
		c.handleFrame(f)
	}
}

func (c *Context) handleFrame(f *transport.Frame) {
	switch f.Kind {
	case transport.KindHello:
		c.handleHello(f)
	case transport.KindBye:
		c.mu.Lock()
		if _, ok := c.remotes[f.From]; ok {
			delete(c.remotes, f.From)
			c.graphChangedLocked()
		}
		c.mu.Unlock()
	case transport.KindData, transport.KindDataReplay:
		c.mu.RLock()
		subs := c.subs[f.Topic]
		c.mu.RUnlock()
		for _, s := range subs {
			if s.typ == f.Type {
				s.deliver(f.Payload)
			}
		}
	case transport.KindRequest:
		c.mu.RLock()
		srv := c.services[f.Topic]
		c.mu.RUnlock()
		if srv != nil && srv.typ == f.Type {
			srv.handle(f.From, f.Seq, f.Payload, false)
		}
	case transport.KindResponse:
		if f.Target == c.guid {
			c.deliverResponse(f)
		}
	}
}

func (c *Context) handleHello(f *transport.Frame) {
	if f.Participant == nil || int(f.Participant.Domain) != c.domain {
		return
	}
	c.mu.Lock()
	prev, known := c.remotes[f.From]
	c.remotes[f.From] = &remoteParticipant{info: *f.Participant, lastSeen: time.Now()}
	changed := !known || !sameGraph(&prev.info, f.Participant)
	if changed {
		c.graphChangedLocked()
	}
	// Transient-local replay to subscriptions we have not seen before.
	var replay []*publisherBase
	if changed {
		for _, n := range f.Participant.Nodes {
			for _, s := range n.Subscriptions {
				if known && hasSubscription(&prev.info, s.Name, s.Type) {
					continue
				}
				for _, p := range c.pubs[s.Name] {
					if p.typ == s.Type && p.qos.Durability == TransientLocal {
						replay = append(replay, p)
					}
				}
			}
		}
	}
	c.mu.Unlock()
	for _, p := range replay {
		p.replay()
	}
}

func hasSubscription(p *transport.ParticipantInfo, name, typ string) bool {
	for _, n := range p.Nodes {
		for _, s := range n.Subscriptions {
			if s.Name == name && s.Type == typ {
				return true
			}
		}
	}
	return false
}

func sameEndpoints(a, b []transport.EndpointInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameGraph(a, b *transport.ParticipantInfo) bool {
	if len(a.Nodes) != len(b.Nodes) {
		return false
	}
	for i := range a.Nodes {
		x, y := &a.Nodes[i], &b.Nodes[i]
		if x.Name != y.Name || x.Namespace != y.Namespace ||
			!sameEndpoints(x.Publishers, y.Publishers) || !sameEndpoints(x.Subscriptions, y.Subscriptions) ||
			!sameEndpoints(x.Services, y.Services) || !sameEndpoints(x.Clients, y.Clients) {
			return false
		}
	}
	return true
}

// graphChangedLocked wakes anything waiting on graph changes. c.mu held.
func (c *Context) graphChangedLocked() {
	close(c.graphGen)
	c.graphGen = make(chan struct{})
}

// graphChanged returns a channel closed on the next graph change.
func (c *Context) graphChanged() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.graphGen
}

// kickHello sends a discovery announcement right away, so a transport
// knows about an endpoint before the first sample on it, and restarts the
// periodic schedule.
func (c *Context) kickHello() {
	if c.shutdown.Load() {
		return
	}
	c.sendHello()
	select {
	case c.helloKick <- struct{}{}:
	default:
	}
}

func (c *Context) participantInfo() *transport.ParticipantInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p := &transport.ParticipantInfo{Domain: uint32(c.domain)}
	for _, n := range c.nodes {
		p.Nodes = append(p.Nodes, n.info())
	}
	return p
}

func (c *Context) helloLoop() {
	defer c.wg.Done()
	if len(c.transports) == 0 {
		return
	}
	lease := 3 * c.helloPeriod
	if lease < 3*time.Second {
		lease = 3 * time.Second
	}
	ticker := time.NewTicker(c.helloPeriod)
	defer ticker.Stop()
	for {
		c.sendHello()
		select {
		case <-c.done:
			return
		case <-c.helloKick:
		case <-ticker.C:
			now := time.Now()
			c.mu.Lock()
			for g, r := range c.remotes {
				if now.Sub(r.lastSeen) > lease {
					delete(c.remotes, g)
					c.graphChangedLocked()
				}
			}
			c.mu.Unlock()
		}
	}
}

// --- publish / request plumbing --------------------------------------------

// publish delivers a serialized sample to local subscriptions and, when any
// remote participant subscribes to the topic, onto the transports.
func (c *Context) publish(p *publisherBase, payload []byte) {
	c.mu.RLock()
	subs := c.subs[p.topic]
	remote := c.remoteSubscribesLocked(p.topic, p.typ)
	c.mu.RUnlock()
	for _, s := range subs {
		if s.typ == p.typ && p.qos.CompatibleWith(s.qos) {
			s.deliver(payload)
		}
	}
	// Transient-local samples always go out so that transports with their
	// own durability (RTPS) can keep them for late joiners.
	if remote || p.qos.Durability == TransientLocal {
		c.sendFrame(&transport.Frame{Kind: transport.KindData, Topic: p.topic, Type: p.typ, Payload: payload})
	}
}

func (c *Context) remoteSubscribesLocked(topic, typ string) bool {
	for _, r := range c.remotes {
		for _, n := range r.info.Nodes {
			for _, s := range n.Subscriptions {
				if s.Name == topic && s.Type == typ {
					return true
				}
			}
		}
	}
	return false
}

func (c *Context) remoteServesLocked(name, typ string) bool {
	for _, r := range c.remotes {
		for _, n := range r.info.Nodes {
			for _, s := range n.Services {
				if s.Name == name && (typ == "" || s.Type == typ) {
					return true
				}
			}
		}
	}
	return false
}

// request routes a service request to a local service or the transports.
func (c *Context) request(name, typ string, payload []byte) (uint64, *pendingCall, error) {
	seq := atomic.AddUint64(&c.seq, 1)
	pc := &pendingCall{ch: make(chan *transport.Frame, 1)}
	c.pendMu.Lock()
	c.pending[seq] = pc
	c.pendMu.Unlock()

	c.mu.RLock()
	srv := c.services[name]
	remote := c.remoteServesLocked(name, typ)
	c.mu.RUnlock()
	switch {
	case srv != nil && srv.typ == typ:
		srv.handle(c.guid, seq, payload, true)
	case remote:
		c.sendFrame(&transport.Frame{Kind: transport.KindRequest, Topic: name, Type: typ, Seq: seq, Payload: payload})
	default:
		c.cancelRequest(seq)
		return 0, nil, ErrNoService
	}
	return seq, pc, nil
}

func (c *Context) cancelRequest(seq uint64) {
	c.pendMu.Lock()
	delete(c.pending, seq)
	c.pendMu.Unlock()
}

func (c *Context) deliverResponse(f *transport.Frame) {
	c.pendMu.Lock()
	pc := c.pending[f.Seq]
	delete(c.pending, f.Seq)
	c.pendMu.Unlock()
	if pc != nil {
		select {
		case pc.ch <- f:
		default:
		}
	}
}

// respond sends a service response back to the requesting participant.
func (c *Context) respond(target transport.GUID, name string, seq uint64, payload []byte, err error) {
	f := &transport.Frame{Kind: transport.KindResponse, Topic: name, Target: target, Seq: seq, Payload: payload, OK: err == nil}
	if err != nil {
		f.Error = err.Error()
	}
	if target == c.guid {
		f.From = c.guid
		c.deliverResponse(f)
		return
	}
	c.sendFrame(f)
}
