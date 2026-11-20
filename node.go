package goros

import (
	"strings"
	"sync"
	"sync/atomic"

	"bitbucket.org/goros/msgs/rcl_interfaces"
	"bitbucket.org/goros/names"
	"bitbucket.org/goros/transport"
)

// NodeOption configures NewNode.
type NodeOption func(*nodeOptions)

type nodeOptions struct {
	namespace     string
	queueSize     int
	overrides     map[string]ParameterValue
	noRosout      bool
	noParamSrv    bool
	noParamEvents bool
	useGlobalArgs bool
	clock         Clock
}

// WithNamespace sets the node namespace (default "/").
func WithNamespace(ns string) NodeOption { return func(o *nodeOptions) { o.namespace = ns } }

// WithNodeQueueSize sets this node's callback queue length.
func WithNodeQueueSize(n int) NodeOption { return func(o *nodeOptions) { o.queueSize = n } }

// WithParameterOverrides supplies initial parameter values that take
// precedence over DeclareParameter defaults.
func WithParameterOverrides(m map[string]ParameterValue) NodeOption {
	return func(o *nodeOptions) { o.overrides = m }
}

// WithoutRosout disables forwarding of log records to /rosout.
func WithoutRosout() NodeOption { return func(o *nodeOptions) { o.noRosout = true } }

// WithoutParameterServices disables the ~/get_parameters etc. services.
func WithoutParameterServices() NodeOption { return func(o *nodeOptions) { o.noParamSrv = true } }

// WithoutParameterEvents disables publishing on /parameter_events.
func WithoutParameterEvents() NodeOption { return func(o *nodeOptions) { o.noParamEvents = true } }

// WithClock replaces the node clock.
func WithClock(c Clock) NodeOption { return func(o *nodeOptions) { o.clock = c } }

// Node is a ROS 2 node: a named collection of publishers, subscriptions,
// services, clients, timers and parameters that share a callback queue.
type Node struct {
	ctx       *Context
	name      string
	namespace string
	queue     chan func()
	clock     Clock
	logger    *Logger
	params    *paramStore

	mu        sync.Mutex
	pubs      []*publisherBase
	subs      []*Subscription
	services  []*serviceBase
	clients   []*clientBase
	timers    []*Timer
	destroyed bool
	executor  *Executor

	paramEvents *Publisher[*rcl_interfaces.ParameterEvent]
	dropped     atomic.Uint64
}

// NewNode creates a node in this context.
func (c *Context) NewNode(name string, opts ...NodeOption) (*Node, error) {
	if !c.OK() {
		return nil, ErrShutdown
	}
	o := nodeOptions{namespace: "/", queueSize: c.queueSize, clock: SystemClock{}}
	for _, opt := range opts {
		opt(&o)
	}
	// Apply __node / __ns remappings from the command line.
	for _, r := range c.args.remaps {
		if r.node != "" && r.node != name {
			continue
		}
		switch r.from {
		case "__node":
			name = r.to
		case "__ns":
			o.namespace = r.to
		}
	}
	if err := names.ValidateNodeName(name); err != nil {
		return nil, err
	}
	if o.namespace == "" {
		o.namespace = "/"
	}
	if !strings.HasPrefix(o.namespace, "/") {
		o.namespace = "/" + o.namespace
	}
	if err := names.ValidateNamespace(o.namespace); err != nil {
		return nil, err
	}
	overrides := map[string]ParameterValue{}
	for _, p := range c.args.params {
		pname := p.Name
		if i := strings.IndexByte(pname, ':'); i >= 0 {
			if pname[:i] != name {
				continue
			}
			pname = pname[i+1:]
		}
		overrides[pname] = p.Value
	}
	for k, v := range o.overrides {
		overrides[k] = v
	}
	n := &Node{
		ctx:       c,
		name:      name,
		namespace: o.namespace,
		queue:     make(chan func(), o.queueSize),
		clock:     o.clock,
		params:    newParamStore(overrides),
	}
	n.logger = &Logger{name: strings.TrimPrefix(strings.ReplaceAll(names.Join(o.namespace, name), "/", "."), "."), ctx: c, node: n, level: c.logLevel}
	c.mu.Lock()
	c.nodes = append(c.nodes, n)
	c.mu.Unlock()

	if !o.noRosout {
		n.logger.rosout, _ = NewPublisher[*rcl_interfaces.Log](n, "/rosout", QoSRosout)
	}
	if !o.noParamEvents {
		n.paramEvents, _ = NewPublisher[*rcl_interfaces.ParameterEvent](n, "/parameter_events", QoSParameterEvents)
	}
	if !o.noParamSrv {
		n.createParameterServices()
	}
	c.kickHello()
	return n, nil
}

// Name returns the node name.
func (n *Node) Name() string { return n.name }

// Namespace returns the node namespace.
func (n *Node) Namespace() string { return n.namespace }

// FullyQualifiedName returns "<namespace>/<name>".
func (n *Node) FullyQualifiedName() string { return names.FullyQualifiedNodeName(n.name, n.namespace) }

// Context returns the owning context.
func (n *Node) Context() *Context { return n.ctx }

// Logger returns the node logger.
func (n *Node) Logger() *Logger { return n.logger }

// Clock returns the node clock.
func (n *Node) Clock() Clock { return n.clock }

// Now returns the current time from the node clock.
func (n *Node) Now() Time { return n.clock.Now() }

// Dropped returns how many callbacks were discarded because the node's
// queue was full.
func (n *Node) Dropped() uint64 { return n.dropped.Load() }

// ResolveTopicName expands a topic or service name relative to this node and
// applies command-line remappings.
func (n *Node) ResolveTopicName(name string) (string, error) {
	for _, r := range n.ctx.args.remaps {
		if r.node != "" && r.node != n.name {
			continue
		}
		if r.from == "__node" || r.from == "__ns" {
			continue
		}
		if r.from == name {
			name = r.to
			break
		}
		if full, err := names.Expand(r.from, n.name, n.namespace); err == nil {
			if cur, err := names.Expand(name, n.name, n.namespace); err == nil && cur == full {
				name = r.to
				break
			}
		}
	}
	return names.Expand(name, n.name, n.namespace)
}

// enqueue schedules fn on the node's callback queue without blocking. It
// returns false if the queue is full.
func (n *Node) enqueue(fn func()) bool {
	select {
	case n.queue <- fn:
		return true
	default:
		n.dropped.Add(1)
		return false
	}
}

func (n *Node) info() transport.NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	ni := transport.NodeInfo{Name: n.name, Namespace: n.namespace}
	for _, p := range n.pubs {
		ni.Publishers = append(ni.Publishers, transport.EndpointInfo{Name: p.topic, Type: p.typ, QoS: p.qos.info()})
	}
	for _, s := range n.subs {
		ni.Subscriptions = append(ni.Subscriptions, transport.EndpointInfo{Name: s.topic, Type: s.typ, QoS: s.qos.info()})
	}
	for _, s := range n.services {
		ni.Services = append(ni.Services, transport.EndpointInfo{Name: s.name, Type: s.typ})
	}
	for _, c := range n.clients {
		ni.Clients = append(ni.Clients, transport.EndpointInfo{Name: c.name, Type: c.typ})
	}
	return ni
}

// Destroy tears down every entity owned by the node and removes it from the
// graph. The node must not be used afterwards.
func (n *Node) Destroy() {
	n.mu.Lock()
	if n.destroyed {
		n.mu.Unlock()
		return
	}
	n.destroyed = true
	timers := n.timers
	pubs := n.pubs
	subs := n.subs
	services := n.services
	clients := n.clients
	n.timers, n.pubs, n.subs, n.services, n.clients = nil, nil, nil, nil, nil
	n.mu.Unlock()
	for _, t := range timers {
		t.Cancel()
	}
	for _, p := range pubs {
		p.destroy()
	}
	for _, s := range subs {
		s.destroy()
	}
	for _, s := range services {
		s.destroy()
	}
	for _, c := range clients {
		c.destroy()
	}
	c := n.ctx
	c.mu.Lock()
	for i, x := range c.nodes {
		if x == n {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
	if ex := n.currentExecutor(); ex != nil {
		ex.RemoveNode(n)
	}
	c.kickHello()
}

func (n *Node) currentExecutor() *Executor {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.executor
}
