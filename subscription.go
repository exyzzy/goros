package goros

import (
	"sync"

	"bitbucket.org/goros/msg"
	"bitbucket.org/goros/transport"
)

// frameData is a tiny helper to build data frames.
type frameData struct {
	topic, typ string
	payload    []byte
}

func (f frameData) frame() *transport.Frame {
	return &transport.Frame{Kind: transport.KindData, Topic: f.topic, Type: f.typ, Payload: f.payload}
}

// Subscription receives samples on a topic and runs a callback for each one
// on the owning node's executor.
type Subscription struct {
	node  *Node
	topic string
	typ   string
	qos   QoSProfile
	cb    func(payload []byte)

	mu      sync.Mutex
	ring    [][]byte
	pending bool
	gone    bool
	dropped uint64
}

// NewSubscription subscribes to topic. The callback's parameter type picks
// the message type:
//
//	goros.NewSubscription(node, "chatter", goros.QoSDefault, func(m *std_msgs.String) { ... })
func NewSubscription[T any, PT msg.Ptr[T]](node *Node, topic string, qos QoSProfile, cb func(PT)) (*Subscription, error) {
	var zero PT = new(T)
	typ := zero.TypeName()
	log := node.logger
	return NewRawSubscription(node, topic, typ, qos, func(payload []byte) {
		m := PT(new(T))
		if err := msg.Decode(payload, m); err != nil {
			log.Warn("failed to decode " + typ + " on " + topic + ": " + err.Error())
			return
		}
		cb(m)
	})
}

// NewRawSubscription subscribes to topic with a callback that receives the
// CDR payload without decoding it (useful for bridges and recorders).
func NewRawSubscription(node *Node, topic, typ string, qos QoSProfile, cb func(payload []byte)) (*Subscription, error) {
	full, err := node.ResolveTopicName(topic)
	if err != nil {
		return nil, err
	}
	s := &Subscription{node: node, topic: full, typ: typ, qos: qos.Resolve(), cb: cb}
	c := node.ctx
	c.mu.Lock()
	for _, other := range c.subs[full] {
		if other.typ != typ {
			c.mu.Unlock()
			return nil, ErrTypeMismatch
		}
	}
	c.subs[full] = append(c.subs[full], s)
	var replay []*publisherBase
	if s.qos.Durability == TransientLocal {
		for _, p := range c.pubs[full] {
			if p.typ == typ && p.qos.Durability == TransientLocal {
				replay = append(replay, p)
			}
		}
	}
	c.graphChangedLocked()
	c.mu.Unlock()
	node.mu.Lock()
	node.subs = append(node.subs, s)
	node.mu.Unlock()
	for _, p := range replay {
		p.replayTo(s)
	}
	c.kickHello()
	return s, nil
}

// Topic returns the fully-qualified topic name.
func (s *Subscription) Topic() string { return s.topic }

// TypeName returns the ROS message type name.
func (s *Subscription) TypeName() string { return s.typ }

// QoS returns the resolved profile.
func (s *Subscription) QoS() QoSProfile { return s.qos }

// PublisherCount counts matched local and remote publishers.
func (s *Subscription) PublisherCount() int { return s.node.ctx.countPublishers(s.topic) }

// Dropped returns how many samples were discarded because the history was
// full (KeepLast) before the executor processed them.
func (s *Subscription) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// deliver queues a sample and schedules processing on the node.
func (s *Subscription) deliver(payload []byte) {
	s.mu.Lock()
	if s.gone {
		s.mu.Unlock()
		return
	}
	if s.qos.History == KeepLast && len(s.ring) >= s.qos.Depth {
		copy(s.ring, s.ring[1:])
		s.ring = s.ring[:len(s.ring)-1]
		s.dropped++
	}
	s.ring = append(s.ring, payload)
	schedule := !s.pending
	s.pending = true
	s.mu.Unlock()
	if schedule {
		if !s.node.enqueue(s.process) {
			s.mu.Lock()
			s.pending = false
			s.mu.Unlock()
		}
	}
}

// process drains queued samples through the callback (runs on executor).
func (s *Subscription) process() {
	for {
		s.mu.Lock()
		if len(s.ring) == 0 || s.gone {
			s.pending = false
			s.mu.Unlock()
			return
		}
		payload := s.ring[0]
		s.ring[0] = nil
		s.ring = s.ring[1:]
		s.mu.Unlock()
		s.cb(payload)
	}
}

// Destroy unsubscribes.
func (s *Subscription) Destroy() {
	s.node.mu.Lock()
	for i, x := range s.node.subs {
		if x == s {
			s.node.subs = append(s.node.subs[:i], s.node.subs[i+1:]...)
			break
		}
	}
	s.node.mu.Unlock()
	s.destroy()
	s.node.ctx.kickHello()
}

func (s *Subscription) destroy() {
	s.mu.Lock()
	if s.gone {
		s.mu.Unlock()
		return
	}
	s.gone = true
	s.ring = nil
	s.mu.Unlock()
	c := s.node.ctx
	c.mu.Lock()
	list := c.subs[s.topic]
	for i, x := range list {
		if x == s {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(c.subs, s.topic)
	} else {
		c.subs[s.topic] = list
	}
	c.graphChangedLocked()
	c.mu.Unlock()
}
