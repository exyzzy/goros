package goros

import (
	"sync"

	"bitbucket.org/goros/msg"
	"bitbucket.org/goros/transport"
)

type publisherBase struct {
	node  *Node
	topic string
	typ   string
	qos   QoSProfile

	mu    sync.Mutex
	cache [][]byte // transient-local history
	gone  bool
}

// Publisher sends messages of type T (a pointer type implementing
// msg.Message, e.g. *std_msgs.String) on a topic.
type Publisher[T msg.Message] struct {
	*publisherBase
}

// NewPublisher creates a publisher on topic with the given QoS.
func NewPublisher[T msg.Message](node *Node, topic string, qos QoSProfile) (*Publisher[T], error) {
	var zero T
	full, err := node.ResolveTopicName(topic)
	if err != nil {
		return nil, err
	}
	p, err := node.ctx.addPublisher(node, full, zero.TypeName(), qos)
	if err != nil {
		return nil, err
	}
	return &Publisher[T]{p}, nil
}

func (c *Context) addPublisher(node *Node, full, typ string, qos QoSProfile) (*publisherBase, error) {
	p := &publisherBase{node: node, topic: full, typ: typ, qos: qos.Resolve()}
	c.mu.Lock()
	for _, other := range c.pubs[full] {
		if other.typ != p.typ {
			c.mu.Unlock()
			return nil, ErrTypeMismatch
		}
	}
	c.pubs[full] = append(c.pubs[full], p)
	c.graphChangedLocked()
	c.mu.Unlock()
	node.mu.Lock()
	node.pubs = append(node.pubs, p)
	node.mu.Unlock()
	c.kickHello()
	return p, nil
}

// Topic returns the fully-qualified topic name.
func (p *publisherBase) Topic() string { return p.topic }

// TypeName returns the ROS message type name.
func (p *publisherBase) TypeName() string { return p.typ }

// QoS returns the resolved profile.
func (p *publisherBase) QoS() QoSProfile { return p.qos }

// SubscriptionCount counts matched local and remote subscriptions.
func (p *publisherBase) SubscriptionCount() int {
	return p.node.ctx.countSubscribers(p.topic)
}

// Publish serializes and sends m.
func (p *Publisher[T]) Publish(m T) error {
	return p.PublishRaw(msg.Encode(m))
}

// PublishRaw sends an already CDR-serialized sample (with encapsulation
// header). The slice must not be modified afterwards.
func (p *publisherBase) PublishRaw(payload []byte) error {
	if !p.node.ctx.OK() {
		return ErrShutdown
	}
	if p.qos.Durability == TransientLocal {
		p.mu.Lock()
		if p.qos.History == KeepLast && len(p.cache) >= p.qos.Depth {
			copy(p.cache, p.cache[1:])
			p.cache = p.cache[:len(p.cache)-1]
		}
		p.cache = append(p.cache, payload)
		p.mu.Unlock()
	}
	p.node.ctx.publish(p, payload)
	return nil
}

// replay resends the transient-local cache (to late joiners).
func (p *publisherBase) replay() {
	p.mu.Lock()
	cache := append([][]byte(nil), p.cache...)
	p.mu.Unlock()
	c := p.node.ctx
	for _, payload := range cache {
		c.sendFrame(&transport.Frame{Kind: transport.KindDataReplay, Topic: p.topic, Type: p.typ, Payload: payload})
	}
}

// replayTo delivers the cache to one local late-joining subscription.
func (p *publisherBase) replayTo(s *Subscription) {
	p.mu.Lock()
	cache := append([][]byte(nil), p.cache...)
	p.mu.Unlock()
	for _, payload := range cache {
		s.deliver(payload)
	}
}

// Destroy removes the publisher from the graph.
func (p *publisherBase) Destroy() {
	p.node.mu.Lock()
	for i, x := range p.node.pubs {
		if x == p {
			p.node.pubs = append(p.node.pubs[:i], p.node.pubs[i+1:]...)
			break
		}
	}
	p.node.mu.Unlock()
	p.destroy()
	p.node.ctx.kickHello()
}

func (p *publisherBase) destroy() {
	p.mu.Lock()
	if p.gone {
		p.mu.Unlock()
		return
	}
	p.gone = true
	p.cache = nil
	p.mu.Unlock()
	c := p.node.ctx
	c.mu.Lock()
	list := c.pubs[p.topic]
	for i, x := range list {
		if x == p {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(c.pubs, p.topic)
	} else {
		c.pubs[p.topic] = list
	}
	c.graphChangedLocked()
	c.mu.Unlock()
}

// RawPublisher publishes pre-serialized samples of a given type name.
type RawPublisher struct {
	*publisherBase
}

// NewRawPublisher creates a publisher for CDR payloads of type typ.
func NewRawPublisher(node *Node, topic, typ string, qos QoSProfile) (*RawPublisher, error) {
	full, err := node.ResolveTopicName(topic)
	if err != nil {
		return nil, err
	}
	p, err := node.ctx.addPublisher(node, full, typ, qos)
	if err != nil {
		return nil, err
	}
	return &RawPublisher{p}, nil
}
