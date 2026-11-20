package goros

import (
	"context"
	"errors"
	"sync"
	"time"

	"bitbucket.org/goros/msg"
)

type clientBase struct {
	node *Node
	name string
	typ  string
	mu   sync.Mutex
	gone bool
}

// Client calls a service.
type Client[Req, Resp any, PReq msg.Ptr[Req], PResp msg.Ptr[Resp]] struct {
	*clientBase
}

// NewClient creates a client for a service:
//
//	cli, _ := goros.NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](node, "add_two_ints")
func NewClient[Req, Resp any, PReq msg.Ptr[Req], PResp msg.Ptr[Resp]](node *Node, name string) (*Client[Req, Resp, PReq, PResp], error) {
	var zero PReq = new(Req)
	base, err := newClientBase(node, name, msg.ServiceTypeName(zero.TypeName()))
	if err != nil {
		return nil, err
	}
	return &Client[Req, Resp, PReq, PResp]{base}, nil
}

// NewRawClient creates a client that exchanges CDR payloads.
func NewRawClient(node *Node, name, typ string) (*RawClient, error) {
	base, err := newClientBase(node, name, typ)
	if err != nil {
		return nil, err
	}
	return &RawClient{base}, nil
}

func newClientBase(node *Node, name, typ string) (*clientBase, error) {
	full, err := node.ResolveTopicName(name)
	if err != nil {
		return nil, err
	}
	c := &clientBase{node: node, name: full, typ: typ}
	node.mu.Lock()
	node.clients = append(node.clients, c)
	node.mu.Unlock()
	node.ctx.kickHello()
	return c, nil
}

// RawClient calls a service with pre-serialized payloads.
type RawClient struct {
	*clientBase
}

// Name returns the fully-qualified service name.
func (c *clientBase) Name() string { return c.name }

// TypeName returns the ROS service type name.
func (c *clientBase) TypeName() string { return c.typ }

// ServiceIsReady reports whether a matching service is currently known.
func (c *clientBase) ServiceIsReady() bool {
	ctx := c.node.ctx
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	if s := ctx.services[c.name]; s != nil && s.typ == c.typ {
		return true
	}
	return ctx.remoteServesLocked(c.name, c.typ)
}

// WaitForService blocks until the service is discovered, the timeout
// elapses (timeout <= 0 waits forever) or the context shuts down.
func (c *clientBase) WaitForService(timeout time.Duration) bool {
	var deadline <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		deadline = t.C
	}
	for {
		changed := c.node.ctx.graphChanged()
		if c.ServiceIsReady() {
			return true
		}
		select {
		case <-changed:
		case <-deadline:
			return false
		case <-c.node.ctx.Done():
			return false
		}
	}
}

// CallRaw sends payload and waits for the response payload.
func (c *clientBase) CallRaw(ctx context.Context, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rc := c.node.ctx
	if !rc.OK() {
		return nil, ErrShutdown
	}
	seq, pc, err := rc.request(c.name, c.typ, payload)
	if err != nil {
		return nil, err
	}
	select {
	case f := <-pc.ch:
		if !f.OK {
			return nil, errors.New("goros: service error: " + f.Error)
		}
		return f.Payload, nil
	case <-ctx.Done():
		rc.cancelRequest(seq)
		return nil, ctx.Err()
	case <-rc.Done():
		rc.cancelRequest(seq)
		return nil, ErrShutdown
	}
}

// Call sends req and waits for the response. Do not call it from inside a
// callback of the same single-threaded executor that runs the service: that
// deadlocks, exactly as in rclcpp. Use CallAsync there instead.
func (c *Client[Req, Resp, PReq, PResp]) Call(ctx context.Context, req PReq) (PResp, error) {
	payload, err := c.CallRaw(ctx, msg.Encode(req))
	if err != nil {
		return nil, err
	}
	resp := PResp(new(Resp))
	if err := msg.Decode(payload, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CallAsync sends req and later runs done on the node's executor.
func (c *Client[Req, Resp, PReq, PResp]) CallAsync(req PReq, done func(PResp, error)) {
	go func() {
		resp, err := c.Call(context.Background(), req)
		if !c.node.enqueue(func() { done(resp, err) }) {
			c.node.logger.Warn("dropped service response callback: queue full")
		}
	}()
}

// Destroy removes the client.
func (c *clientBase) Destroy() {
	c.node.mu.Lock()
	for i, x := range c.node.clients {
		if x == c {
			c.node.clients = append(c.node.clients[:i], c.node.clients[i+1:]...)
			break
		}
	}
	c.node.mu.Unlock()
	c.destroy()
	c.node.ctx.kickHello()
}

func (c *clientBase) destroy() {
	c.mu.Lock()
	c.gone = true
	c.mu.Unlock()
}
