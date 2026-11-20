package goros

import (
	"errors"
	"sync"

	"bitbucket.org/goros/msg"
	"bitbucket.org/goros/transport"
)

type serviceBase struct {
	node    *Node
	name    string
	typ     string
	handler func(payload []byte) ([]byte, error)
	mu      sync.Mutex
	gone    bool
}

// Service answers requests on a service name. Handler errors are reported
// to the caller as a failed call.
type Service[Req, Resp any, PReq msg.Ptr[Req], PResp msg.Ptr[Resp]] struct {
	*serviceBase
}

// NewService creates a service. Request and response types are inferred
// from the handler:
//
//	goros.NewService(node, "add_two_ints",
//	    func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
//	        resp.Sum = req.A + req.B
//	        return nil
//	    })
func NewService[Req, Resp any, PReq msg.Ptr[Req], PResp msg.Ptr[Resp]](node *Node, name string, handler func(PReq, PResp) error) (*Service[Req, Resp, PReq, PResp], error) {
	var zero PReq = new(Req)
	typ := msg.ServiceTypeName(zero.TypeName())
	base, err := NewRawService(node, name, typ, func(payload []byte) ([]byte, error) {
		req := PReq(new(Req))
		if err := msg.Decode(payload, req); err != nil {
			return nil, err
		}
		resp := PResp(new(Resp))
		if err := handler(req, resp); err != nil {
			return nil, err
		}
		return msg.Encode(resp), nil
	})
	if err != nil {
		return nil, err
	}
	return &Service[Req, Resp, PReq, PResp]{base}, nil
}

// NewRawService creates a service whose handler works on CDR payloads.
func NewRawService(node *Node, name, typ string, handler func(payload []byte) ([]byte, error)) (*serviceBase, error) {
	full, err := node.ResolveTopicName(name)
	if err != nil {
		return nil, err
	}
	s := &serviceBase{node: node, name: full, typ: typ, handler: handler}
	c := node.ctx
	c.mu.Lock()
	if _, exists := c.services[full]; exists {
		c.mu.Unlock()
		return nil, ErrServiceExists
	}
	c.services[full] = s
	c.graphChangedLocked()
	c.mu.Unlock()
	node.mu.Lock()
	node.services = append(node.services, s)
	node.mu.Unlock()
	c.kickHello()
	return s, nil
}

// Name returns the fully-qualified service name.
func (s *serviceBase) Name() string { return s.name }

// TypeName returns the ROS service type name.
func (s *serviceBase) TypeName() string { return s.typ }

// handle schedules the request on the node's executor.
func (s *serviceBase) handle(from transport.GUID, seq uint64, payload []byte, local bool) {
	c := s.node.ctx
	ok := s.node.enqueue(func() {
		s.mu.Lock()
		gone := s.gone
		s.mu.Unlock()
		if gone {
			c.respond(from, s.name, seq, nil, errors.New("service destroyed"))
			return
		}
		resp, err := s.handler(payload)
		c.respond(from, s.name, seq, resp, err)
	})
	if !ok {
		c.respond(from, s.name, seq, nil, ErrQueueFull)
	}
}

// Destroy removes the service.
func (s *serviceBase) Destroy() {
	s.node.mu.Lock()
	for i, x := range s.node.services {
		if x == s {
			s.node.services = append(s.node.services[:i], s.node.services[i+1:]...)
			break
		}
	}
	s.node.mu.Unlock()
	s.destroy()
	s.node.ctx.kickHello()
}

func (s *serviceBase) destroy() {
	s.mu.Lock()
	if s.gone {
		s.mu.Unlock()
		return
	}
	s.gone = true
	s.mu.Unlock()
	c := s.node.ctx
	c.mu.Lock()
	if c.services[s.name] == s {
		delete(c.services, s.name)
	}
	c.graphChangedLocked()
	c.mu.Unlock()
}
