// Package msg defines the interfaces every generated ROS 2 message and
// service type implements, plus a registry used by tooling (CLI, bridges)
// to instantiate messages by their ROS type name.
package msg

import (
	"sync"

	"bitbucket.org/goros/cdr"
)

// Message is implemented by all generated message types (pointer receivers).
type Message interface {
	// TypeName returns the ROS 2 interface name, e.g. "std_msgs/msg/String".
	TypeName() string
	MarshalCDR(*cdr.Encoder)
	UnmarshalCDR(*cdr.Decoder)
}

// Ptr is a constraint satisfied by *T when *T implements Message. It lets
// generic APIs be instantiated with the value type (goros.NewPublisher
// [std_msgs.String]) while still being able to allocate new instances.
type Ptr[T any] interface {
	*T
	Message
}

// Encode serializes m into a fresh byte slice.
func Encode(m Message) []byte {
	e := cdr.NewEncoder()
	m.MarshalCDR(e)
	out := make([]byte, len(e.Bytes()))
	copy(out, e.Bytes())
	return out
}

// EncodeTo serializes m using enc (which is reset first) and returns the
// encoder's buffer, which is only valid until the next use of enc.
func EncodeTo(enc *cdr.Encoder, m Message) []byte {
	enc.Reset()
	m.MarshalCDR(enc)
	return enc.Bytes()
}

// Decode deserializes data into m.
func Decode(data []byte, m Message) error {
	d := cdr.NewDecoder(data)
	m.UnmarshalCDR(d)
	return d.Err()
}

// ServiceTypeName derives the service name from a request or response type
// name: "pkg/srv/Foo_Request" -> "pkg/srv/Foo".
func ServiceTypeName(reqOrResp string) string {
	for _, suffix := range []string{"_Request", "_Response"} {
		if len(reqOrResp) > len(suffix) && reqOrResp[len(reqOrResp)-len(suffix):] == suffix {
			return reqOrResp[:len(reqOrResp)-len(suffix)]
		}
	}
	return reqOrResp
}

// --- registry ---------------------------------------------------------------

type Factory func() Message

type serviceEntry struct {
	req, resp Factory
}

var (
	regMu    sync.RWMutex
	messages = map[string]Factory{}
	services = map[string]serviceEntry{}
)

// Register records a factory for a message type name. Generated code calls
// this from init().
func Register(typeName string, f Factory) {
	regMu.Lock()
	messages[typeName] = f
	regMu.Unlock()
}

// RegisterService records request/response factories for a service type.
func RegisterService(typeName string, req, resp Factory) {
	regMu.Lock()
	services[typeName] = serviceEntry{req, resp}
	regMu.Unlock()
}

// New instantiates a registered message type, or returns nil.
func New(typeName string) Message {
	regMu.RLock()
	f := messages[typeName]
	regMu.RUnlock()
	if f == nil {
		return nil
	}
	return f()
}

// NewServiceRequest / NewServiceResponse instantiate halves of a registered
// service type, or return nil.
func NewServiceRequest(typeName string) Message {
	regMu.RLock()
	s, ok := services[typeName]
	regMu.RUnlock()
	if !ok {
		return nil
	}
	return s.req()
}

func NewServiceResponse(typeName string) Message {
	regMu.RLock()
	s, ok := services[typeName]
	regMu.RUnlock()
	if !ok {
		return nil
	}
	return s.resp()
}

// TypeNames returns all registered message type names (unsorted).
func TypeNames() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(messages))
	for k := range messages {
		out = append(out, k)
	}
	return out
}

// ServiceTypeNames returns all registered service type names (unsorted).
func ServiceTypeNames() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(services))
	for k := range services {
		out = append(out, k)
	}
	return out
}
